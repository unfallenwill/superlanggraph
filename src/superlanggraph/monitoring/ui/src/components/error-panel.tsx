/**
 * 错误面板（对应设计文档 §8.1 #error-panel）
 * 数据源：MonitorSnapshot.error_summary（ErrorSummary：错误率 + 最近错误列表）+ SSE 错误帧（上层归并后传入）
 * 纯展示组件：数据一律来自 props，不发请求；布局响应式由组装层负责。
 */
import { Statistic, Table } from "antd";
import type { ColumnsType } from "antd/es/table";
import type { ErrorSummary } from "../types";

export interface ErrorPanelProps {
  /** 错误摘要；未传时以零错误渲染（自测用） */
  summary?: ErrorSummary;
}

/** 最近错误条目类型：取自 ErrorSummary.recent 的具名元素类型 */
type RecentError = ErrorSummary["recent"][number];

/** unix 秒 → 本地时间 HH:MM:SS */
function formatTs(ts: number): string {
  return new Date(ts * 1000).toLocaleTimeString("zh-CN", { hour12: false });
}

const COLUMNS: ColumnsType<RecentError> = [
  {
    title: "时间",
    dataIndex: "ts",
    width: 96,
    render: (ts: number) => formatTs(ts),
  },
  {
    title: "Graph",
    dataIndex: "graph_id",
    width: 110,
    ellipsis: true,
  },
  {
    title: "Node",
    dataIndex: "node",
    width: 110,
    ellipsis: true,
    // run 级错误无节点，null 展示占位符
    render: (node: string | null) => node ?? "-",
  },
  {
    title: "Run",
    dataIndex: "run_id",
    ellipsis: true,
  },
  {
    title: "消息",
    dataIndex: "message",
    ellipsis: true,
  },
];

export function ErrorPanel({ summary }: ErrorPanelProps) {
  const errors = summary?.errors ?? 0;
  const runsFinished = summary?.runs_finished ?? 0;
  const rate = summary?.rate ?? 0;
  const recent = summary?.recent ?? [];

  return (
    <section id="error-panel" aria-label="错误情况">
      {/* 错误率摘要：rate = errors / max(runs_finished, 1)，窗口内 */}
      <Statistic
        title="错误率"
        value={rate * 100}
        precision={1}
        suffix="%"
      />
      <p>
        {errors} / {runsFinished} 次运行
      </p>

      {/* 最近错误列表（消息已在入库前截断至 500 字符） */}
      <Table<RecentError>
        size="small"
        rowKey={(item, index) => `${item.ts}-${item.run_id}-${index ?? 0}`}
        columns={COLUMNS}
        dataSource={recent}
        pagination={false}
      />
    </section>
  );
}
