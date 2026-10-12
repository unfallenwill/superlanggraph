/**
 * Worker 面板（对应设计文档 §8.1 #workers-panel）
 * 数据源：MonitorSnapshot.queue.workers（[WorkerInfo]）+ SSE worker_heartbeat 帧（上层归并后传入）
 * 纯展示组件：数据一律来自 props，不发请求；布局响应式由组装层负责。
 */
import { Table, Tag } from "antd";
import type { ColumnsType } from "antd/es/table";
import type { WorkerInfo } from "../types";

export interface WorkersPanelProps {
  /** worker 列表；未传时展示 Table 自带空态 */
  workers?: WorkerInfo[];
}

/** unix 秒 → 本地时间 HH:MM:SS */
function formatSeen(ts: number): string {
  return new Date(ts * 1000).toLocaleTimeString("zh-CN", { hour12: false });
}

const COLUMNS: ColumnsType<WorkerInfo> = [
  {
    title: "Worker",
    dataIndex: "worker_id",
    ellipsis: true,
  },
  {
    title: "在飞",
    dataIndex: "inflight",
    width: 72,
    align: "right",
  },
  {
    title: "并发",
    dataIndex: "concurrency",
    width: 72,
    align: "right",
  },
  {
    title: "最近心跳",
    dataIndex: "last_seen_ts",
    width: 96,
    render: (ts: number | null) =>
      ts == null ? "无心跳" : formatSeen(ts),
  },
  {
    title: "存活",
    dataIndex: "alive",
    width: 80,
    render: (alive: boolean) => (
      <Tag color={alive ? "green" : "default"}>{alive ? "存活" : "失联"}</Tag>
    ),
  },
];

export function WorkersPanel({ workers }: WorkersPanelProps) {
  return (
    <section id="workers-panel" aria-label="Worker 状态">
      <Table<WorkerInfo>
        size="small"
        rowKey="worker_id"
        columns={COLUMNS}
        dataSource={workers ?? []}
        pagination={false}
      />
    </section>
  );
}
