/**
 * 运行状态分布卡片（对应设计文档 §8.1 #status-cards）
 * 数据源：MonitorSnapshot.status_counts（六态键恒全）+ SSE run_lifecycle 帧增量（由上层维护后传入）
 * 纯展示组件：数据一律来自 props，不发请求；布局响应式由组装层负责。
 */
import { Badge, Card, Flex, Statistic } from "antd";
import type { MonitorSnapshot } from "../types";

/** 六态展示标签（键与 RUN_STATUS 字面量一致） */
const STATUS_LABELS: Record<string, string> = {
  pending: "待运行",
  running: "运行中",
  success: "成功",
  error: "错误",
  timeout: "超时",
  interrupted: "中断",
};

/** 六态 → antd Badge 预设色：灰/蓝/绿/红/橙/黄 */
const STATUS_COLORS: Record<string, string> = {
  pending: "default",
  running: "blue",
  success: "green",
  error: "red",
  timeout: "orange",
  interrupted: "gold",
};

/** 自测默认值：六态全零 */
const EMPTY_COUNTS: Record<string, number> = {
  pending: 0,
  running: 0,
  success: 0,
  error: 0,
  timeout: 0,
  interrupted: 0,
};

export interface StatusCardsProps {
  /** 监控快照；未传时以全零六态渲染（脚手架未就绪时的自测形态） */
  snapshot?: MonitorSnapshot;
}

export function StatusCards({ snapshot }: StatusCardsProps) {
  // 快照缺键时回退 0，避免 undefined 上屏
  const counts: Record<string, number> = {
    ...EMPTY_COUNTS,
    ...(snapshot?.status_counts ?? {}),
  };

  return (
    <section id="status-cards" aria-label="运行状态分布">
      <Flex gap="small" wrap="wrap">
        {Object.entries(STATUS_LABELS).map(([status, label]) => (
          <Card
            key={status}
            size="small"
            title={
              <Badge
                color={STATUS_COLORS[status]}
                text={label}
              />
            }
            style={{ flex: "1 1 90px" }}
          >
            <Statistic value={counts[status] ?? 0} />
          </Card>
        ))}
      </Flex>
    </section>
  );
}
