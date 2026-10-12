/**
 * 队列面板（对应设计文档 §8.1 #queue-panel）
 * 数据源：MonitorSnapshot.queue（QueueSnapshot），探针未运行时为 null。
 * 纯展示组件：数据一律来自 props，不发请求；布局响应式由组装层负责。
 */
import { Descriptions, Empty } from "antd";
import type { QueueSnapshot } from "../types";

export interface QueuePanelProps {
  /** 最近探针快照；undefined/null 表示探针未运行，展示空态 */
  queue?: QueueSnapshot | null;
}

/** unix 秒 → 本地时间 HH:MM:SS */
function formatTs(ts: number): string {
  return new Date(ts * 1000).toLocaleTimeString("zh-CN", { hour12: false });
}

export function QueuePanel({ queue }: QueuePanelProps) {
  if (!queue) {
    return (
      <section id="queue-panel" aria-label="队列状态">
        <Empty description="探针未运行，暂无队列数据" />
      </section>
    );
  }

  return (
    <section id="queue-panel" aria-label="队列状态">
      <Descriptions size="small" column={2} bordered>
        <Descriptions.Item label="tasks 队列深度">
          {queue.tasks_length}
        </Descriptions.Item>
        <Descriptions.Item label="ctrl 队列深度">
          {queue.ctrl_length}
        </Descriptions.Item>
        <Descriptions.Item label="PEL 未确认">
          {queue.pending_tasks}
        </Descriptions.Item>
        <Descriptions.Item label="最老滞留">
          {queue.oldest_pending_ms.toFixed(0)} ms
        </Descriptions.Item>
        <Descriptions.Item label="活跃 run">
          {queue.active_runs}
        </Descriptions.Item>
        <Descriptions.Item label="采样时刻">
          {formatTs(queue.ts)}
        </Descriptions.Item>
      </Descriptions>
    </section>
  );
}
