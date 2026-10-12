/**
 * 原始事件滚屏：antd List 展示，环形缓冲 ≤200 条（由父组件截断传入）。
 * 最新事件在顶部，免自动滚动管理；React 渲染自动转义，无 XSS 风险。
 */
import { Card, List, Typography } from "antd";
import type { MonitorEvent } from "../types";

function fmtClock(ts: number): string {
  const d = new Date(ts * 1000);
  const p = (x: number) => String(x).padStart(2, "0");
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

function fmtMs(ms: number): string {
  return ms >= 1000 ? `${(ms / 1000).toFixed(2)}s` : `${Math.round(ms)}ms`;
}

/** 单条事件摘要：按 type 拼接，错误内容红色 */
function renderEvent(e: MonitorEvent) {
  const time = <Typography.Text type="secondary">{fmtClock(e.ts)}</Typography.Text>;
  switch (e.type) {
    case "run_lifecycle":
      return (
        <span>
          {time} run {e.run_id.slice(0, 8)} → {e.status}
          {e.error && <Typography.Text type="danger"> {e.error}</Typography.Text>}
        </span>
      );
    case "node_span":
      return (
        <span>
          {time} span {e.graph_id}.{e.node} {fmtMs(e.duration_ms)}（排队 {fmtMs(e.queue_ms)}）
          {e.outcome === "error" && (
            <Typography.Text type="danger"> {e.error ?? "error"}</Typography.Text>
          )}
        </span>
      );
    case "token_usage":
      return (
        <span>
          {time} token {e.graph_id} +{e.input_tokens}/{e.output_tokens}
        </span>
      );
    case "worker_heartbeat":
      return (
        <span>
          {time} worker {e.worker_id} 在飞 {e.inflight}/{e.concurrency}
        </span>
      );
  }
}

export default function EventLog({ events }: { events: MonitorEvent[] }) {
  // 展示为最新在上；key 用 event_id（uuid 唯一，重复 seq 帧已在 hooks 层去重）
  const reversed = [...events].reverse();
  return (
    <Card
      title="事件流（≤200 条，最新在上）"
      size="small"
      styles={{ body: { maxHeight: 360, overflowY: "auto", padding: "0 16px" } }}
    >
      <List
        size="small"
        dataSource={reversed}
        locale={{ emptyText: "暂无事件" }}
        rowKey={(e) => e.event_id}
        renderItem={(e) => (
          <List.Item style={{ padding: "3px 0" }}>
            <span style={{ fontSize: 12, fontFamily: "monospace" }}>{renderEvent(e)}</span>
          </List.Item>
        )}
      />
    </Card>
  );
}
