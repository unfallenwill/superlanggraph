/**
 * 仪表盘组装：REST 轮询（30s 纠偏）+ SSE 实时折叠 + antd 网格布局。
 * 五个基础面板由前端-B 提供（components/ 同名文件，具名导出、可选 props、纯展示）；
 * 图表三件套（node-timeline / series-chart / event-log）在本侧实现。
 */
import { useCallback, useEffect, useState } from "react";
import { Card, Col, ConfigProvider, Row, theme } from "antd";
import zhCN from "antd/locale/zh_CN";
import type { MonitorEvent, MonitorSnapshot, NodeStat } from "./types";
import {
  applyEventToSnapshot,
  applyNodeSpan,
  useEventStream,
  useNodes,
  useOverview,
  useSeries,
} from "./hooks";
import { StatusCards } from "./components/status-cards";
import { QueuePanel } from "./components/queue-panel";
import { WorkersPanel } from "./components/workers-panel";
import { TokensPanel } from "./components/tokens-panel";
import { ErrorPanel } from "./components/error-panel";
import NodeTimeline from "./components/node-timeline";
import SeriesChart from "./components/series-chart";
import EventLog from "./components/event-log";

const EVENT_LOG_CAP = 200; // 事件滚屏环形缓冲上限

export default function App() {
  const { data: overview, error: overviewErr } = useOverview();
  const { points, error: seriesErr } = useSeries();
  const { stats: polledNodes, error: nodesErr } = useNodes();

  // snapshot 单一状态源：REST 轮询全量纠偏（整体替换），SSE 帧在其副本上折叠
  const [snapshot, setSnapshot] = useState<MonitorSnapshot | null>(null);
  // 节点统计独立状态：轮询 50 条作种子，node_span 帧增量折叠，下次轮询重置
  const [nodeStats, setNodeStats] = useState<NodeStat[]>([]);
  const [log, setLog] = useState<MonitorEvent[]>([]);

  useEffect(() => {
    if (polledNodes) setNodeStats(polledNodes);
  }, [polledNodes]);

  // REST 轮询纠偏：每次 overview 到达即整体替换 snapshot（含首绘）
  useEffect(() => {
    if (overview) setSnapshot(overview);
  }, [overview]);

  // SSE 数据帧：滚屏入环形缓冲 + 折叠进 snapshot / nodeStats
  const onFrame = useCallback((e: MonitorEvent) => {
    setLog((prev) =>
      prev.length >= EVENT_LOG_CAP
        ? [...prev.slice(prev.length - EVENT_LOG_CAP + 1), e]
        : [...prev, e],
    );
    setSnapshot((prev) => (prev ? applyEventToSnapshot(prev, e) : prev));
    if (e.type === "node_span") setNodeStats((prev) => applyNodeSpan(prev, e));
  }, []);

  const { connected } = useEventStream(setSnapshot, onFrame);

  const banner = [overviewErr, seriesErr, nodesErr].filter(Boolean).join("；");

  return (
    <ConfigProvider theme={{ algorithm: theme.darkAlgorithm }} locale={zhCN}>
      <div style={{ padding: 12 }}>
        {/* 非阻塞错误提示条：任一 REST 轮询失败时置顶展示，随轮询自愈 */}
        {banner && (
          <div
            role="alert"
            style={{
              position: "sticky",
              top: 0,
              zIndex: 100,
              marginBottom: 12,
              padding: "6px 12px",
              background: "#5c1a1a",
              color: "#ffd7d7",
              borderRadius: 6,
              fontSize: 13,
            }}
          >
            接口异常：{banner}（将随 30s 轮询自动纠偏重试）
          </div>
        )}

        <Row gutter={[12, 12]}>
          <Col xs={24} md={12} xl={6}>
            {/* StatusCards 自带六张小卡片外框，不再包一层 Card */}
            <StatusCards snapshot={snapshot ?? undefined} />
          </Col>
          <Col xs={24} md={12} xl={6}>
            <Card title="队列" size="small">
              <QueuePanel queue={snapshot?.queue} />
            </Card>
          </Col>
          <Col xs={24} md={12} xl={6}>
            <Card title="Workers" size="small">
              <WorkersPanel workers={snapshot?.queue?.workers} />
            </Card>
          </Col>
          <Col xs={24} md={12} xl={6}>
            <Card title="令牌窗口合计" size="small">
              <TokensPanel totals={snapshot?.token_totals} />
            </Card>
          </Col>
          <Col xs={24} md={12} xl={6}>
            <Card title="错误" size="small">
              <ErrorPanel summary={snapshot?.error_summary} />
            </Card>
          </Col>
          <Col xs={24} xl={12}>
            <NodeTimeline stats={nodeStats} />
          </Col>
          <Col xs={24} xl={12}>
            <SeriesChart points={points ?? []} />
          </Col>
          <Col xs={24} xl={12}>
            <EventLog events={log} />
          </Col>
        </Row>

        {/* 连接状态角标（非阻塞，右下角） */}
        <div
          style={{
            position: "fixed",
            right: 8,
            bottom: 8,
            zIndex: 100,
            padding: "2px 10px",
            borderRadius: 10,
            fontSize: 12,
            background: "#1f1f1f",
            color: connected ? "#52c41a" : "#faad14",
          }}
        >
          {connected ? "实时流已连接" : "实时流重连中…"}
        </div>
      </div>
    </ConfigProvider>
  );
}
