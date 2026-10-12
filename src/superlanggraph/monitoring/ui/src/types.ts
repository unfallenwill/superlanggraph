/**
 * 监控 API 响应与 SSE 事件类型（与 docs/monitoring-design.md §4/§6.2/§7 的 JSON 形状对应）。
 * 后端字段以设计文档为准；前端-B 的面板组件与本侧图表组件共同消费。
 */

export type RunStatus =
  | "pending"
  | "running"
  | "success"
  | "error"
  | "timeout"
  | "interrupted";

export type Outcome = "return" | "error" | "interrupt";

export interface WorkerInfo {
  worker_id: string;
  inflight: number;
  concurrency: number;
  last_seen_ts: number | null;
  alive: boolean;
}

export interface QueueSnapshot {
  ts: number;
  tasks_length: number;
  ctrl_length: number;
  pending_tasks: number;
  oldest_pending_ms: number;
  active_runs: number;
  workers: WorkerInfo[];
}

/** 窗口内按 (graph_id, node) 聚合的节点统计 */
export interface NodeStat {
  graph_id: string;
  node: string;
  count: number;
  total_ms: number;
  avg_ms: number;
  max_ms: number;
  errors: number;
}

export interface TokenTotals {
  input_tokens: number;
  output_tokens: number;
  total_tokens: number;
}

/** 错误摘要 recent 元素（具名类型；run 级错误没有 node，为 null） */
export interface ErrorItem {
  ts: number;
  run_id: string;
  graph_id: string;
  node: string | null;
  message: string;
}

export interface ErrorSummary {
  errors: number;
  runs_finished: number;
  rate: number;
  recent: ErrorItem[];
}

export interface MonitorSnapshot {
  generated_at: number;
  window_minutes: number;
  /** 六态键恒全 */
  status_counts: Record<RunStatus, number>;
  /** 探针未运行时为 null */
  queue: QueueSnapshot | null;
  token_totals: TokenTotals;
  error_summary: ErrorSummary;
  /** 按 total_ms 降序前 10 */
  node_top: NodeStat[];
}

/** 分钟序列点；缺口由服务端补零，bucket_ts 为分钟起点 epoch 秒 */
export interface MinutePoint {
  bucket_ts: number;
  runs_started: number;
  runs_finished: number;
  errors: number;
  input_tokens: number;
  output_tokens: number;
  queue_depth_avg: number | null;
  queue_depth_last: number | null;
}

// ---- SSE 事件（§4 判别联合；帧 data 为其中之一）----

export interface MonitorEventBase {
  event_id: string;
  ts: number;
  source: "driver" | "worker" | "monitor";
}

export interface RunLifecycleEvent extends MonitorEventBase {
  type: "run_lifecycle";
  run_id: string;
  thread_id: string;
  graph_id: string;
  status: RunStatus;
  error: string | null;
  duration_ms: number | null;
}

export interface NodeSpanEvent extends MonitorEventBase {
  type: "node_span";
  run_id: string;
  thread_id: string;
  graph_id: string;
  task_id: string;
  node: string;
  queue_ms: number;
  duration_ms: number;
  outcome: Outcome;
  error: string | null;
}

export interface TokenUsageEvent extends MonitorEventBase {
  type: "token_usage";
  run_id: string;
  thread_id: string;
  graph_id: string;
  node: string | null;
  input_tokens: number;
  output_tokens: number;
  total_tokens: number;
}

export interface WorkerHeartbeatEvent extends MonitorEventBase {
  type: "worker_heartbeat";
  worker_id: string;
  inflight: number;
  concurrency: number;
}

export type MonitorEvent =
  | RunLifecycleEvent
  | NodeSpanEvent
  | TokenUsageEvent
  | WorkerHeartbeatEvent;

/** 服务端事件日志条目（SSE 帧本身只含 event，seq 在帧 id） */
export interface SequencedEvent {
  seq: number;
  event: MonitorEvent;
}

// ---- REST 响应包装 ----

export interface SeriesResponse {
  points: MinutePoint[];
}

export interface NodesResponse {
  stats: NodeStat[];
  spans?: NodeSpanEvent[];
}

export interface RecentEventsResponse {
  events: SequencedEvent[];
}
