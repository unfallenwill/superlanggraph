/**
 * 数据获取与实时流 hooks：
 * - useOverview / useSeries / useNodes：REST 轮询（默认 30s 周期纠偏，全量替换）；
 * - useEventStream：EventSource 订阅，按 seq 去重后按 type 分发；
 * - applyEventToSnapshot / applyNodeSpan：纯函数折叠，供 App 组合状态。
 */
import { useEffect, useRef, useState } from "react";
import type {
  ErrorItem,
  MinutePoint,
  MonitorEvent,
  MonitorSnapshot,
  NodeSpanEvent,
  NodeStat,
  RunStatus,
  WorkerInfo,
} from "./types";

const API = "/monitor";

/** 终态：计入 error_summary.runs_finished */
const TERMINAL: ReadonlySet<RunStatus> = new Set([
  "success",
  "error",
  "timeout",
  "interrupted",
] as RunStatus[]);

/** REST 轮询通用实现：挂载立即拉取，此后每 refreshMs 全量纠偏一次 */
function usePoll<T>(url: string, refreshMs: number): { data: T | null; error: string | null } {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let alive = true;
    const load = async () => {
      try {
        const res = await fetch(url, { headers: { Accept: "application/json" } });
        if (!res.ok) throw new Error(`HTTP ${res.status} ${url}`);
        const d = (await res.json()) as T;
        if (alive) {
          setData(d);
          setError(null);
        }
      } catch (e) {
        if (alive) setError(e instanceof Error ? e.message : String(e));
      }
    };
    void load();
    const timer = setInterval(() => void load(), refreshMs);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, [url, refreshMs]);
  return { data, error };
}

export function useOverview(refreshMs = 30_000) {
  return usePoll<MonitorSnapshot>(`${API}/overview`, refreshMs);
}

export function useSeries(refreshMs = 30_000) {
  const { data, error } = usePoll<{ points: MinutePoint[] }>(
    `${API}/series?minutes=60`,
    refreshMs,
  );
  return { points: data?.points ?? null, error };
}

export function useNodes(refreshMs = 30_000) {
  const { data, error } = usePoll<{ stats: NodeStat[] }>(`${API}/nodes?limit=50`, refreshMs);
  return { stats: data?.stats ?? null, error };
}

/**
 * SSE 实时流：订阅仅建立一次，回调经 ref 透传，App 重渲染不会重建连接。
 * 断线由浏览器原生重连（自动携带 Last-Event-ID，服务端按 seq 回放）；
 * 15s 心跳为 ": ping" 注释帧，浏览器忽略，无需处理。
 */
export function useEventStream(
  onSnapshot: (s: MonitorSnapshot) => void,
  onEvent: (e: MonitorEvent) => void,
): { connected: boolean } {
  const [connected, setConnected] = useState(false);
  const cbs = useRef({ onSnapshot, onEvent });
  cbs.current = { onSnapshot, onEvent };
  const lastSeq = useRef(0);

  useEffect(() => {
    const es = new EventSource(`${API}/stream`);
    es.onopen = () => setConnected(true);
    es.onerror = () => setConnected(false);

    const listen = (type: string) => (ev: MessageEvent) => {
      let data: unknown;
      try {
        data = JSON.parse(ev.data as string);
      } catch {
        return; // 非法 JSON 帧直接忽略
      }
      if (type === "snapshot") {
        cbs.current.onSnapshot(data as MonitorSnapshot);
        return;
      }
      // 至少一次语义：回放与订阅交接窗口可能重复，按 seq 单调去重
      const seq = Number(ev.lastEventId);
      if (!(seq > lastSeq.current)) return;
      lastSeq.current = seq;
      cbs.current.onEvent(data as MonitorEvent);
    };

    for (const t of [
      "snapshot",
      "run_lifecycle",
      "node_span",
      "token_usage",
      "worker_heartbeat",
    ]) {
      es.addEventListener(t, listen(t) as EventListener);
    }
    return () => es.close();
  }, []);

  return { connected };
}

/**
 * 把一条实时事件折叠进 snapshot 副本（纯函数）。
 * node_span 不在此处理：节点统计走独立状态（见 applyNodeSpan）；
 * queue 的探针维度字段（队列深度等）由 30s 轮询纠偏，不做本地估算。
 */
export function applyEventToSnapshot(s: MonitorSnapshot, e: MonitorEvent): MonitorSnapshot {
  switch (e.type) {
    case "run_lifecycle": {
      const status_counts: MonitorSnapshot["status_counts"] = {
        ...s.status_counts,
        [e.status]: s.status_counts[e.status] + 1,
      };
      const finished = TERMINAL.has(e.status)
        ? s.error_summary.runs_finished + 1
        : s.error_summary.runs_finished;
      let error_summary = { ...s.error_summary, runs_finished: finished };
      if (e.status === "error") {
        const recent: ErrorItem[] = [
          {
            ts: e.ts,
            run_id: e.run_id,
            graph_id: e.graph_id,
            node: null,
            message: e.error ?? "",
          },
          ...s.error_summary.recent,
        ].slice(0, 10);
        error_summary = {
          ...error_summary,
          errors: s.error_summary.errors + 1,
          rate: (s.error_summary.errors + 1) / Math.max(finished, 1),
          recent,
        };
      } else {
        error_summary = {
          ...error_summary,
          rate: error_summary.errors / Math.max(finished, 1),
        };
      }
      return { ...s, status_counts, error_summary };
    }
    case "token_usage":
      return {
        ...s,
        token_totals: {
          input_tokens: s.token_totals.input_tokens + e.input_tokens,
          output_tokens: s.token_totals.output_tokens + e.output_tokens,
          total_tokens: s.token_totals.total_tokens + e.total_tokens,
        },
      };
    case "worker_heartbeat": {
      if (!s.queue) return s; // 探针未运行时无 worker 列表可更新
      const worker: WorkerInfo = {
        worker_id: e.worker_id,
        inflight: e.inflight,
        concurrency: e.concurrency,
        last_seen_ts: e.ts,
        alive: true,
      };
      const workers = [...s.queue.workers];
      const i = workers.findIndex((w) => w.worker_id === e.worker_id);
      if (i >= 0) workers[i] = worker;
      else workers.push(worker);
      return { ...s, queue: { ...s.queue, workers } };
    }
    default:
      return s;
  }
}

/** node_span 折叠进节点统计列表：本地重算 count/avg/max/errors，保持 total_ms 降序 */
export function applyNodeSpan(stats: NodeStat[], e: NodeSpanEvent): NodeStat[] {
  const i = stats.findIndex((s) => s.graph_id === e.graph_id && s.node === e.node);
  const errDelta = e.outcome === "error" ? 1 : 0;
  let next: NodeStat[];
  if (i < 0) {
    next = [
      ...stats,
      {
        graph_id: e.graph_id,
        node: e.node,
        count: 1,
        total_ms: e.duration_ms,
        avg_ms: e.duration_ms,
        max_ms: e.duration_ms,
        errors: errDelta,
      },
    ];
  } else {
    const s = stats[i];
    const count = s.count + 1;
    const totalMs = s.total_ms + e.duration_ms;
    next = [
      ...stats.slice(0, i),
      {
        ...s,
        count,
        total_ms: totalMs,
        avg_ms: totalMs / count,
        max_ms: Math.max(s.max_ms, e.duration_ms),
        errors: s.errors + errDelta,
      },
      ...stats.slice(i + 1),
    ];
  }
  return next.sort((a, b) => b.total_ms - a.total_ms);
}
