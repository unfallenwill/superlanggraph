"""InMemoryMetricStore 测试：桶对齐、缺口补零、状态净计数、p95、事件续读（§10）。

不连 Redis；心跳键值形状断言属 Redis 版，此处不覆盖（设计 §10 说明）。
"""

import asyncio
import time
from typing import Any, get_args

from superlanggraph.monitoring.events import (
    OUTCOME,
    RUN_STATUS,
    NodeSpanEvent,
    RunLifecycleEvent,
    TokenUsageEvent,
    WorkerHeartbeatEvent,
)
from superlanggraph.monitoring.store import (
    InMemoryMetricStore,
    NodeStat,
    QueueSnapshot,
    p95,
)


def _ts(offset_seconds: float = 0.0) -> float:
    return time.time() - offset_seconds


def _minute_base() -> float:
    """当前分钟起点 + 20 秒：保证同分钟测试不跨桶边界。"""
    return int(time.time() // 60) * 60 + 20.0


def _lifecycle(
    run_id: str,
    status: RUN_STATUS,
    ts: float,
    *,
    error: str | None = None,
    duration_ms: float | None = None,
) -> RunLifecycleEvent:
    return RunLifecycleEvent(
        event_id=f"ev-{run_id}-{status}",
        ts=ts,
        source="driver",
        run_id=run_id,
        thread_id="th",
        graph_id="g1",
        status=status,
        error=error,
        duration_ms=duration_ms,
    )


def _span(
    ts: float,
    *,
    run_id: str = "r1",
    graph_id: str = "g1",
    node: str = "n1",
    duration_ms: float = 100.0,
    outcome: OUTCOME = "return",
    error: str | None = None,
) -> NodeSpanEvent:
    return NodeSpanEvent(
        event_id=f"span-{ts}-{node}",
        ts=ts,
        source="worker",
        run_id=run_id,
        thread_id="th",
        graph_id=graph_id,
        task_id=f"task-{node}",
        node=node,
        queue_ms=1.0,
        duration_ms=duration_ms,
        outcome=outcome,
        error=error,
    )


def _tokens(ts: float, *, run_id: str = "r1", total: int = 15) -> TokenUsageEvent:
    return TokenUsageEvent(
        event_id=f"tok-{ts}-{total}",
        ts=ts,
        source="worker",
        run_id=run_id,
        thread_id="th",
        graph_id="g1",
        node="n1",
        input_tokens=10,
        output_tokens=5,
        total_tokens=total,
    )


def _heartbeat(ts: float, worker_id: str = "w1") -> WorkerHeartbeatEvent:
    return WorkerHeartbeatEvent(
        event_id=f"hb-{ts}",
        ts=ts,
        source="worker",
        worker_id=worker_id,
        inflight=1,
        concurrency=4,
    )


# ---- 桶对齐与分钟序列 ----


async def test_events_in_same_minute_share_bucket() -> None:
    store = InMemoryMetricStore()
    base = _minute_base()
    await store.record_event(_lifecycle("r1", "running", base))
    await store.record_event(_lifecycle("r1", "success", base + 5.0))
    points = await store.minute_series(minutes=3)
    started_points = [p for p in points if p.runs_started > 0]
    assert len(started_points) == 1
    point = started_points[0]
    assert point.bucket_ts == int(base // 60) * 60
    assert (point.runs_started, point.runs_finished) == (1, 1)
    assert point.queue_depth_avg is None
    assert point.queue_depth_last is None


async def test_minute_series_gap_fill_and_ascending() -> None:
    """缺口补零、bucket_ts 升序、窗口长度等于请求分钟数。"""
    store = InMemoryMetricStore()
    await store.record_event(_lifecycle("r1", "running", _ts(150.0)))  # 约 2~3 分钟前
    points = await store.minute_series(minutes=5)
    assert len(points) == 5
    assert [p.bucket_ts for p in points] == sorted(p.bucket_ts for p in points)
    assert points[-1].bucket_ts == int(time.time() // 60) * 60
    assert points[0].bucket_ts == points[-1].bucket_ts - 240
    non_zero = [p for p in points if p.runs_started > 0]
    assert len(non_zero) == 1 and non_zero[0].runs_started == 1
    assert all(p.runs_finished == 0 and p.errors == 0 for p in points)


# ---- 状态迁移净计数 ----


async def test_status_migration_net_counts() -> None:
    """pending→running→success 与直达终态均净计数；重复终态不双计。"""
    store = InMemoryMetricStore()
    ts = _ts(5.0)
    await store.record_event(_lifecycle("r1", "pending", ts))
    await store.record_event(_lifecycle("r1", "running", ts + 1))
    await store.record_event(_lifecycle("r1", "success", ts + 2))
    await store.record_event(_lifecycle("r2", "success", ts + 3))
    snapshot = await store.overview()
    assert snapshot.status_counts == dict.fromkeys(get_args(RUN_STATUS), 0) | {
        "success": 2
    }
    # 重复终态：run1 再次 success，不重复计入 finished。
    await store.record_event(_lifecycle("r1", "success", ts + 4))
    again = await store.overview()
    assert again.status_counts["success"] == 2
    assert again.error_summary.runs_finished == 2
    points = await store.minute_series(minutes=5)
    assert sum(p.runs_started for p in points) == 2
    assert sum(p.runs_finished for p in points) == 2
    assert sum(p.errors for p in points) == 0


async def test_error_status_counts_and_message_truncation() -> None:
    """error 迁移计 errors 与 recent 列表；超长消息截断至 500 字符。"""
    store = InMemoryMetricStore()
    long_message = "x" * 600
    await store.record_event(
        _lifecycle("r1", "error", _ts(5.0), error=long_message, duration_ms=9.0)
    )
    snapshot = await store.overview()
    assert snapshot.status_counts["error"] == 1
    assert snapshot.error_summary.errors == 1
    assert snapshot.error_summary.runs_finished == 1
    assert snapshot.error_summary.rate == 1.0
    assert len(snapshot.error_summary.recent) == 1
    entry = snapshot.error_summary.recent[0]
    assert entry["message"] == "x" * 500
    assert entry["run_id"] == "r1" and entry["node"] is None


# ---- p95 纯函数边界 ----


def test_p95_empty_returns_none() -> None:
    assert p95([]) is None


def test_p95_single_sample() -> None:
    assert p95([_span(0.0, duration_ms=50.0)]) == 50.0


def test_p95_rank_and_node_filter() -> None:
    spans = [
        _span(1.0, node="a", duration_ms=100.0),
        _span(2.0, node="a", duration_ms=200.0),
        _span(3.0, node="a", duration_ms=300.0),
        _span(4.0, node="a", duration_ms=400.0),
        _span(5.0, node="b", duration_ms=9999.0),
    ]
    assert p95(spans) == 9999.0  # ceil(5*0.95)=5 → 全体最大
    assert p95(spans, node="a") == 400.0  # 过滤后 n=4，ceil(3.8)=4 → a 组最大
    assert p95([_span(1.0, node="a", duration_ms=1.0)], node="missing") is None


# ---- recent_events 续读 ----


async def test_recent_events_after_seq_filter_and_order() -> None:
    store = InMemoryMetricStore()
    for index in range(5):
        await store.record_event(_heartbeat(float(index), f"w{index}"))
    recent = await store.recent_events()
    assert [item.seq for item in recent] == [1, 2, 3, 4, 5]
    resumed = await store.recent_events(after_seq=2)
    assert [item.seq for item in resumed] == [3, 4, 5]
    limited = await store.recent_events(limit=2, after_seq=2)
    assert [item.seq for item in limited] == [3, 4]
    tail = await store.recent_events(limit=2)
    assert [item.seq for item in tail] == [4, 5]


# ---- overview 形状 ----


async def test_overview_shape_and_window() -> None:
    store = InMemoryMetricStore()
    ts = _ts(5.0)
    await store.record_event(_lifecycle("r1", "running", ts))
    await store.record_event(_span(ts, duration_ms=200.0))
    await store.record_event(_span(ts, node="n2", duration_ms=800.0))
    await store.record_event(_tokens(ts))
    snapshot = await store.overview(window_minutes=30)
    assert set(snapshot.status_counts) == set(get_args(RUN_STATUS))
    assert snapshot.queue is None
    assert snapshot.window_minutes == 30
    assert snapshot.token_totals.total_tokens == 15
    assert len(snapshot.node_top) == 2
    assert snapshot.node_top[0].node == "n2"  # total_ms 降序
    assert snapshot.node_top[0].max_ms == 800.0


async def test_overview_node_top_includes_error_counts() -> None:
    store = InMemoryMetricStore()
    await store.record_event(
        _span(_ts(5.0), outcome="error", duration_ms=10.0, error="bad")
    )
    snapshot = await store.overview()
    stat = snapshot.node_top[0]
    assert (stat.count, stat.errors) == (1, 1)
    assert snapshot.error_summary.recent[0]["node"] == "n1"


# ---- 探针纠偏 ----


async def test_record_probe_overrides_running_and_pending() -> None:
    store = InMemoryMetricStore()
    await store.record_event(_lifecycle("r1", "running", _ts(5.0)))
    snapshot = await store.overview()
    assert snapshot.status_counts["running"] == 1
    now = time.time()
    await store.record_probe(
        QueueSnapshot(
            ts=now,
            tasks_length=2,
            ctrl_length=1,
            pending_tasks=3,
            oldest_pending_ms=100.0,
            active_runs=4,
            workers=[],
        )
    )
    corrected = await store.overview()
    assert corrected.status_counts["running"] == 4
    assert corrected.status_counts["pending"] == 0
    assert corrected.queue is not None and corrected.queue.tasks_length == 2
    point = (await store.minute_series(minutes=2))[-1]
    assert point.queue_depth_last == 6  # tasks + ctrl + pending
    assert point.queue_depth_avg == 6.0


# ---- run 汇总 ----


async def test_run_summary_with_spans_ascending() -> None:
    store = InMemoryMetricStore()
    ts = _ts(30.0)
    await store.record_event(_lifecycle("r1", "running", ts))
    await store.record_event(_span(ts + 1, duration_ms=100.0))
    await store.record_event(_span(ts + 2, node="n2", duration_ms=200.0))
    await store.record_event(_tokens(ts + 3))
    detail = await store.run_summary("r1")
    assert detail is not None
    summary: dict[str, Any] = detail["summary"]
    assert summary["status"] == "running"
    assert summary["node_count"] == 2
    assert summary["total_tokens"] == 15
    spans: list[NodeSpanEvent] = detail["spans"]
    assert [span.node for span in spans] == ["n1", "n2"]  # 按 ts 升序
    assert await store.run_summary("no-such-run") is None


# ---- node_stats / recent_spans ----


async def test_node_stats_graph_filter_and_ranking() -> None:
    store = InMemoryMetricStore()
    ts = _ts(5.0)
    await store.record_event(_span(ts, graph_id="g1", node="a", duration_ms=100.0))
    await store.record_event(_span(ts, graph_id="g1", node="b", duration_ms=900.0))
    await store.record_event(_span(ts, graph_id="g2", node="c", duration_ms=500.0))
    stats = await store.node_stats()
    assert [(s.graph_id, s.node) for s in stats] == [("g1", "b"), ("g2", "c"), ("g1", "a")]
    filtered = await store.node_stats(graph_id="g2")
    assert [s.node for s in filtered] == ["c"]
    limited = await store.node_stats(limit=1)
    assert [s.node for s in limited] == ["b"]
    assert all(isinstance(s, NodeStat) for s in stats)


async def test_recent_spans_descending_and_filter() -> None:
    store = InMemoryMetricStore()
    base = _ts(60.0)
    await store.record_event(_span(base, node="a", duration_ms=1.0))
    await store.record_event(_span(base + 10, node="b", duration_ms=2.0))
    await store.record_event(_span(base + 20, graph_id="g2", node="c", duration_ms=3.0))
    all_spans = await store.recent_spans(graph_id=None)
    assert [span.node for span in all_spans] == ["c", "b", "a"]
    only_g1 = await store.recent_spans(graph_id="g1")
    assert [span.node for span in only_g1] == ["b", "a"]


# ---- subscribe / aclose ----


async def test_subscribe_receives_events_and_aclose_terminates() -> None:
    store = InMemoryMetricStore()
    stream = store.subscribe()
    # 先启动一次 __anext__ 让订阅队列注册，再投递事件（否则事件无接收者）。
    waiter = asyncio.ensure_future(stream.__anext__())
    await asyncio.sleep(0.01)
    await store.record_event(_heartbeat(1.0))
    item = await asyncio.wait_for(waiter, timeout=2.0)
    assert item.seq == 1
    assert isinstance(item.event, WorkerHeartbeatEvent)
    await store.aclose()
    try:
        await stream.__anext__()
        raise AssertionError("aclose 后订阅流应终止")
    except StopAsyncIteration:
        pass
