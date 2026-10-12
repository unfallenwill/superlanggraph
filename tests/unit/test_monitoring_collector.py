"""监控采集器测试：钩子非阻塞、背压丢弃、异常隔离、令牌合并与 Redis 探针（§10）。

不连 Redis：store 用测试替身，RedisProbe 用假 client 验证键读取与存活判定。
"""

import asyncio
import fnmatch
import json
import time
from collections.abc import AsyncIterator, Callable
from typing import Any, cast

from redis.asyncio import Redis

from superlanggraph.monitoring.collector import MonitoringCollector, RedisProbe
from superlanggraph.monitoring.events import (
    MonitorEvent,
    NodeSpanEvent,
    RunLifecycleEvent,
    TokenUsageEvent,
)
from superlanggraph.monitoring.store import MetricStore, QueueSnapshot, WorkerInfo


class StubStore:
    """MetricStore 测试替身：记录事件；fail_times 次调用内抛异常模拟故障。"""

    def __init__(self, *, fail_times: int = 0, delay: float = 0.0) -> None:
        self.received: list[MonitorEvent] = []
        self.calls = 0
        self.fail_times = fail_times
        self.delay = delay

    async def record_event(self, event: MonitorEvent) -> None:
        self.calls += 1
        if self.delay > 0.0:
            await asyncio.sleep(self.delay)
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("store 故障注入")
        self.received.append(event)

    async def aclose(self) -> None:
        return None


def _stub(fail_times: int = 0) -> MetricStore:
    return cast("MetricStore", StubStore(fail_times=fail_times))


async def _wait_until(predicate: Callable[[], bool], timeout: float = 2.0) -> None:
    """轮询等待排水完成；超时即断言失败，避免测试挂死。"""
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("等待排水条件超时")
        await asyncio.sleep(0.01)


# ---- 钩子与排水 ----


async def test_hook_is_sync_and_nonblocking() -> None:
    """钩子入队即返回：未启动排水时 store 零调用，事件由 aclose 兜底落库。"""
    stub = StubStore()
    collector = MonitoringCollector(cast("MetricStore", stub), source="driver")
    collector.on_run_lifecycle(
        run_id="r", thread_id="t", graph_id="g", status="running"
    )
    assert stub.calls == 0  # 钩子同步返回，store 未被触碰
    await collector.aclose()
    assert len(stub.received) == 1 and collector.dropped == 0


async def test_hook_returns_while_store_slow() -> None:
    """store 处理缓慢时钩子仍立即返回（非阻塞）。"""
    stub = StubStore(delay=0.3)
    collector = MonitoringCollector(cast("MetricStore", stub), source="driver", queue_size=8)
    collector.start()
    begin = time.monotonic()
    collector.on_worker_heartbeat(worker_id="w", inflight=1, concurrency=2)
    assert time.monotonic() - begin < 0.05
    await collector.aclose()


async def test_queue_full_drops_oldest_and_counts() -> None:
    """队列满丢最旧：dropped 计 1，store 只收到最新一条。"""
    stub = StubStore()
    collector = MonitoringCollector(cast("MetricStore", stub), source="driver", queue_size=1)
    collector.on_run_lifecycle(run_id="r", thread_id="t", graph_id="g", status="running")
    collector.on_run_lifecycle(run_id="r", thread_id="t", graph_id="g", status="success")
    assert collector.dropped == 1
    collector.start()
    await _wait_until(lambda: len(stub.received) == 1)
    assert isinstance(stub.received[0], RunLifecycleEvent)
    assert stub.received[0].status == "success"
    assert collector.dropped == 1


async def test_store_exception_does_not_kill_drain_loop() -> None:
    """store 抛异常被吞：排水循环存活，后续事件照常落库。"""
    stub = StubStore(fail_times=1)
    collector = MonitoringCollector(cast("MetricStore", stub), source="worker")
    collector.start()
    collector.on_run_lifecycle(run_id="r", thread_id="t", graph_id="g", status="error")
    await _wait_until(lambda: stub.calls == 1)
    assert stub.received == []
    collector.on_run_lifecycle(run_id="r", thread_id="t", graph_id="g", status="success")
    await _wait_until(lambda: len(stub.received) == 1)
    assert isinstance(stub.received[0], RunLifecycleEvent)
    assert stub.received[0].status == "success"


async def test_start_is_idempotent() -> None:
    """重复 start 不创建第二个排水任务：每条事件仅落库一次。"""
    stub = StubStore()
    collector = MonitoringCollector(cast("MetricStore", stub), source="driver")
    collector.start()
    collector.start()
    collector.on_worker_heartbeat(worker_id="w", inflight=0, concurrency=1)
    await _wait_until(lambda: len(stub.received) == 1)
    await collector.aclose()


async def test_aclose_drains_remaining_without_start() -> None:
    """未启动排水任务时 aclose 也能排空队列剩余事件。"""
    stub = StubStore()
    collector = MonitoringCollector(cast("MetricStore", stub), source="monitor")
    collector.on_worker_heartbeat(worker_id="w1", inflight=0, concurrency=1)
    collector.on_worker_heartbeat(worker_id="w2", inflight=1, concurrency=2)
    await collector.aclose()
    assert len(stub.received) == 2
    assert collector.dropped == 0


async def test_aclose_is_idempotent() -> None:
    collector = MonitoringCollector(_stub(), source="driver")
    await collector.aclose()
    await collector.aclose()


# ---- 令牌 max 合并与 task_end flush ----


async def test_token_usage_merged_by_task_and_flushed_on_task_end() -> None:
    """同 task 多次上报取各计数最大值；task_end flush 后累加器清零。"""
    stub = StubStore()
    collector = MonitoringCollector(cast("MetricStore", stub), source="worker")
    collector.on_task_start(
        run_id="r", thread_id="t", graph_id="g", task_id="t1", node="chat",
        enqueued_ts=time.time() - 0.01,
    )
    collector.on_token_usage(
        task_id="t1", run_id="r", thread_id="t", graph_id="g", node="chat",
        input_tokens=10, output_tokens=2, total_tokens=12,
    )
    collector.on_token_usage(
        task_id="t1", run_id="r", thread_id="t", graph_id="g", node="chat",
        input_tokens=25, output_tokens=1, total_tokens=26,
    )
    collector.on_task_end(
        run_id="r", thread_id="t", graph_id="g", task_id="t1", node="chat",
        outcome="return", duration_ms=50.0,
    )
    await collector.aclose()
    spans = [e for e in stub.received if isinstance(e, NodeSpanEvent)]
    tokens = [e for e in stub.received if isinstance(e, TokenUsageEvent)]
    assert len(spans) == 1 and spans[0].queue_ms > 0.0
    assert len(tokens) == 1
    assert (tokens[0].input_tokens, tokens[0].output_tokens, tokens[0].total_tokens) == (
        25, 2, 26,
    )


async def test_task_end_without_start_has_zero_queue_and_no_token() -> None:
    """无 on_task_start 时 span.queue_ms 为 0；无令牌累计则不产生 token 事件；
    重复 task_end 不二次 flush。"""
    stub = StubStore()
    collector = MonitoringCollector(cast("MetricStore", stub), source="worker")
    collector.on_task_end(
        run_id="r", thread_id="t", graph_id="g", task_id="t1", node="chat",
        outcome="return", duration_ms=10.0,
    )
    collector.on_task_end(
        run_id="r", thread_id="t", graph_id="g", task_id="t1", node="chat",
        outcome="error", duration_ms=12.0, error="x",
    )
    await collector.aclose()
    spans = [e for e in stub.received if isinstance(e, NodeSpanEvent)]
    tokens = [e for e in stub.received if isinstance(e, TokenUsageEvent)]
    assert len(spans) == 2 and all(span.queue_ms == 0.0 for span in spans)
    assert tokens == []


async def test_task_start_overwrites_redelivery() -> None:
    """XAUTOCLAIM 重投同 task 时以最新一次 start 的排队信息为准。"""
    stub = StubStore()
    collector = MonitoringCollector(cast("MetricStore", stub), source="worker")
    collector.on_task_start(
        run_id="r", thread_id="t", graph_id="g", task_id="t1", node="n",
        enqueued_ts=time.time() - 5.0,
    )
    collector.on_task_start(
        run_id="r", thread_id="t", graph_id="g", task_id="t1", node="n",
        enqueued_ts=time.time() - 0.001,
    )
    collector.on_task_end(
        run_id="r", thread_id="t", graph_id="g", task_id="t1", node="n",
        outcome="return", duration_ms=1.0,
    )
    await collector.aclose()
    spans = [e for e in stub.received if isinstance(e, NodeSpanEvent)]
    assert len(spans) == 1 and spans[0].queue_ms < 5000.0


# ---- RedisProbe（假 client） ----


class FakeRedis:
    """RedisProbe 依赖的窄接口替身：仅实现 snapshot 用到的命令。"""

    def __init__(self) -> None:
        self.stream_lengths: dict[str, int] = {}
        self.pending_count = 0
        self.oldest_idle_ms: float | None = None
        self.scan_keys: list[str] = []
        self.worker_ids: set[bytes] = set()
        self.heartbeat_values: dict[str, bytes | None] = {}

    async def xlen(self, key: str) -> int:
        return self.stream_lengths.get(key, 0)

    async def xpending(self, key: str, group: str) -> dict[str, Any]:
        return {"pending": self.pending_count}

    async def xpending_range(
        self, key: str, group: str, min: str, max: str, count: int
    ) -> list[dict[str, Any]]:
        if self.oldest_idle_ms is None:
            return []
        return [{"time_since_delivered": self.oldest_idle_ms}]

    def scan_iter(self, match: str, count: int) -> AsyncIterator[str]:
        async def _iterate() -> AsyncIterator[str]:
            for key in self.scan_keys:
                if fnmatch.fnmatch(key, match):
                    yield key

        return _iterate()

    async def smembers(self, key: str) -> set[bytes]:
        return set(self.worker_ids)

    async def get(self, key: str) -> bytes | None:
        return self.heartbeat_values.get(key)


async def test_probe_snapshot_shapes() -> None:
    client = FakeRedis()
    client.stream_lengths["slg:tasks"] = 3
    client.stream_lengths["slg:ctrl"] = 1
    client.pending_count = 2
    client.oldest_idle_ms = 1500.0
    client.scan_keys = ["slg:thread-active:a", "slg:thread-active:b", "other:key"]
    client.worker_ids = {b"w-2", b"w-1"}
    client.heartbeat_values = {
        "slg:mon:worker-hb:w-1": json.dumps(
            {"ts": 1781136000.0, "inflight": 3, "concurrency": 32}
        ).encode(),
    }
    snapshot = await RedisProbe(
        cast("Redis", client), broker_prefix="slg", monitor_prefix="slg:mon"
    ).snapshot()
    assert isinstance(snapshot, QueueSnapshot)
    assert snapshot.tasks_length == 3
    assert snapshot.ctrl_length == 1
    assert snapshot.pending_tasks == 2
    assert snapshot.oldest_pending_ms == 1500.0
    assert snapshot.active_runs == 2
    assert [w.worker_id for w in snapshot.workers] == ["w-1", "w-2"]
    alive = snapshot.workers[0]
    assert alive == WorkerInfo(
        worker_id="w-1", inflight=3, concurrency=32, last_seen_ts=1781136000.0, alive=True
    )
    stale = snapshot.workers[1]
    assert stale == WorkerInfo(
        worker_id="w-2", inflight=0, concurrency=0, last_seen_ts=None, alive=False
    )


async def test_probe_heartbeat_plain_timestamp_payload() -> None:
    """心跳值为纯数值（仅时间戳）时兼容：alive 且遥测字段置零。"""
    client = FakeRedis()
    client.worker_ids = {b"w-1"}
    client.heartbeat_values = {"slg:mon:worker-hb:w-1": b"1781136000.0"}
    snapshot = await RedisProbe(cast("Redis", client)).snapshot()
    assert snapshot.workers == [
        WorkerInfo(
            worker_id="w-1", inflight=0, concurrency=0, last_seen_ts=1781136000.0, alive=True
        )
    ]


async def test_probe_empty_redis_is_zeroed() -> None:
    """空 Redis：各深度为 0、无 worker、PEL 空。"""
    snapshot = await RedisProbe(cast("Redis", FakeRedis())).snapshot()
    assert snapshot.tasks_length == 0
    assert snapshot.ctrl_length == 0
    assert snapshot.pending_tasks == 0
    assert snapshot.oldest_pending_ms == 0.0
    assert snapshot.active_runs == 0
    assert snapshot.workers == []
