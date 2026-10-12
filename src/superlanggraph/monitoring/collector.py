"""监控采集器与探针：钩子入队（同步、非阻塞、绝不抛异常）+ 后台排水 + Redis 轮询。

- MonitoringCollector：driver / worker 侧的事件入口。钩子只做内存入队，满时丢弃
  最旧；排水后台任务逐条送 store。令牌用量按 task_id 做 max 合并（流式分片可能
  重复上报累计值），task_end 时统一 flush。
- RedisProbe：server 侧 monitor 后台角色周期调用，读 broker 流键与 monitoring
  自有键生成队列快照。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Literal, cast

from redis.asyncio import Redis

from superlanggraph.monitoring.events import (
    OUTCOME,
    RUN_STATUS,
    MonitorEvent,
    NodeSpanEvent,
    RunLifecycleEvent,
    TokenUsageEvent,
    WorkerHeartbeatEvent,
)
from superlanggraph.monitoring.store import MetricStore, QueueSnapshot, WorkerInfo


@dataclass(slots=True)
class _PendingSpan:
    """task_start 时暂存的排队信息，task_end 时并入 NodeSpanEvent。"""

    run_id: str
    thread_id: str
    graph_id: str
    node: str
    queue_ms: float


@dataclass(slots=True)
class _TokenAcc:
    """同一 task 内多次 token 上报的 max 合并累积器。"""

    run_id: str
    thread_id: str
    graph_id: str
    node: str | None
    input_tokens: int
    output_tokens: int
    total_tokens: int


class MonitoringCollector:
    """进程内单例的事件采集器；store 由组合根注入（MetricStore 协议任一实现）。"""

    def __init__(
        self,
        store: MetricStore,
        *,
        source: Literal["driver", "worker", "monitor"],
        queue_size: int = 10_000,
    ) -> None:
        self._store = store
        self._source: Literal["driver", "worker", "monitor"] = source
        self._queue: asyncio.Queue[MonitorEvent] = asyncio.Queue(maxsize=queue_size)
        self._dropped = 0
        self._drain_task: asyncio.Task[None] | None = None
        self._pending_spans: dict[str, _PendingSpan] = {}
        self._token_acc: dict[str, _TokenAcc] = {}

    def start(self) -> None:
        """创建排水后台任务；幂等。须在事件循环内调用（start_background 中）。"""
        if self._drain_task is not None and not self._drain_task.done():
            return
        self._drain_task = asyncio.create_task(self._drain_loop())

    async def aclose(self) -> None:
        """取消排水任务并排空队列（剩余事件尽力落库）；幂等。"""
        task, self._drain_task = self._drain_task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        while True:
            try:
                event = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            with contextlib.suppress(Exception):
                await self._store.record_event(event)

    @property
    def dropped(self) -> int:
        """背压丢弃的事件总数（自身诊断）。"""
        return self._dropped

    # ---- 钩子（供 driver / worker 调用，全部同步、非阻塞、绝不抛异常） ----

    def on_run_lifecycle(
        self,
        *,
        run_id: str,
        thread_id: str,
        graph_id: str,
        status: str,
        error: str | None = None,
        duration_ms: float | None = None,
    ) -> None:
        # 钩子绝不抛异常：监控失败不影响业务。
        with contextlib.suppress(Exception):
            self._enqueue(
                RunLifecycleEvent(
                    event_id=uuid.uuid4().hex,
                    ts=time.time(),
                    source=self._source,
                    run_id=run_id,
                    thread_id=thread_id,
                    graph_id=graph_id,
                    status=cast("RUN_STATUS", status),
                    error=error,
                    duration_ms=duration_ms,
                )
            )

    def on_task_start(
        self,
        *,
        run_id: str,
        thread_id: str,
        graph_id: str,
        task_id: str,
        node: str,
        enqueued_ts: float,
    ) -> None:
        """enqueued_ts 取 Envelope.ts（driver 构造任务信封时的 time.time()）。"""
        with contextlib.suppress(Exception):
            queue_ms = max((time.time() - enqueued_ts) * 1000.0, 0.0)
            # XAUTOCLAIM 重投同一 task 时以最新一次 start 为准，直接覆盖。
            self._pending_spans[task_id] = _PendingSpan(
                run_id=run_id,
                thread_id=thread_id,
                graph_id=graph_id,
                node=node,
                queue_ms=queue_ms,
            )

    def on_task_end(
        self,
        *,
        run_id: str,
        thread_id: str,
        graph_id: str,
        task_id: str,
        node: str,
        outcome: str,
        duration_ms: float,
        error: str | None = None,
    ) -> None:
        """同时 flush 该 task_id 的令牌累计。"""
        with contextlib.suppress(Exception):
            span = self._pending_spans.pop(task_id, None)
            queue_ms = span.queue_ms if span is not None else 0.0
            self._enqueue(
                NodeSpanEvent(
                    event_id=uuid.uuid4().hex,
                    ts=time.time(),
                    source=self._source,
                    run_id=run_id,
                    thread_id=thread_id,
                    graph_id=graph_id,
                    task_id=task_id,
                    node=node,
                    queue_ms=queue_ms,
                    duration_ms=duration_ms,
                    outcome=cast("OUTCOME", outcome),
                    error=error,
                )
            )
            self._flush_tokens(task_id)

    def on_token_usage(
        self,
        *,
        task_id: str,
        run_id: str,
        thread_id: str,
        graph_id: str,
        node: str,
        input_tokens: int,
        output_tokens: int,
        total_tokens: int,
    ) -> None:
        """按 task_id 做 max 合并（流式分片可能重复上报累计值），task_end 时 flush。"""
        with contextlib.suppress(Exception):
            acc = self._token_acc.get(task_id)
            if acc is None:
                self._token_acc[task_id] = _TokenAcc(
                    run_id=run_id,
                    thread_id=thread_id,
                    graph_id=graph_id,
                    node=node,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    total_tokens=total_tokens,
                )
                return
            acc.node = node
            acc.input_tokens = max(acc.input_tokens, input_tokens)
            acc.output_tokens = max(acc.output_tokens, output_tokens)
            acc.total_tokens = max(acc.total_tokens, total_tokens)

    def on_worker_heartbeat(self, *, worker_id: str, inflight: int, concurrency: int) -> None:
        with contextlib.suppress(Exception):
            self._enqueue(
                WorkerHeartbeatEvent(
                    event_id=uuid.uuid4().hex,
                    ts=time.time(),
                    source=self._source,
                    worker_id=worker_id,
                    inflight=inflight,
                    concurrency=concurrency,
                )
            )

    # ---- 内部 ----

    def _enqueue(self, event: MonitorEvent) -> None:
        """入队；满时丢弃最旧一条再入队，丢弃计入 dropped。"""
        try:
            self._queue.put_nowait(event)
            return
        except asyncio.QueueFull:
            pass
        with contextlib.suppress(asyncio.QueueEmpty):
            self._queue.get_nowait()
        self._dropped += 1
        with contextlib.suppress(asyncio.QueueFull):  # 队列 size >= 1 时不可达
            self._queue.put_nowait(event)

    def _flush_tokens(self, task_id: str) -> None:
        acc = self._token_acc.pop(task_id, None)
        if acc is None:
            return
        self._enqueue(
            TokenUsageEvent(
                event_id=uuid.uuid4().hex,
                ts=time.time(),
                source=self._source,
                run_id=acc.run_id,
                thread_id=acc.thread_id,
                graph_id=acc.graph_id,
                node=acc.node,
                input_tokens=acc.input_tokens,
                output_tokens=acc.output_tokens,
                total_tokens=acc.total_tokens,
            )
        )

    async def _drain_loop(self) -> None:
        """逐条消费队列送 store；store 单次失败被吞，排水循环保持存活。"""
        while True:
            event = await self._queue.get()
            with contextlib.suppress(Exception):
                await self._store.record_event(event)


class RedisProbe:
    """队列深度 / PEL / 活跃 run / worker 存活的周期探针。

    读取两类键：broker 流键（``{broker_prefix}:tasks`` / ``:ctrl`` /
    ``:thread-active:*``）与 monitoring 自有键（``{monitor_prefix}:workers`` /
    ``:worker-hb:{worker_id}``，由 store 的 record_event 维护）。
    """

    def __init__(
        self,
        client: Redis,
        *,
        broker_prefix: str = "slg",
        group: str = "drivers",
        monitor_prefix: str = "slg:mon",
    ) -> None:
        self._client = client
        self._broker_prefix = broker_prefix
        self._group = group
        self._monitor_prefix = monitor_prefix

    async def snapshot(self) -> QueueSnapshot:
        """XLEN tasks/ctrl；XPENDING count 与最老条目 idle；SCAN thread-active 计活跃
        run；SMEMBERS worker 注册表 + 心跳键得存活列表。"""
        tasks_length = int(await self._client.xlen(f"{self._broker_prefix}:tasks"))
        ctrl_length = int(await self._client.xlen(f"{self._broker_prefix}:ctrl"))
        pending_tasks, oldest_pending_ms = await self._pending_stats()
        active_runs = await self._count_active_runs()
        return QueueSnapshot(
            ts=time.time(),
            tasks_length=tasks_length,
            ctrl_length=ctrl_length,
            pending_tasks=pending_tasks,
            oldest_pending_ms=oldest_pending_ms,
            active_runs=active_runs,
            workers=await self._worker_infos(),
        )

    async def _pending_stats(self) -> tuple[int, float]:
        """PEL 摘要取未确认数；range 取最老条目的 idle 毫秒（PEL 空时为 0）。"""
        summary = await self._client.xpending(f"{self._broker_prefix}:tasks", self._group)
        count = int(summary.get("pending", 0))
        if count == 0:
            return 0, 0.0
        oldest = cast(
            "list[dict[str, Any]]",
            await self._client.xpending_range(
                f"{self._broker_prefix}:tasks", self._group, min="-", max="+", count=1
            ),
        )
        if not oldest:
            return count, 0.0
        return count, float(oldest[0]["time_since_delivered"])

    async def _count_active_runs(self) -> int:
        """SCAN {broker_prefix}:thread-active:* 计活跃 run（10s 周期、模式窄）。"""
        count = 0
        async for _key in self._client.scan_iter(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            match=f"{self._broker_prefix}:thread-active:*", count=100
        ):
            count += 1
        return count

    async def _worker_infos(self) -> list[WorkerInfo]:
        raw_ids = cast("set[Any]", await self._client.smembers(f"{self._monitor_prefix}:workers"))
        infos = [await self._worker_info(_decode_value(raw)) for raw in raw_ids]
        infos.sort(key=lambda info: info.worker_id)
        return infos

    async def _worker_info(self, worker_id: str) -> WorkerInfo:
        """心跳键存在即存活；值携带最近心跳载荷（JSON {ts, inflight, concurrency}），
        兼容纯数值（仅时间戳）。"""
        raw = await self._client.get(f"{self._monitor_prefix}:worker-hb:{worker_id}")
        if raw is None:
            # 注册表残留但心跳已过期：存活判定为否，遥测字段置空。
            return WorkerInfo(
                worker_id=worker_id, inflight=0, concurrency=0, last_seen_ts=None, alive=False
            )
        last_seen: float | None = None
        inflight = 0
        concurrency = 0
        try:
            payload: Any = json.loads(_decode_value(raw))
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, dict):
            fields = cast("dict[str, Any]", payload)
            last_seen = float(fields.get("ts", 0.0))
            inflight = int(fields.get("inflight", 0))
            concurrency = int(fields.get("concurrency", 0))
        elif isinstance(payload, int | float):
            last_seen = float(payload)
        return WorkerInfo(
            worker_id=worker_id,
            inflight=inflight,
            concurrency=concurrency,
            last_seen_ts=last_seen,
            alive=True,
        )


def _decode_value(value: Any) -> str:
    """redis-py 宽松响应收窄：bytes / str 统一转 str。"""
    return value.decode() if isinstance(value, bytes) else str(value)
