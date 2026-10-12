"""监控指标聚合存储：Redis 实现与进程内实现，共享同一 MetricStore 协议。

键布局（P = prefix，默认 ``slg:mon``）见 docs/monitoring-design.md §6.1：
分钟桶 Hash（runs/nodes/tokens/queue）、每图原始样本 ZSet（1h、cap 500）、
单 run 汇总 Hash 与时间线 ZSet、最近错误 List（cap 100）、事件日志 ZSet
（score=seq，1h、cap event_log_size）、pub/sub 广播、worker 注册表与心跳键
（TTL 15s）。写路径统一走 pipeline；桶键 TTL 每次写入时刷新——桶只在自身
分钟内被写，刷新与「首次写入后 retention」实际等价。

redis-py 的宽松返回类型照 ``broker/redis.py`` 惯例在模块边界收敛（bytes → str、
数值字符串还原），类型噪音不外溢到服务层。状态分布 Hash 用 HINCRBY 迁移
（旧态 -1 / 新态 +1），首次见到 run 视为从 pending 出发；pending/running 两字段
由探针周期覆盖纠偏。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
import time
from collections.abc import AsyncGenerator, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Protocol, cast

from pydantic import BaseModel
from redis.asyncio import Redis
from redis.asyncio.client import Pipeline

from superlanggraph.monitoring.events import (
    MonitorEvent,
    NodeSpanEvent,
    RunLifecycleEvent,
    TokenUsageEvent,
    WorkerHeartbeatEvent,
)

if TYPE_CHECKING:
    # collector 顶部运行时依赖本模块符号，这里必须延迟导入以避免顶层循环。
    from superlanggraph.monitoring.collector import RedisProbe

type RawMapping = dict[bytes | str, bytes | str]
"""HGETALL 等命令在 decode_responses=False 下的运行时形状。"""

_RUN_STATUSES = ("pending", "running", "error", "success", "timeout", "interrupted")
_TERMINAL_STATUSES = frozenset({"success", "error", "timeout", "interrupted"})
_RECENT_ERRORS_CAP = 100
_MESSAGE_MAX_CHARS = 500
_SPANS_TTL_SECONDS = 3600
_LOG_TTL_SECONDS = 3600
_HEARTBEAT_TTL_SECONDS = 15
_MEMORY_RETENTION_SECONDS = 24 * 3600
_SUBSCRIBER_QUEUE_SIZE = 256
_NODE_METRIC_SLOTS = ("count", "total_ms", "max_ms", "errors")


# ---- 快照与序列模型（§6.2） ----


class WorkerInfo(BaseModel):
    worker_id: str
    inflight: int
    concurrency: int
    last_seen_ts: float | None = None
    alive: bool


class QueueSnapshot(BaseModel):
    ts: float
    tasks_length: int
    ctrl_length: int
    pending_tasks: int
    oldest_pending_ms: float
    active_runs: int
    workers: list[WorkerInfo]


class NodeStat(BaseModel):
    """窗口内按 (graph_id, node) 聚合。"""

    graph_id: str
    node: str
    count: int
    total_ms: float
    avg_ms: float
    max_ms: float
    errors: int


class TokenTotals(BaseModel):
    input_tokens: int
    output_tokens: int
    total_tokens: int


class ErrorSummary(BaseModel):
    errors: int
    runs_finished: int
    rate: float
    recent: list[dict[str, Any]]


class MonitorSnapshot(BaseModel):
    generated_at: float
    window_minutes: int
    status_counts: dict[str, int]
    queue: QueueSnapshot | None
    token_totals: TokenTotals
    error_summary: ErrorSummary
    node_top: list[NodeStat]


class MinutePoint(BaseModel):
    bucket_ts: int
    runs_started: int
    runs_finished: int
    errors: int
    input_tokens: int
    output_tokens: int
    queue_depth_avg: float | None = None
    queue_depth_last: int | None = None


class SequencedEvent(BaseModel):
    seq: int
    event: MonitorEvent


def p95(spans: Sequence[NodeSpanEvent], node: str | None = None) -> float | None:
    """对原始样本近端排序计算 p95（最近邻秩 ceil(0.95*n)）；空样本返回 None。"""
    durations = sorted(s.duration_ms for s in spans if node is None or s.node == node)
    if not durations:
        return None
    rank = math.ceil(len(durations) * 0.95)
    return durations[min(rank, len(durations)) - 1]


# ---- 模块级纯助手（两个实现共享） ----


def _align_bucket(ts: float) -> int:
    """对齐到分钟起点的 epoch 秒。"""
    return int(ts // 60) * 60


def _bucket_range(now: float, minutes: int) -> list[int]:
    """最近 minutes 个分钟桶（含当前分钟），升序。"""
    current = _align_bucket(now)
    return [current - 60 * offset for offset in range(minutes - 1, -1, -1)]


def _truncate(message: str | None) -> str | None:
    if message is None:
        return None
    return message if len(message) <= _MESSAGE_MAX_CHARS else message[:_MESSAGE_MAX_CHARS]


def _node_field(graph_id: str, node: str, metric: str) -> str:
    return f"{graph_id}|{node}|{metric}"


def _decode_value(value: Any) -> str:
    """redis-py 宽松返回值（bytes / str / 其他）统一收敛为 str。"""
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


def _stringify(mapping: Mapping[bytes | str, bytes | str]) -> dict[str, str]:
    return {_decode_value(key): _decode_value(value) for key, value in mapping.items()}


def _to_int(value: Any) -> int:
    return int(float(value))


def _to_float(value: Any) -> float:
    return float(value)


def _minute_point(
    bucket_ts: int,
    runs: Mapping[str, Any],
    tokens: Mapping[str, Any],
    queue: Mapping[str, Any],
) -> MinutePoint:
    samples = _to_int(queue.get("depth_samples", 0))
    depth_sum = _to_float(queue.get("depth_sum", 0.0))
    depth_last = queue.get("depth_last")
    return MinutePoint(
        bucket_ts=bucket_ts,
        runs_started=_to_int(runs.get("started", 0)),
        runs_finished=_to_int(runs.get("finished", 0)),
        errors=_to_int(runs.get("errors", 0)),
        input_tokens=_to_int(tokens.get("input_tokens", 0)),
        output_tokens=_to_int(tokens.get("output_tokens", 0)),
        queue_depth_avg=depth_sum / samples if samples > 0 else None,
        queue_depth_last=_to_int(depth_last) if depth_last is not None else None,
    )


def _merge_node_buckets(
    rows: Iterable[Mapping[str, Any]], graph_filter: str | None
) -> dict[tuple[str, str], list[float]]:
    """跨分钟桶合并节点字段，返回 (graph, node) → [count, total, max, errors]。"""
    merged: dict[tuple[str, str], list[float]] = {}
    for row in rows:
        for field, value in row.items():
            graph, node, metric = _decode_value(field).rsplit("|", 2)
            if graph_filter is not None and graph != graph_filter:
                continue
            slot = merged.setdefault((graph, node), [0.0, 0.0, 0.0, 0.0])
            index = _NODE_METRIC_SLOTS.index(metric)
            amount = _to_float(value)
            if metric == "max_ms":
                slot[index] = max(slot[index], amount)
            else:
                slot[index] += amount
    return merged


def _node_stat(graph_id: str, node: str, slot: list[float]) -> NodeStat:
    count = int(slot[0])
    total_ms = slot[1]
    return NodeStat(
        graph_id=graph_id,
        node=node,
        count=count,
        total_ms=total_ms,
        avg_ms=total_ms / count if count > 0 else 0.0,
        max_ms=slot[2],
        errors=int(slot[3]),
    )


def _ranked_node_stats(
    rows: Iterable[Mapping[str, Any]], graph_filter: str | None, limit: int
) -> list[NodeStat]:
    stats = [
        _node_stat(graph, node, slot)
        for (graph, node), slot in _merge_node_buckets(rows, graph_filter).items()
    ]
    stats.sort(key=lambda stat: stat.total_ms, reverse=True)
    return stats[:limit]


def _token_totals(rows: Iterable[Mapping[str, Any]]) -> TokenTotals:
    materialized = list(rows)
    return TokenTotals(
        input_tokens=sum(_to_int(row.get("input_tokens", 0)) for row in materialized),
        output_tokens=sum(_to_int(row.get("output_tokens", 0)) for row in materialized),
        total_tokens=sum(_to_int(row.get("total_tokens", 0)) for row in materialized),
    )


def _window_errors(entries: Iterable[dict[str, Any]], window_start: float) -> list[dict[str, Any]]:
    return [entry for entry in entries if _to_float(entry.get("ts", 0.0)) >= window_start]


class MetricStore(Protocol):
    """监控存储协议；采集器与路由层仅依赖此抽象。"""

    async def record_event(self, event: MonitorEvent) -> None:
        """消费一条监控事件：写桶/ZSet/错误列表/事件日志并 PUBLISH。内部分配 seq。"""
        ...

    async def record_probe(self, snapshot: QueueSnapshot) -> None:
        """写入探针快照：{P}:probe 覆盖、分钟桶采样、pending/running 纠偏、worker 注册表清理。"""
        ...

    async def poll_probe(self) -> QueueSnapshot:
        """执行一轮 Redis 探针（供 monitor 后台角色调用）；RedisMetricStore 内含 RedisProbe。"""
        ...

    async def overview(self, *, window_minutes: int = 60) -> MonitorSnapshot:
        """窗口内总览快照：状态分布、队列、令牌合计、错误汇总、节点 top 10。"""
        ...

    async def minute_series(self, *, minutes: int = 60) -> list[MinutePoint]:
        """时间窗内逐分钟点，缺口补零，按 bucket_ts 升序。"""
        ...

    async def node_stats(
        self, *, graph_id: str | None = None, minutes: int = 60, limit: int = 50
    ) -> list[NodeStat]:
        """窗口内节点桶合并；graph_id 过滤；按 total_ms 降序。"""
        ...

    async def recent_spans(self, *, graph_id: str | None, limit: int = 200) -> list[NodeSpanEvent]:
        """每图最近原始样本（多图时按 ts 归并），降序。"""
        ...

    async def run_summary(self, run_id: str) -> dict[str, Any] | None:
        """{P}:run:{run_id} Hash + {P}:run-spans:{run_id} 时间线；无记录返回 None。"""
        ...

    async def recent_events(
        self, *, limit: int = 200, after_seq: int | None = None
    ) -> list[SequencedEvent]:
        """事件日志读取；after_seq 给定时仅返回 seq 更大者，升序。"""
        ...

    def subscribe(self) -> AsyncGenerator[SequencedEvent, None]:
        """订阅 {P}:bus 实时广播（尾随语义，不自然结束）。"""
        ...

    async def aclose(self) -> None:
        """释放 store 持有资源；订阅连接由各自生成器负责关闭。"""
        ...


class RedisMetricStore:
    """Redis 实现；client 由组合根注入（decode_responses=False，键值为 bytes）。"""

    def __init__(
        self,
        client: Redis,
        *,
        prefix: str = "slg:mon",
        broker_prefix: str = "slg",
        group: str = "drivers",
        retention_hours: int = 24,
        spans_per_graph: int = 500,
        event_log_size: int = 500,
    ) -> None:
        self._client = client
        self._prefix = prefix
        self._broker_prefix = broker_prefix
        self._group = group
        self._retention_seconds = retention_hours * 3600
        self._spans_per_graph = spans_per_graph
        self._event_log_size = event_log_size
        self._probe_impl: RedisProbe | None = None

    # ---- MetricStore 接口 ----

    async def record_event(self, event: MonitorEvent) -> None:
        seq = int(await self._client.incr(self._seq_key))
        data = SequencedEvent(seq=seq, event=event).model_dump_json()
        bucket_ts = _align_bucket(event.ts)
        pipe = self._client.pipeline()
        if isinstance(event, RunLifecycleEvent):
            await self._write_run_lifecycle(pipe, event, bucket_ts)
        elif isinstance(event, NodeSpanEvent):
            raw_max = await self._client.hget(
                self._bucket_nodes_key(bucket_ts), _node_field(event.graph_id, event.node, "max_ms")
            )
            old_max = _to_float(raw_max) if raw_max is not None else None
            self._write_node_span(pipe, event, bucket_ts, old_max)
        elif isinstance(event, TokenUsageEvent):
            self._write_token_usage(pipe, event, bucket_ts)
        else:
            assert isinstance(event, WorkerHeartbeatEvent)
            self._write_worker_heartbeat(pipe, event)
        pipe.zadd(self._log_key, {data: seq})
        pipe.zremrangebyrank(self._log_key, 0, -(self._event_log_size + 1))
        pipe.expire(self._log_key, _LOG_TTL_SECONDS)
        pipe.publish(self._bus_key, data)  # pyright: ignore[reportUnknownMemberType]
        await pipe.execute()

    async def record_probe(self, snapshot: QueueSnapshot) -> None:
        expired: list[str] = []
        members = await self._client.smembers(self._workers_key)
        for raw in members:
            worker_id = _decode_value(raw)
            if not await self._client.exists(self._worker_hb_key(worker_id)):
                expired.append(worker_id)
        depth = snapshot.tasks_length + snapshot.ctrl_length + snapshot.pending_tasks
        queue_bucket = self._bucket_queue_key(_align_bucket(snapshot.ts))
        pipe = self._client.pipeline()
        pipe.hset(self._probe_key, mapping={"snapshot": snapshot.model_dump_json()})
        pipe.hincrbyfloat(queue_bucket, "depth_sum", float(depth))
        pipe.hincrby(queue_bucket, "depth_samples", 1)
        pipe.hset(
            queue_bucket,
            mapping={"depth_last": str(depth), "active_last": str(snapshot.active_runs)},
        )
        pipe.expire(queue_bucket, self._retention_seconds)
        # 纠偏：thread-active 覆盖 pending+running 全部在途 run，事件口径下 pending 恒空，
        # 故 running 以 active_runs 覆盖、pending 清零（终态计数保持累计口径不动）。
        pipe.hset(
            self._status_key,
            mapping={"running": str(snapshot.active_runs), "pending": "0"},
        )
        if expired:
            pipe.srem(self._workers_key, *expired)
        await pipe.execute()

    async def poll_probe(self) -> QueueSnapshot:
        if self._probe_impl is None:
            # collector 顶部依赖本模块符号，只能延迟导入以避免顶层循环。
            from superlanggraph.monitoring.collector import RedisProbe  # noqa: PLC0415

            self._probe_impl = RedisProbe(
                self._client,
                broker_prefix=self._broker_prefix,
                group=self._group,
                monitor_prefix=self._prefix,
            )
        return await self._probe_impl.snapshot()

    async def overview(self, *, window_minutes: int = 60) -> MonitorSnapshot:
        now = time.time()
        buckets = _bucket_range(now, window_minutes)
        pipe = self._client.pipeline()
        for ts in buckets:
            pipe.hgetall(self._bucket_runs_key(ts))
            pipe.hgetall(self._bucket_tokens_key(ts))
            pipe.hgetall(self._bucket_nodes_key(ts))
        pipe.hgetall(self._status_key)
        pipe.hget(self._probe_key, "snapshot")
        pipe.lrange(self._errors_key, 0, -1)
        rows = await pipe.execute()
        base = len(buckets) * 3
        runs_rows = [_stringify(cast("RawMapping", rows[i * 3])) for i in range(len(buckets))]
        tokens_rows = [_stringify(cast("RawMapping", rows[i * 3 + 1])) for i in range(len(buckets))]
        nodes_rows = [_stringify(cast("RawMapping", rows[i * 3 + 2])) for i in range(len(buckets))]
        status_raw = _stringify(cast("RawMapping", rows[base]))
        probe_raw = cast("bytes | str | None", rows[base + 1])
        errors_raw = cast("list[bytes | str]", rows[base + 2])
        runs_finished = sum(_to_int(row.get("finished", 0)) for row in runs_rows)
        errors = sum(_to_int(row.get("errors", 0)) for row in runs_rows)
        recent = _window_errors(
            (cast("dict[str, Any]", json.loads(_decode_value(raw))) for raw in errors_raw),
            now - window_minutes * 60,
        )
        queue = (
            QueueSnapshot.model_validate_json(_decode_value(probe_raw))
            if probe_raw is not None
            else None
        )
        return MonitorSnapshot(
            generated_at=now,
            window_minutes=window_minutes,
            status_counts={status: _to_int(status_raw.get(status, 0)) for status in _RUN_STATUSES},
            queue=queue,
            token_totals=_token_totals(tokens_rows),
            error_summary=ErrorSummary(
                errors=errors,
                runs_finished=runs_finished,
                rate=errors / max(runs_finished, 1),
                recent=recent,
            ),
            node_top=_ranked_node_stats(nodes_rows, None, 10),
        )

    async def minute_series(self, *, minutes: int = 60) -> list[MinutePoint]:
        buckets = _bucket_range(time.time(), minutes)
        pipe = self._client.pipeline()
        for ts in buckets:
            pipe.hgetall(self._bucket_runs_key(ts))
            pipe.hgetall(self._bucket_tokens_key(ts))
            pipe.hgetall(self._bucket_queue_key(ts))
        rows = await pipe.execute()
        return [
            _minute_point(
                ts,
                _stringify(cast("RawMapping", rows[index * 3])),
                _stringify(cast("RawMapping", rows[index * 3 + 1])),
                _stringify(cast("RawMapping", rows[index * 3 + 2])),
            )
            for index, ts in enumerate(buckets)
        ]

    async def node_stats(
        self, *, graph_id: str | None = None, minutes: int = 60, limit: int = 50
    ) -> list[NodeStat]:
        buckets = _bucket_range(time.time(), minutes)
        pipe = self._client.pipeline()
        for ts in buckets:
            pipe.hgetall(self._bucket_nodes_key(ts))
        rows = await pipe.execute()
        return _ranked_node_stats(
            (_stringify(cast("RawMapping", row)) for row in rows), graph_id, limit
        )

    async def recent_spans(self, *, graph_id: str | None, limit: int = 200) -> list[NodeSpanEvent]:
        if graph_id is not None:
            members = cast(
                "list[bytes | str]",
                await self._client.zrevrange(  # pyright: ignore[reportUnknownMemberType]
                    self._spans_key(graph_id), 0, limit - 1
                ),
            )
            return [_decode_span(member) for member in members]
        keys: list[str] = []
        async for raw_key in self._client.scan_iter(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            match=f"{self._prefix}:spans:*"
        ):
            keys.append(_decode_value(raw_key))
        merged: list[NodeSpanEvent] = []
        for key in keys:
            members = cast(
                "list[bytes | str]",
                await self._client.zrevrange(  # pyright: ignore[reportUnknownMemberType]
                    key, 0, limit - 1
                ),
            )
            merged.extend(_decode_span(member) for member in members)
        merged.sort(key=lambda span: span.ts, reverse=True)
        return merged[:limit]

    async def run_summary(self, run_id: str) -> dict[str, Any] | None:
        raw = await self._client.hgetall(self._run_key(run_id))
        if not raw:
            return None
        members = cast(
            "list[bytes | str]",
            await self._client.zrange(  # pyright: ignore[reportUnknownMemberType]
                self._run_spans_key(run_id), 0, -1
            ),
        )
        return {
            "summary": _decode_run_summary(_stringify(raw)),
            "spans": [_decode_span(member) for member in members],
        }

    async def recent_events(
        self, *, limit: int = 200, after_seq: int | None = None
    ) -> list[SequencedEvent]:
        if after_seq is None:
            members = cast(
                "list[bytes | str]",
                await self._client.zrange(  # pyright: ignore[reportUnknownMemberType]
                    self._log_key, -limit, -1
                ),
            )
        else:
            members = cast(
                "list[bytes | str]",
                await self._client.zrangebyscore(  # pyright: ignore[reportUnknownMemberType]
                    self._log_key, f"({after_seq}", "+inf", start=0, num=limit
                ),
            )
        return [SequencedEvent.model_validate_json(_decode_value(member)) for member in members]

    def subscribe(self) -> AsyncGenerator[SequencedEvent, None]:
        return self._subscribe_bus()

    async def aclose(self) -> None:
        """无长持资源：订阅 pubsub 由各自生成器的 finally 关闭，此处无需清理。"""

    # ---- 键构造 ----

    @property
    def _seq_key(self) -> str:
        return f"{self._prefix}:seq"

    @property
    def _status_key(self) -> str:
        return f"{self._prefix}:run-status"

    @property
    def _probe_key(self) -> str:
        return f"{self._prefix}:probe"

    @property
    def _workers_key(self) -> str:
        return f"{self._prefix}:workers"

    @property
    def _bus_key(self) -> str:
        return f"{self._prefix}:bus"

    @property
    def _log_key(self) -> str:
        return f"{self._prefix}:log"

    @property
    def _errors_key(self) -> str:
        return f"{self._prefix}:recent-errors"

    def _bucket_runs_key(self, ts: int) -> str:
        return f"{self._prefix}:bucket:{ts}:runs"

    def _bucket_nodes_key(self, ts: int) -> str:
        return f"{self._prefix}:bucket:{ts}:nodes"

    def _bucket_tokens_key(self, ts: int) -> str:
        return f"{self._prefix}:bucket:{ts}:tokens"

    def _bucket_queue_key(self, ts: int) -> str:
        return f"{self._prefix}:bucket:{ts}:queue"

    def _spans_key(self, graph_id: str) -> str:
        return f"{self._prefix}:spans:{graph_id}"

    def _run_key(self, run_id: str) -> str:
        return f"{self._prefix}:run:{run_id}"

    def _run_spans_key(self, run_id: str) -> str:
        return f"{self._prefix}:run-spans:{run_id}"

    def _worker_hb_key(self, worker_id: str) -> str:
        return f"{self._prefix}:worker-hb:{worker_id}"

    # ---- 写路径（pipeline 命令按类型分派） ----

    async def _write_run_lifecycle(
        self, pipe: Pipeline, event: RunLifecycleEvent, bucket_ts: int
    ) -> None:
        run_key = self._run_key(event.run_id)
        raw_status = await self._client.hget(run_key, "status")
        old_status = _decode_value(raw_status) if raw_status is not None else None
        mapping: dict[str, str] = {
            "run_id": event.run_id,
            "thread_id": event.thread_id,
            "graph_id": event.graph_id,
            "status": event.status,
            "updated_ts": repr(event.ts),
        }
        runs_bucket = self._bucket_runs_key(bucket_ts)
        if old_status is None:
            # 首次见到该 run：视为从 pending 出发，进入迁移净计数。
            mapping["started_ts"] = repr(event.ts)
            pipe.hincrby(runs_bucket, "started", 1)
            pipe.hincrby(self._status_key, "pending", 1)
        effective_old = old_status or "pending"
        if effective_old != event.status:
            pipe.hincrby(self._status_key, effective_old, -1)
            pipe.hincrby(self._status_key, event.status, 1)
            if event.status in _TERMINAL_STATUSES:
                pipe.hincrby(runs_bucket, "finished", 1)
                mapping["finished_ts"] = repr(event.ts)
            if event.status == "error":
                pipe.hincrby(runs_bucket, "errors", 1)
                self._push_error(
                    pipe,
                    {
                        "ts": event.ts,
                        "run_id": event.run_id,
                        "graph_id": event.graph_id,
                        "node": None,
                        "message": _truncate(event.error),
                    },
                )
        if event.error:
            mapping["error"] = event.error
        if event.duration_ms is not None:
            mapping["duration_ms"] = repr(event.duration_ms)
        # redis-py 存根的 constrained TypeVar 在 mapping 位置无法匹配具体 dict（存根怪癖），
        # 运行时形状正确，照 broker/redis.py 惯例豁免此行。
        pipe.hset(run_key, mapping=mapping)  # pyright: ignore[reportArgumentType]
        pipe.expire(run_key, self._retention_seconds)

    def _write_node_span(
        self, pipe: Pipeline, event: NodeSpanEvent, bucket_ts: int, old_max_ms: float | None
    ) -> None:
        nodes_bucket = self._bucket_nodes_key(bucket_ts)
        node_prefix = f"{event.graph_id}|{event.node}|"
        pipe.hincrby(nodes_bucket, node_prefix + "count", 1)
        pipe.hincrbyfloat(nodes_bucket, node_prefix + "total_ms", event.duration_ms)
        if event.outcome == "error":
            pipe.hincrby(nodes_bucket, node_prefix + "errors", 1)
            self._push_error(
                pipe,
                {
                    "ts": event.ts,
                    "run_id": event.run_id,
                    "graph_id": event.graph_id,
                    "node": event.node,
                    "message": _truncate(event.error),
                },
            )
        new_max = event.duration_ms if old_max_ms is None else max(old_max_ms, event.duration_ms)
        pipe.hset(nodes_bucket, node_prefix + "max_ms", repr(new_max))
        pipe.expire(nodes_bucket, self._retention_seconds)
        span_json = event.model_dump_json()
        spans_key = self._spans_key(event.graph_id)
        pipe.zadd(spans_key, {span_json: event.ts})
        pipe.zremrangebyrank(spans_key, 0, -(self._spans_per_graph + 1))
        pipe.expire(spans_key, _SPANS_TTL_SECONDS)
        run_spans_key = self._run_spans_key(event.run_id)
        pipe.zadd(run_spans_key, {span_json: event.ts})
        pipe.expire(run_spans_key, self._retention_seconds)
        run_key = self._run_key(event.run_id)
        pipe.hincrby(run_key, "node_count", 1)
        pipe.expire(run_key, self._retention_seconds)

    def _write_token_usage(self, pipe: Pipeline, event: TokenUsageEvent, bucket_ts: int) -> None:
        tokens_bucket = self._bucket_tokens_key(bucket_ts)
        pipe.hincrby(tokens_bucket, "input_tokens", event.input_tokens)
        pipe.hincrby(tokens_bucket, "output_tokens", event.output_tokens)
        pipe.hincrby(tokens_bucket, "total_tokens", event.total_tokens)
        pipe.expire(tokens_bucket, self._retention_seconds)
        run_key = self._run_key(event.run_id)
        pipe.hincrby(run_key, "input_tokens", event.input_tokens)
        pipe.hincrby(run_key, "output_tokens", event.output_tokens)
        pipe.hincrby(run_key, "total_tokens", event.total_tokens)
        pipe.expire(run_key, self._retention_seconds)

    def _write_worker_heartbeat(self, pipe: Pipeline, event: WorkerHeartbeatEvent) -> None:
        pipe.sadd(self._workers_key, event.worker_id)
        pipe.set(
            self._worker_hb_key(event.worker_id),
            json.dumps(
                {"ts": time.time(), "inflight": event.inflight, "concurrency": event.concurrency}
            ),
            ex=_HEARTBEAT_TTL_SECONDS,
        )

    def _push_error(self, pipe: Pipeline, entry: dict[str, Any]) -> None:
        pipe.lpush(self._errors_key, json.dumps(entry, ensure_ascii=False))
        pipe.ltrim(self._errors_key, 0, _RECENT_ERRORS_CAP - 1)
        pipe.expire(self._errors_key, self._retention_seconds)

    async def _subscribe_bus(self) -> AsyncGenerator[SequencedEvent, None]:
        pubsub = self._client.pubsub()  # pyright: ignore[reportUnknownMemberType]
        await pubsub.subscribe(self._bus_key)
        try:
            while True:
                message = cast(
                    "dict[str, Any] | None",
                    await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0),
                )
                if message is None:
                    continue
                raw = message.get("data")
                if raw is None:
                    continue
                yield SequencedEvent.model_validate_json(_decode_value(raw))
        finally:
            with contextlib.suppress(Exception):
                await pubsub.aclose()


def _decode_span(member: bytes | str) -> NodeSpanEvent:
    return NodeSpanEvent.model_validate_json(_decode_value(member))


_RUN_SUMMARY_INT_FIELDS = frozenset({"node_count", "input_tokens", "output_tokens", "total_tokens"})
_RUN_SUMMARY_FLOAT_FIELDS = frozenset({"started_ts", "updated_ts", "finished_ts", "duration_ms"})


def _decode_run_summary(fields: Mapping[str, str]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for key, value in fields.items():
        if key in _RUN_SUMMARY_INT_FIELDS:
            summary[key] = int(float(value))
        elif key in _RUN_SUMMARY_FLOAT_FIELDS:
            summary[key] = float(value)
        elif key == "error":
            summary[key] = value or None
        else:
            summary[key] = value
    return summary


class InMemoryMetricStore:
    """进程内实现：单测与 enabled=False 场景；桶对齐、迁移净计数、纠偏语义与 Redis 版一致。"""

    def __init__(self, *, window_minutes: int = 60, event_log_size: int = 500) -> None:
        self._window_minutes = window_minutes
        self._event_log_size = event_log_size
        self._seq = 0
        self._log: list[SequencedEvent] = []
        self._status_counts: dict[str, int] = dict.fromkeys(_RUN_STATUSES, 0)
        self._bucket_runs: dict[int, dict[str, int]] = {}
        self._bucket_tokens: dict[int, dict[str, int]] = {}
        self._bucket_queue: dict[int, dict[str, float]] = {}
        self._bucket_nodes: dict[int, dict[str, float]] = {}
        self._runs: dict[str, dict[str, Any]] = {}
        self._run_spans: dict[str, list[NodeSpanEvent]] = {}
        self._spans: dict[str, list[NodeSpanEvent]] = {}
        self._recent_errors: list[dict[str, Any]] = []
        self._probe: QueueSnapshot | None = None
        self._workers: dict[str, WorkerInfo] = {}
        self._subscribers: set[asyncio.Queue[SequencedEvent | None]] = set()

    async def record_event(self, event: MonitorEvent) -> None:
        self._seq += 1
        self._log.append(SequencedEvent(seq=self._seq, event=event))
        del self._log[: max(len(self._log) - self._event_log_size, 0)]
        bucket_ts = _align_bucket(event.ts)
        self._gc(bucket_ts)
        if isinstance(event, RunLifecycleEvent):
            self._record_run_lifecycle(event, bucket_ts)
        elif isinstance(event, NodeSpanEvent):
            self._record_node_span(event, bucket_ts)
        elif isinstance(event, TokenUsageEvent):
            self._record_token_usage(event, bucket_ts)
        else:
            assert isinstance(event, WorkerHeartbeatEvent)
            self._workers[event.worker_id] = WorkerInfo(
                worker_id=event.worker_id,
                inflight=event.inflight,
                concurrency=event.concurrency,
                last_seen_ts=event.ts,
                alive=True,
            )
        for queue in self._subscribers:
            if queue.full():
                with contextlib.suppress(asyncio.QueueEmpty):
                    queue.get_nowait()
            queue.put_nowait(SequencedEvent(seq=self._seq, event=event))

    async def record_probe(self, snapshot: QueueSnapshot) -> None:
        self._probe = snapshot
        bucket_ts = _align_bucket(snapshot.ts)
        self._gc(bucket_ts)
        depth = snapshot.tasks_length + snapshot.ctrl_length + snapshot.pending_tasks
        queue_bucket = self._bucket_queue.setdefault(bucket_ts, {})
        queue_bucket["depth_sum"] = queue_bucket.get("depth_sum", 0.0) + depth
        queue_bucket["depth_samples"] = queue_bucket.get("depth_samples", 0.0) + 1
        queue_bucket["depth_last"] = float(depth)
        queue_bucket["active_last"] = float(snapshot.active_runs)
        # 纠偏语义与 Redis 版一致：running 以 active_runs 覆盖，pending 清零。
        self._status_counts["running"] = snapshot.active_runs
        self._status_counts["pending"] = 0
        for worker_id, info in list(self._workers.items()):
            last_seen = info.last_seen_ts
            stale = last_seen is None or snapshot.ts - last_seen > _HEARTBEAT_TTL_SECONDS
            if stale:
                del self._workers[worker_id]

    async def poll_probe(self) -> QueueSnapshot:
        if self._probe is not None:
            return self._probe
        return QueueSnapshot(
            ts=time.time(),
            tasks_length=0,
            ctrl_length=0,
            pending_tasks=0,
            oldest_pending_ms=0.0,
            active_runs=0,
            workers=[],
        )

    async def overview(self, *, window_minutes: int = 60) -> MonitorSnapshot:
        now = time.time()
        buckets = _bucket_range(now, window_minutes)
        runs_rows = [self._bucket_runs.get(ts, {}) for ts in buckets]
        tokens_rows = [self._bucket_tokens.get(ts, {}) for ts in buckets]
        nodes_rows = [self._bucket_nodes.get(ts, {}) for ts in buckets]
        runs_finished = sum(row.get("finished", 0) for row in runs_rows)
        errors = sum(row.get("errors", 0) for row in runs_rows)
        return MonitorSnapshot(
            generated_at=now,
            window_minutes=window_minutes,
            status_counts={status: self._status_counts.get(status, 0) for status in _RUN_STATUSES},
            queue=self._probe,
            token_totals=_token_totals(tokens_rows),
            error_summary=ErrorSummary(
                errors=errors,
                runs_finished=runs_finished,
                rate=errors / max(runs_finished, 1),
                recent=_window_errors(self._recent_errors, now - window_minutes * 60),
            ),
            node_top=_ranked_node_stats(nodes_rows, None, 10),
        )

    async def minute_series(self, *, minutes: int = 60) -> list[MinutePoint]:
        buckets = _bucket_range(time.time(), minutes)
        return [
            _minute_point(
                ts,
                self._bucket_runs.get(ts, {}),
                self._bucket_tokens.get(ts, {}),
                self._bucket_queue.get(ts, {}),
            )
            for ts in buckets
        ]

    async def node_stats(
        self, *, graph_id: str | None = None, minutes: int = 60, limit: int = 50
    ) -> list[NodeStat]:
        buckets = _bucket_range(time.time(), minutes)
        return _ranked_node_stats(
            (self._bucket_nodes.get(ts, {}) for ts in buckets), graph_id, limit
        )

    async def recent_spans(self, *, graph_id: str | None, limit: int = 200) -> list[NodeSpanEvent]:
        source = (
            [self._spans.get(graph_id, [])] if graph_id is not None else list(self._spans.values())
        )
        merged = [span for spans in source for span in spans]
        merged.sort(key=lambda span: span.ts, reverse=True)
        return merged[:limit]

    async def run_summary(self, run_id: str) -> dict[str, Any] | None:
        record = self._runs.get(run_id)
        if record is None:
            return None
        spans = sorted(self._run_spans.get(run_id, []), key=lambda span: span.ts)
        return {"summary": dict(record), "spans": spans}

    async def recent_events(
        self, *, limit: int = 200, after_seq: int | None = None
    ) -> list[SequencedEvent]:
        if after_seq is None:
            return self._log[-limit:] if limit > 0 else []
        return [item for item in self._log if item.seq > after_seq][:limit]

    def subscribe(self) -> AsyncGenerator[SequencedEvent, None]:
        return self._subscribe_events()

    async def aclose(self) -> None:
        for queue in list(self._subscribers):
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(None)
        self._subscribers.clear()

    # ---- 内部实现 ----

    def _ensure_run(self, run_id: str, thread_id: str, graph_id: str) -> dict[str, Any]:
        """取回 run 汇总记录；缺失时以零值散记录兜底（与 Redis 版 HINCRBY 自动建键一致）。"""
        record = self._runs.get(run_id)
        if record is None:
            record = {
                "run_id": run_id,
                "thread_id": thread_id,
                "graph_id": graph_id,
                "node_count": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
            }
            self._runs[run_id] = record
        return record

    def _record_run_lifecycle(self, event: RunLifecycleEvent, bucket_ts: int) -> None:
        record = self._ensure_run(event.run_id, event.thread_id, event.graph_id)
        first_seen = "status" not in record
        runs_bucket = self._bucket_runs.setdefault(bucket_ts, {})
        if first_seen:
            record["started_ts"] = event.ts
            runs_bucket["started"] = runs_bucket.get("started", 0) + 1
            self._status_counts["pending"] += 1
        effective_old = str(record.get("status", "pending"))
        if effective_old != event.status:
            self._status_counts[effective_old] = self._status_counts.get(effective_old, 0) - 1
            self._status_counts[event.status] = self._status_counts.get(event.status, 0) + 1
            if event.status in _TERMINAL_STATUSES:
                runs_bucket["finished"] = runs_bucket.get("finished", 0) + 1
                record["finished_ts"] = event.ts
            if event.status == "error":
                runs_bucket["errors"] = runs_bucket.get("errors", 0) + 1
                self._insert_error(
                    {
                        "ts": event.ts,
                        "run_id": event.run_id,
                        "graph_id": event.graph_id,
                        "node": None,
                        "message": _truncate(event.error),
                    }
                )
        record["status"] = event.status
        record["updated_ts"] = event.ts
        if event.error:
            record["error"] = event.error
        if event.duration_ms is not None:
            record["duration_ms"] = event.duration_ms

    def _record_node_span(self, event: NodeSpanEvent, bucket_ts: int) -> None:
        fields = self._bucket_nodes.setdefault(bucket_ts, {})
        prefix = f"{event.graph_id}|{event.node}|"
        fields[prefix + "count"] = fields.get(prefix + "count", 0.0) + 1
        fields[prefix + "total_ms"] = fields.get(prefix + "total_ms", 0.0) + event.duration_ms
        if event.outcome == "error":
            fields[prefix + "errors"] = fields.get(prefix + "errors", 0.0) + 1
            self._insert_error(
                {
                    "ts": event.ts,
                    "run_id": event.run_id,
                    "graph_id": event.graph_id,
                    "node": event.node,
                    "message": _truncate(event.error),
                }
            )
        old_max = fields.get(prefix + "max_ms")
        fields[prefix + "max_ms"] = (
            event.duration_ms if old_max is None else max(old_max, event.duration_ms)
        )
        self._spans.setdefault(event.graph_id, []).append(event)
        self._run_spans.setdefault(event.run_id, []).append(event)
        record = self._ensure_run(event.run_id, event.thread_id, event.graph_id)
        record["node_count"] = _to_int(record.get("node_count", 0)) + 1

    def _record_token_usage(self, event: TokenUsageEvent, bucket_ts: int) -> None:
        tokens_bucket = self._bucket_tokens.setdefault(bucket_ts, {})
        tokens_bucket["input_tokens"] = tokens_bucket.get("input_tokens", 0) + event.input_tokens
        tokens_bucket["output_tokens"] = tokens_bucket.get("output_tokens", 0) + event.output_tokens
        tokens_bucket["total_tokens"] = tokens_bucket.get("total_tokens", 0) + event.total_tokens
        record = self._ensure_run(event.run_id, event.thread_id, event.graph_id)
        record["input_tokens"] = _to_int(record.get("input_tokens", 0)) + event.input_tokens
        record["output_tokens"] = _to_int(record.get("output_tokens", 0)) + event.output_tokens
        record["total_tokens"] = _to_int(record.get("total_tokens", 0)) + event.total_tokens

    def _insert_error(self, entry: dict[str, Any]) -> None:
        self._recent_errors.insert(0, entry)
        del self._recent_errors[_RECENT_ERRORS_CAP:]

    def _gc(self, now_bucket: int) -> None:
        """清理超过留存期的桶与 run 记录，防长时运行下的内存无界增长。"""
        cutoff = now_bucket - _MEMORY_RETENTION_SECONDS
        for table in (
            self._bucket_runs,
            self._bucket_tokens,
            self._bucket_queue,
            self._bucket_nodes,
        ):
            for ts in [ts for ts in table if ts < cutoff]:
                del table[ts]
        for run_id in [
            run_id
            for run_id, record in self._runs.items()
            if _to_float(record.get("updated_ts", 0.0)) < cutoff
        ]:
            del self._runs[run_id]
            self._run_spans.pop(run_id, None)

    async def _subscribe_events(self) -> AsyncGenerator[SequencedEvent, None]:
        queue: asyncio.Queue[SequencedEvent | None] = asyncio.Queue(maxsize=_SUBSCRIBER_QUEUE_SIZE)
        self._subscribers.add(queue)
        try:
            while True:
                item = await queue.get()
                if item is None:
                    return
                yield item
        finally:
            self._subscribers.discard(queue)
