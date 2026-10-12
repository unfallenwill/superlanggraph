"""节点执行器：worker 角色的核心。

worker 拿到任务后，从注册表取原始节点 Runnable，在构造好的官方兼容上下文里
执行它：scratchpad shim 让官方 ``interrupt()`` 原样工作，Runtime shim 让
``get_stream_writer()`` 把 custom 事件实时发回事件流，RESUME 写入收集后随
结果回传 driver。
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import time
import uuid
from typing import Any, cast

from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import var_child_runnable_config
from langgraph._internal._constants import (
    CONFIG_KEY_CHECKPOINT_NS,
    CONFIG_KEY_RUNTIME,
    CONFIG_KEY_SCRATCHPAD,
    CONFIG_KEY_SEND,
    CONFIG_KEY_STREAM,
)
from langgraph._internal._scratchpad import PregelScratchpad
from langgraph.errors import GraphInterrupt
from langgraph.pregel._messages import StreamMessagesHandler
from langgraph.pregel.protocol import StreamChunk
from langgraph.runtime import Runtime
from xxhash import xxh3_128_hexdigest

from superlanggraph.core.envelope import Envelope
from superlanggraph.core.protocols import EventBus, ResultBus, TaskQueue
from superlanggraph.core.serde import dumps_json_text
from superlanggraph.core.task_codec import NodeOutcome, NodeTask, decode_task, encode_outcome
from superlanggraph.monitoring import MonitoringCollector
from superlanggraph.observability import extract_from_envelope, tracer
from superlanggraph.observability import meter as get_meter
from superlanggraph.registry.loader import Registry


class _LazyCounter:
    """与官方 LazyAtomicCounter 相同的「先返回后自增」语义。"""

    def __init__(self) -> None:
        self._next = itertools.count(0).__next__

    def __call__(self) -> int:
        return self._next()


class _ResumeCollector:
    """收集官方 interrupt() 发出的 RESUME 写入，随结果回传 driver。"""

    def __init__(self) -> None:
        self.items: list[tuple[str, Any]] = []

    def extend(self, writes: list[tuple[str, Any]]) -> None:
        self.items.extend(writes)


def _build_scratchpad(resume: list[Any], null_resume: Any) -> PregelScratchpad:
    """构造与官方任务 scratchpad 同形状的 shim。

    ``null_resume`` 是 checkpoint 中 NULL 任务 RESUME 写入的值（Command(resume)
    的传递通道），官方语义：消费一次后置空。
    """
    holder = {"null_resume": null_resume}

    def get_null_resume(consume: bool = False) -> Any:
        value = holder["null_resume"]
        if consume:
            holder["null_resume"] = None
        return value

    return PregelScratchpad(
        step=0,
        stop=2**31 - 1,
        call_counter=_LazyCounter(),
        interrupt_counter=_LazyCounter(),
        get_null_resume=get_null_resume,
        resume=resume,
        subgraph_counter=_LazyCounter(),
    )


class NodeWorker:
    """消费任务队列、执行节点、发布结果与事件的执行器。"""

    def __init__(
        self,
        registry: Registry,
        tasks: TaskQueue,
        results: ResultBus,
        events: EventBus,
        concurrency: int = 32,
        collector: MonitoringCollector | None = None,
        worker_id: str | None = None,
        heartbeat_interval_seconds: float = 5.0,
    ) -> None:
        self._registry = registry
        self._tasks = tasks
        self._results = results
        self._events = events
        # 单 worker 进程内的任务并发度：dequeue 前先占槽，避免无界并发。
        self._slots = asyncio.Semaphore(concurrency)
        self._concurrency = concurrency
        self._inflight: set[asyncio.Task[None]] = set()
        # 官方 stream_writer 是同步回调：事件先进内存队列，由排水任务写 Redis。
        self._outbox: asyncio.Queue[Envelope] = asyncio.Queue()
        self._monitor = collector
        self._worker_id = worker_id or uuid.uuid4().hex
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._tracer = tracer("superlanggraph.worker")
        self._task_duration = get_meter("superlanggraph.worker").create_histogram(
            "slg.task.duration", unit="s", description="节点任务执行时长"
        )

    async def serve_forever(self, lease_seconds: float = 30.0) -> None:
        """主循环：领取任务 → 执行 → 发布结果 → 确认；同时排水事件队列、
        周期回收 worker 死亡遗留的停滞任务（XAUTOCLAIM）。"""
        drain = asyncio.create_task(self._drain_outbox())
        reclaimer = asyncio.create_task(self._reclaim_loop(lease_seconds))
        heartbeat = asyncio.create_task(self._heartbeat_loop())
        try:
            await self._serve_loop(lease_seconds)
        finally:
            for background in (reclaimer, drain, heartbeat):
                background.cancel()
            for background in (reclaimer, drain, heartbeat):
                with contextlib.suppress(asyncio.CancelledError):
                    await background
            # 优雅停机：等在飞任务收尾，未 ack 的由 XAUTOCLAIM 兜底。
            if self._inflight:
                await asyncio.gather(*self._inflight, return_exceptions=True)

    async def _heartbeat_loop(self) -> None:
        """监控心跳：周期上报存活与负载（worker 面板数据源；未接线时零开销）。"""
        while True:
            await asyncio.sleep(self._heartbeat_interval_seconds)
            if self._monitor is not None:
                self._monitor.on_worker_heartbeat(
                    worker_id=self._worker_id,
                    inflight=len(self._inflight),
                    concurrency=self._concurrency,
                )

    async def _drain_outbox(self) -> None:
        while True:
            envelope = await self._outbox.get()
            with contextlib.suppress(Exception):
                await self._events.append_event(envelope.run_id, envelope)

    async def _reclaim_loop(self, lease_seconds: float) -> None:
        """周期认领停滞任务；driver 按 task_id 去重，重复结果安全丢弃。"""
        while True:
            await asyncio.sleep(lease_seconds)
            try:
                stale = await self._tasks.reclaim_stale(int(lease_seconds * 1000))
            except Exception:
                continue
            for receipt, envelope in stale:
                await self._handle_envelope(envelope, ack_receipt=receipt)

    async def _serve_loop(self, lease_seconds: float) -> None:
        while True:
            try:
                # 先占槽再 dequeue：满载时消息留在队列里给其他 worker。
                await self._slots.acquire()
            except asyncio.CancelledError:
                raise
            try:
                lease = await self._tasks.dequeue_task(lease_seconds)
            except Exception:
                self._slots.release()
                await asyncio.sleep(0.5)
                continue
            if lease is None:
                self._slots.release()
                continue
            receipt, envelope = lease
            worker_task = asyncio.create_task(self._handle_envelope(envelope, ack_receipt=receipt))
            self._inflight.add(worker_task)
            worker_task.add_done_callback(self._on_inflight_done)

    def _on_inflight_done(self, task: asyncio.Task[None]) -> None:
        self._inflight.discard(task)
        self._slots.release()
        # 检索并吞掉异常，避免未取回警告；单任务失败由未 ack 重投兜底。
        with contextlib.suppress(Exception):
            task.result()

    async def _handle_envelope(self, envelope: Envelope, *, ack_receipt: str) -> None:
        context = extract_from_envelope(envelope)
        try:
            task = decode_task(envelope.payload)
        except Exception as exc:
            await self._events.append_event(
                envelope.run_id,
                Envelope(
                    type="event",
                    run_id=envelope.run_id,
                    payload={
                        "event": "error",
                        "data": f"任务载荷解码失败: {exc!r}",
                    },
                ),
            )
            await self._tasks.ack_task(ack_receipt)
            return
        started = time.monotonic()
        if self._monitor is not None:
            self._monitor.on_task_start(
                run_id=task.run_id,
                thread_id=task.thread_id,
                graph_id=task.graph_id,
                task_id=task.task_id,
                node=task.node,
                enqueued_ts=envelope.ts,
            )
        with self._tracer.start_as_current_span(
            "task.execute",
            context=context,
            attributes={
                "run.id": envelope.run_id,
                "task.id": envelope.task_id or "",
                "task.node": task.node,
                "graph.id": task.graph_id,
            },
        ) as span:
            outcome = await self._execute_safely(task)
            span.set_attribute("task.outcome", outcome.kind)
            if outcome.error_text:
                span.set_attribute("task.error", outcome.error_text[:200])
            self._task_duration.record(time.monotonic() - started, {"task.node": task.node})
        if self._monitor is not None:
            self._monitor.on_task_end(
                run_id=task.run_id,
                thread_id=task.thread_id,
                graph_id=task.graph_id,
                task_id=task.task_id,
                node=task.node,
                outcome=outcome.kind,
                duration_ms=(time.monotonic() - started) * 1000,
                error=outcome.error_text or None,
            )
        await self._results.publish_result(
            envelope.run_id,
            Envelope(
                type="result",
                run_id=envelope.run_id,
                task_id=outcome.task_id,
                payload=encode_outcome(outcome),
            ),
        )
        await self._tasks.ack_task(ack_receipt)

    async def _execute_safely(self, task: NodeTask) -> NodeOutcome:
        try:
            return await self.execute(task)
        except Exception as exc:
            return NodeOutcome(
                task_id=task.task_id,
                kind="error",
                error_text=f"worker 内部错误: {exc!r}",
            )

    async def execute(self, task: NodeTask) -> NodeOutcome:
        """执行单个节点任务：构造官方兼容上下文后调用原节点。"""
        graph = self._registry.compile_local(task.graph_id)
        node = graph.nodes.get(task.node)
        if node is None:
            return NodeOutcome(
                task_id=task.task_id,
                kind="error",
                error_text=f"图中不存在节点: {task.node}",
            )
        bound = node.bound

        collector = _ResumeCollector()
        # 官方 resume_map 以 task_checkpoint_ns 的 xxh3 哈希为键。
        ns_hash = xxh3_128_hexdigest(task.checkpoint_ns.encode())
        resume = list(task.resume)
        mapped = task.resume_map.get(ns_hash)
        if mapped is not None:
            resume.append(mapped)
        scratchpad = _build_scratchpad(resume, task.null_resume)
        runtime = Runtime(
            context=None,
            store=None,
            stream_writer=_custom_sink(task, self._outbox),
            previous=None,
            execution_info=None,
        )
        # 官方 interrupt()/get_stream_writer() 从 configurable 顶层取 shim：
        # CONF 常量的值就是 "configurable"，shim 必须平铺，不能嵌套。
        config: dict[str, Any] = {
            "run_name": task.node,
            "metadata": dict(task.metadata),
            "tags": list(task.tags),
            "callbacks": [_messages_relay(task, self._outbox, self._monitor)],
            "configurable": {
                "thread_id": task.thread_id,
                "checkpoint_ns": task.checkpoint_ns,
                CONFIG_KEY_SCRATCHPAD: scratchpad,
                CONFIG_KEY_SEND: collector.extend,
                CONFIG_KEY_CHECKPOINT_NS: task.checkpoint_ns,
                CONFIG_KEY_STREAM: _message_sink(task, self._outbox),
                CONFIG_KEY_RUNTIME: runtime,
            },
        }
        try:
            # 同步节点经 run_in_executor 执行时不自动带 contextvar，
            # 显式注入 config 上下文，保证官方 interrupt() 在线程内可用。
            config_token = var_child_runnable_config.set(cast("RunnableConfig", config))
            try:
                value = await bound.ainvoke(task.input, config)
            finally:
                var_child_runnable_config.reset(config_token)
        except GraphInterrupt as exc:
            return NodeOutcome(
                task_id=task.task_id,
                kind="interrupt",
                value=exc.args[0] if exc.args else (),
                resumes=collector.items,
            )
        except Exception as exc:
            return NodeOutcome(
                task_id=task.task_id,
                kind="error",
                error_text=repr(exc),
                resumes=collector.items,
            )
        return NodeOutcome(
            task_id=task.task_id, kind="return", value=value, resumes=collector.items
        )


def _custom_sink(task: NodeTask, outbox: asyncio.Queue[Envelope]):
    """官方 get_stream_writer() 拿到的函数：custom 事件进 outbox。"""

    def writer(*chunks: Any) -> None:
        for chunk in chunks:
            outbox.put_nowait(
                Envelope(
                    type="event",
                    run_id=task.run_id,
                    task_id=task.task_id,
                    payload={"event": "custom", "data": dumps_json_text(chunk)},
                )
            )

    return writer


def _messages_relay(
    task: NodeTask,
    outbox: asyncio.Queue[Envelope],
    collector: MonitoringCollector | None = None,
) -> StreamMessagesHandler:
    """messages 模式的 token 回传：把官方 handler 的三元组还原成事件信封。

    真实 LLM 调用发生在 worker，driver 侧挂的 StreamMessagesHandler 看不到
    token；callbacks 无法跨 Redis 信封序列化，只能在这里按官方同款契约重建。
    handler 回调收到 (ns, "messages", (message, metadata))，data 只取 payload，
    与 driver 直发形状一致。

    ``collector`` 非空时顺路提取 usage_metadata 上报令牌用量（getattr 零解析
    开销，不触碰 JSON 序列化路径）；多次分片上报由采集器按 task_id 做
    max 合并。
    """

    def relay(item: StreamChunk) -> None:
        _, _, payload = item
        outbox.put_nowait(
            Envelope(
                type="event",
                run_id=task.run_id,
                task_id=task.task_id,
                payload={"event": "messages", "data": dumps_json_text(payload)},
            )
        )
        if collector is not None:
            usage = getattr(payload[0], "usage_metadata", None)
            if isinstance(usage, dict):
                # isinstance 收窄后键值类型仍未知，显式固定 JSON 边界形状。
                fields = cast("dict[str, Any]", usage)
                collector.on_token_usage(
                    task_id=task.task_id,
                    run_id=task.run_id,
                    thread_id=task.thread_id,
                    graph_id=task.graph_id,
                    node=task.node,
                    input_tokens=int(fields.get("input_tokens") or 0),
                    output_tokens=int(fields.get("output_tokens") or 0),
                    total_tokens=int(fields.get("total_tokens") or 0),
                )

    return StreamMessagesHandler(relay, False)


def _message_sink(task: NodeTask, outbox: asyncio.Queue[Envelope]):
    """CONFIG_KEY_STREAM 的 messages 模式汇聚点。"""

    def sink(chunk: Any) -> None:
        outbox.put_nowait(
            Envelope(
                type="event",
                run_id=task.run_id,
                task_id=task.task_id,
                payload={"event": "messages", "data": dumps_json_text(chunk)},
            )
        )

    return sink
