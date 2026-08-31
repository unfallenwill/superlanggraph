"""driver 运行器：官方 Pregel 循环的宿主 + 控制流分发器。

- ``RunExecutor`` 实现 ``NodeExecutor`` 协议：官方循环每决定执行一个节点，
  任务经 Redis 发给 worker，结果回来后中断/错误在 driver 进程重抛，由官方
  流程走重试与检查点。run 的全部事件写入事件流，终态写回 run 记录。
- ``ControlDispatcher`` 消费控制流：start 按 per-thread 策略调度
  （interrupt / rollback / enqueue），cancel 取消在途 run。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, cast

from langchain_core.runnables import RunnableConfig
from langgraph._internal._constants import (
    CONFIG_KEY_RESUME_MAP,
    CONFIG_KEY_SCRATCHPAD,
    CONFIG_KEY_SEND,
)
from langgraph.errors import GraphInterrupt
from langgraph.types import Command
from opentelemetry import propagate

from superlanggraph.broker.runs import RunStore
from superlanggraph.checkpoint.redis_saver import RedisSaver
from superlanggraph.core.envelope import Envelope
from superlanggraph.core.protocols import ControlBus, EventBus, ResultBus, TaskQueue
from superlanggraph.core.serde import dumps_json_text
from superlanggraph.core.task_codec import NodeOutcome, NodeTask, decode_outcome, encode_task
from superlanggraph.observability import inject_into_envelope, tracer
from superlanggraph.registry.loader import Registry

logger = logging.getLogger(__name__)

_STREAM_MODES = ("values", "updates", "tasks", "checkpoints", "debug")
"""driver 直接转发的官方流模式；messages / custom 由 worker 实时发事件流。"""


def _build_graph_input(spec: RunSpec) -> Any:
    """把控制消息里的 input / command 还原为官方 astream 可接受的输入。"""
    if spec.input is not None:
        return spec.input
    if spec.command:
        return Command(
            resume=spec.command.get("resume"),
            update=spec.command.get("update"),
            goto=spec.command.get("goto", ()),
        )
    return None


def _as_node_tuple(value: Any) -> tuple[str, ...] | None:
    """interrupt_before/after 接受节点名列表或 "*"。"""
    if value is None:
        return None
    if value == "*":
        return ("*",)
    if isinstance(value, str):
        return (value,)
    items = cast("list[Any]", value)
    return tuple(str(item) for item in items)


@dataclass(frozen=True, slots=True)
class RunSpec:
    """一次 run 的完整驱动参数。"""

    run_id: str
    thread_id: str
    graph_id: str
    input: Any
    command: dict[str, Any] | None = None
    extra_configurable: dict[str, Any] = field(default_factory=dict[str, Any])
    interrupt_before: tuple[str, ...] | None = None
    interrupt_after: tuple[str, ...] | None = None
    durability: str | None = None
    trace_context: dict[str, str] | None = None


class RunExecutor:
    """驱动一个 run：官方 astream + 远程节点执行 + 事件发射。"""

    def __init__(
        self,
        registry: Registry,
        saver: RedisSaver,
        tasks: TaskQueue,
        results: ResultBus,
        events: EventBus,
        store: RunStore,
    ) -> None:
        self._registry = registry
        self._saver = saver
        self._tasks = tasks
        self._results = results
        self._events = events
        self._store = store
        self._pending: dict[tuple[str, str], asyncio.Future[NodeOutcome]] = {}
        self._tracer = tracer("superlanggraph.driver")

    async def execute_node(
        self,
        *,
        graph_id: str,
        node: str,
        thread_id: str,
        run_id: str,
        checkpoint_ns: str,
        input: Any,
        config: RunnableConfig | None,
    ) -> Any:
        """NodeExecutor 协议：派发节点任务并等待 worker 结果。"""
        task_id = str(uuid.uuid4())
        conf = (config or {}).get("configurable") or {}
        # 官方任务上下文中 scratchpad / send 位于 configurable 顶层。
        scratchpad = conf.get(CONFIG_KEY_SCRATCHPAD)
        send = conf.get(CONFIG_KEY_SEND)
        resume_map = cast("dict[str, Any]", conf.get(CONFIG_KEY_RESUME_MAP) or {})
        fut: asyncio.Future[NodeOutcome] = asyncio.get_running_loop().create_future()
        self._pending[(run_id, task_id)] = fut
        envelope = Envelope(
            type="task",
            run_id=run_id,
            task_id=task_id,
            payload=encode_task(
                NodeTask(
                    task_id=task_id,
                    graph_id=graph_id,
                    node=node,
                    run_id=run_id,
                    thread_id=thread_id,
                    checkpoint_ns=checkpoint_ns,
                    input=input,
                    resume=list(scratchpad.resume) if scratchpad else [],
                    null_resume=(scratchpad.get_null_resume(False) if scratchpad else None),
                    resume_map=resume_map,
                    metadata=(config or {}).get("metadata") or {},
                    tags=(config or {}).get("tags") or [],
                )
            ),
        )
        envelope = inject_into_envelope(envelope)
        await self._tasks.enqueue_task(envelope)
        outcome = await fut
        self._pending.pop((run_id, task_id), None)
        return await self._apply_outcome(outcome, scratchpad, send)

    async def run(self, spec: RunSpec) -> None:
        """完整驱动一次 run（作为独立 asyncio.Task 运行）。"""
        drain = asyncio.create_task(self._drain_results(spec.run_id))
        heartbeat = asyncio.create_task(self._heartbeat_loop(spec.run_id))
        interrupted = False
        carrier = dict(spec.trace_context or {})
        with self._tracer.start_as_current_span(
            "run.execute",
            context=propagate.extract(carrier),
            attributes={
                "run.id": spec.run_id,
                "thread.id": spec.thread_id,
                "graph.id": spec.graph_id,
            },
        ):
            try:
                await self._store.update(spec.run_id, status="running")
                graph = self._registry.compile_distributed(
                    spec.graph_id, self, checkpointer=self._saver
                )
                config: RunnableConfig = {
                    "configurable": {
                        "thread_id": spec.thread_id,
                        "checkpoint_ns": "",
                        "run_id": spec.run_id,
                        **spec.extra_configurable,
                    }
                }
                graph_input = _build_graph_input(spec)
                async for chunk in graph.astream(
                    graph_input,
                    config=config,
                    interrupt_before=spec.interrupt_before,
                    interrupt_after=spec.interrupt_after,
                    durability=cast("Any", spec.durability) if spec.durability else None,
                    stream_mode=list(_STREAM_MODES),
                    subgraphs=True,
                ):
                    ns, mode, data = cast("tuple[tuple[str, ...], str, Any]", chunk)
                    # 官方把挂起信号以 __interrupt__ 键放进 dict 输出（values/updates
                    # 皆可），键存在即视为中断挂起，与具体模式无关。
                    data_dict = cast("dict[str, Any]", data) if isinstance(data, dict) else None
                    if data_dict is not None and "__interrupt__" in data_dict:
                        interrupted = True
                    await self._emit(spec.run_id, mode, ns, data)
                await self._finish(
                    spec.run_id,
                    spec.thread_id,
                    "interrupted" if interrupted else "success",
                )
            except asyncio.CancelledError:
                await self._finish(spec.run_id, spec.thread_id, "interrupted")
            except Exception as exc:
                await self._emit_text(
                    spec.run_id, "error", {"error": repr(exc), "message": str(exc)}
                )
                await self._finish(spec.run_id, spec.thread_id, "error", error=repr(exc))
            finally:
                for key in [k for k in self._pending if k[0] == spec.run_id]:
                    self._pending.pop(key, None)
        heartbeat.cancel()
        drain.cancel()

    async def _heartbeat_loop(self, run_id: str) -> None:
        """driver 心跳：守尸据此区分「在跑」与「孤儿」。"""
        while True:
            await self._store.heartbeat(run_id)
            await asyncio.sleep(5)

    async def _apply_outcome(self, outcome: NodeOutcome, scratchpad: Any, send: Any) -> Any:
        """把 worker 结果还原为官方循环期待的返回值 / 异常。"""
        if outcome.resumes:
            if send is not None:
                send(outcome.resumes)
            if scratchpad is not None:
                # 官方 scratchpad 为 frozen dataclass：resume 列表只允许
                # 原地扩展（与官方 interrupt() 的 append 行为一致）。
                latest = outcome.resumes[-1][1]
                if isinstance(latest, list):
                    scratchpad.resume.extend(latest)
                elif latest is not None:
                    scratchpad.resume.append(latest)
        if outcome.kind == "return":
            return outcome.value
        if outcome.kind == "interrupt":
            interrupts = tuple(outcome.value or ())
            raise GraphInterrupt(interrupts)
        raise RuntimeError(outcome.error_text or "远程节点执行失败")

    async def _drain_results(self, run_id: str) -> None:
        """消费该 run 的结果流，唤醒等待中的节点任务。"""
        async for _entry_id, envelope in self._results.read_results(run_id):
            outcome = decode_outcome(envelope.payload)
            fut = self._pending.get((run_id, str(envelope.task_id)))
            if fut is not None and not fut.done():
                fut.set_result(outcome)

    async def _emit(self, run_id: str, mode: str, ns: tuple[str, ...], data: Any) -> None:
        payload: dict[str, Any] = {"event": mode, "data": dumps_json_text(data)}
        if ns:
            payload["ns"] = [str(part) for part in ns]
        await self._events.append_event(
            run_id, Envelope(type="event", run_id=run_id, payload=payload)
        )

    async def _emit_text(self, run_id: str, event: str, data: dict[str, Any]) -> None:
        await self._events.append_event(
            run_id,
            Envelope(
                type="event",
                run_id=run_id,
                payload={"event": event, "data": json.dumps(data, ensure_ascii=False)},
            ),
        )

    async def _finish(
        self, run_id: str, thread_id: str, status: str, error: str | None = None
    ) -> None:
        fields: dict[str, Any] = {"status": status}
        if error:
            fields["error"] = error
        await self._store.update(run_id, **fields)
        await self._store.clear_active(thread_id, run_id)
        await self._emit_text(run_id, "end", {"status": status})


class ControlDispatcher:
    """消费控制流：start / cancel。per-thread 串行化在这里实现。

    同时承担守尸职责：周期扫描活动 run，心跳过期的孤儿由本实例认领，
    从 checkpoint 重新驱动（认领锁防止多实例重复恢复）。
    """

    REAP_INTERVAL_SECONDS = 10.0

    def __init__(
        self,
        executor: RunExecutor,
        store: RunStore,
        saver: RedisSaver,
        control: ControlBus,
    ) -> None:
        self._executor = executor
        self._store = store
        self._saver = saver
        self._control = control
        self._thread_tasks: dict[str, asyncio.Task[None]] = {}
        self._local_run_ids: set[str] = set()
        self.draining = False

    async def serve_forever(self) -> None:
        reaper = asyncio.create_task(self._reap_loop())
        try:
            async for envelope in self._control.consume_control():
                kind = envelope.payload.get("kind")
                if kind == "start":
                    if self.draining:
                        continue
                    await self._start(envelope)
                elif kind == "cancel":
                    await self._cancel(envelope)
        finally:
            reaper.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reaper

    async def _reap_loop(self) -> None:
        while True:
            await asyncio.sleep(self.REAP_INTERVAL_SECONDS)
            try:
                await self._reap_orphans()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

    async def aclose(self) -> None:
        """停止本实例：取消全部在途 run 协程（模拟 driver 死亡的对照操作）。"""
        for task in list(self._thread_tasks.values()):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._thread_tasks.clear()
        self._local_run_ids.clear()

    async def _reap_orphans(self) -> None:
        """认领心跳过期的孤儿 run：从最新 checkpoint 重新驱动。"""
        if self.draining:
            return
        async for thread_id, run_id in self._store.list_active_runs():
            if run_id in self._local_run_ids:
                continue
            record = await self._store.get(run_id)
            if record is None or record["status"] not in ("running", "pending"):
                continue
            if await self._store.heartbeat_alive(run_id):
                continue
            if not await self._store.try_claim_recovery(run_id):
                continue
            logger.warning("认领孤儿 run %s（线程 %s），从 checkpoint 恢复", run_id, thread_id)
            await self._store.update(run_id, status="pending")
            await self._start(
                Envelope(
                    type="control",
                    run_id=run_id,
                    payload={
                        "kind": "start",
                        "run_id": run_id,
                        "thread_id": thread_id,
                        "graph_id": record["graph_id"],
                        "input": record.get("input"),
                        "command": record.get("command"),
                        "extra_configurable": {},
                    },
                )
            )

    async def _start(self, envelope: Envelope) -> None:
        payload = envelope.payload
        thread_id = str(payload["thread_id"])
        spec = RunSpec(
            run_id=str(payload["run_id"]),
            thread_id=thread_id,
            graph_id=str(payload["graph_id"]),
            input=payload.get("input"),
            command=payload.get("command"),
            extra_configurable=payload.get("extra_configurable") or {},
            interrupt_before=_as_node_tuple(payload.get("interrupt_before")),
            interrupt_after=_as_node_tuple(payload.get("interrupt_after")),
            durability=payload.get("durability"),
            trace_context=dict(envelope.trace or {}),
        )
        run_id = spec.run_id
        previous = self._thread_tasks.get(thread_id)
        if previous is not None and not previous.done():
            previous.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await previous
        self._thread_tasks.pop(thread_id, None)
        self._local_run_ids.add(run_id)
        await self._store.set_active(thread_id, run_id)
        run_task = asyncio.create_task(self._executor.run(spec))

        async def _cleanup() -> None:
            try:
                await run_task
            except asyncio.CancelledError:
                return
            except Exception:
                return
            finally:
                self._local_run_ids.discard(run_id)

        self._thread_tasks[thread_id] = asyncio.create_task(_cleanup())

    async def _await_and_drop(self, task: asyncio.Task[None]) -> None:
        try:
            await task
        except asyncio.CancelledError:
            return
        except Exception:
            return

    def _cancel_local(self, thread_id: str) -> None:
        task = self._thread_tasks.get(thread_id)
        if task is not None and not task.done():
            task.cancel()

    async def _cancel(self, envelope: Envelope) -> None:
        payload = envelope.payload
        run_id = str(payload["run_id"])
        thread_id = str(payload["thread_id"])
        action = str(payload.get("action", "interrupt"))
        # 幂等：只有运行中的 run 可以被取消，已到终态的忽略。
        record = await self._store.get(run_id)
        if record is None or record["status"] != "running":
            return
        self._cancel_local(thread_id)
        if action == "rollback":
            await self._saver.adelete_thread(thread_id)
            await self._store.delete(run_id)
        else:
            await self._store.update(run_id, status="interrupted")
