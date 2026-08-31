"""server 运行时聚合：组合根与路由之间的装配层。

单进程模式下 driver / worker / cron 循环也运行于此（后台任务）；多进程
部署时由 cli 按角色裁剪装配内容，路由代码不变。
"""

from __future__ import annotations

import asyncio
import contextlib
import faulthandler
import signal
import sys
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from redis.asyncio import Redis

from superlanggraph.broker.runs import RunStore, now_iso
from superlanggraph.checkpoint.redis_saver import RedisSaver
from superlanggraph.core.envelope import Envelope
from superlanggraph.core.errors import SlgError
from superlanggraph.core.protocols import ControlBus, EventBus, ResultBus, TaskQueue
from superlanggraph.cron.scheduler import CronTable, next_after, utc_now
from superlanggraph.driver.runner import ControlDispatcher, RunExecutor
from superlanggraph.observability import setup_otel, shutdown_otel
from superlanggraph.registry.loader import Registry
from superlanggraph.store.redis_store import RedisStore
from superlanggraph.worker.executor import NodeWorker


class RunRejectedError(SlgError):
    """multitask 策略为 reject 且线程忙时抛出（映射 409）。"""


@dataclass(slots=True)
class ServerRuntime:
    """路由层依赖的运行时聚合；由组合根构造。"""

    redis: Redis
    registry: Registry
    saver: RedisSaver
    runs: RunStore
    crons: CronTable
    kv: RedisStore
    tasks: TaskQueue
    results: ResultBus
    events: EventBus
    control: ControlBus
    executor: RunExecutor
    dispatcher: ControlDispatcher
    worker: NodeWorker
    _background: list[asyncio.Task[None]] = field(default_factory=list[asyncio.Task[None]])

    async def start_background(self, roles: set[str] | None = None) -> None:
        """按角色启动进程内后台循环；roles 为空表示单机全角色模式。

        角色映射：driver→控制分发、worker→节点执行、cron→定时调度；
        server 角色本身没有后台循环（纯 HTTP 接入）。
        """
        enabled = roles if roles else {"server", "driver", "worker", "cron"}
        if "driver" in enabled:
            self._background.append(
                asyncio.create_task(self.dispatcher.serve_forever(), name="slg-dispatcher")
            )
        if "worker" in enabled:
            self._background.append(
                asyncio.create_task(self.worker.serve_forever(), name="slg-worker")
            )
        if "cron" in enabled:
            self._background.append(asyncio.create_task(self._cron_loop(), name="slg-cron"))

    async def serve_roles(self, roles: set[str]) -> None:
        """无 HTTP 的纯后台角色入口（driver / worker / cron 独立进程部署）。

        阻塞直至收到停止信号；退出前执行优雅停机（drain + 心跳收尾）。
        每 120 秒自动向 stderr 转储一次全部线程堆栈（挂起现场诊断）。
        """
        faulthandler.dump_traceback_later(120, repeat=True, file=sys.stderr)

        setup_otel(role="-".join(sorted(roles)))
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)

        await self.start_background(roles)
        await stop.wait()
        await self.stop_background()
        shutdown_otel()

    async def stop_background(self) -> None:
        """优雅停机：先停止接收新 run，等在途 run 到终态（有上限），
        再取消后台角色。剩余未完成 run 保持 pending/running 记录，
        由下一个实例的守尸接管恢复。"""
        self.dispatcher.draining = True
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 30.0
        while loop.time() < deadline:
            if not await self._has_active_runs():
                break
            await asyncio.sleep(0.2)
        for task in self._background:
            task.cancel()
        for task in self._background:
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _has_active_runs(self) -> bool:
        async for _thread_id, _run_id in self.runs.list_active_runs():
            return True
        return False

    # ---- cron 调度 ----

    async def _cron_loop(self) -> None:
        while True:
            try:
                await self._fire_due_crons()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            await asyncio.sleep(1.0)

    async def _fire_due_crons(self) -> None:
        # 多实例部署时的 cron 触发锁：同一轮只有持锁实例触发。
        lock_key = "slg:cron-lock"
        if not await self.redis.set(lock_key, "1", nx=True, ex=5):
            return
        now = utc_now()
        for record in await self.crons.all():
            if record.next_run_at is None:
                continue
            due = datetime.fromisoformat(record.next_run_at)
            if due.tzinfo is None:
                due = due.replace(tzinfo=UTC)
            if due > now:
                continue
            await self.crons.update(
                record.cron_id,
                next_run_at=next_after(record.schedule, now).isoformat(),
            )
            thread_id = record.thread_id or f"cron-{uuid.uuid4().hex}"
            await self.ensure_thread(thread_id)
            await self.create_run(
                thread_id=thread_id,
                assistant_id=record.assistant_id,
                input=record.input,
                metadata=record.metadata,
                multitask_strategy="enqueue",
            )

    # ---- run 创建的公共路径（runs 路由与 cron 共用） ----

    async def create_run(
        self,
        *,
        thread_id: str,
        assistant_id: str,
        input: Any = None,
        command: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        multitask_strategy: str = "reject",
        extra_configurable: dict[str, Any] | None = None,
        interrupt_before: tuple[str, ...] | None = None,
        interrupt_after: tuple[str, ...] | None = None,
        durability: str | None = None,
    ) -> dict[str, Any]:
        active = await self.runs.get_active(thread_id)
        if active is not None and multitask_strategy == "reject":
            raise RunRejectedError(f"线程 {thread_id} 有正在执行的 run: {active}")
        if active is not None and multitask_strategy in ("interrupt", "rollback"):
            await self.control.publish_control(
                Envelope(
                    type="control",
                    run_id=active,
                    payload={
                        "kind": "cancel",
                        "run_id": active,
                        "thread_id": thread_id,
                        "action": multitask_strategy,
                    },
                )
            )
            await self._wait_active_clear(thread_id)
        assistant = await self.resolve_assistant(assistant_id)
        record = await self.runs.create(
            {
                "thread_id": thread_id,
                "assistant_id": assistant["assistant_id"],
                "graph_id": assistant["graph_id"],
                "metadata": metadata,
                "multitask_strategy": multitask_strategy,
                "input": input,
                "command": command,
            }
        )
        await self.ensure_thread(thread_id)
        await self.redis.set(f"slg:thread-graph:{thread_id}", assistant["graph_id"])
        await self.control.publish_control(
            Envelope(
                type="control",
                run_id=record["run_id"],
                payload={
                    "kind": "start",
                    "run_id": record["run_id"],
                    "thread_id": thread_id,
                    "graph_id": assistant["graph_id"],
                    "input": input,
                    "command": command,
                    "extra_configurable": extra_configurable or {},
                    "interrupt_before": list(interrupt_before) if interrupt_before else None,
                    "interrupt_after": list(interrupt_after) if interrupt_after else None,
                    "durability": durability,
                },
            )
        )
        return record

    async def _wait_active_clear(self, thread_id: str, timeout: float = 5.0) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            if await self.runs.get_active(thread_id) is None:
                return
            await asyncio.sleep(0.05)

    # ---- assistants ----

    async def resolve_assistant(self, assistant_id: str) -> dict[str, Any]:
        record = await self.get_assistant(assistant_id)
        if record is None:
            record = await self.get_assistant_by_name(assistant_id)
        if record is None:
            raise RunRejectedError(f"assistant 不存在: {assistant_id}")
        return record

    async def get_assistant(self, assistant_id: str) -> dict[str, Any] | None:
        raw = await self.redis.hgetall(f"slg:assistant:{assistant_id}")
        return _decode_hash(raw)

    async def get_assistant_by_name(self, name: str) -> dict[str, Any] | None:
        assistant_id = await self.redis.get(f"slg:assistant-name:{name}")
        if assistant_id is None:
            return None
        return await self.get_assistant(_decode(assistant_id))

    async def put_assistant(self, record: dict[str, Any]) -> None:
        pipeline = self.redis.pipeline()
        pipeline.hset(f"slg:assistant:{record['assistant_id']}", mapping=record)  # type: ignore[arg-type]
        pipeline.set(f"slg:assistant-name:{record['name']}", record["assistant_id"])
        await pipeline.execute()

    async def seed_assistants(self) -> None:
        """为注册表中的每个图确保存在默认 assistant（name = graph_id）。"""
        for graph_id in self.registry.graph_ids():
            if await self.get_assistant_by_name(graph_id) is not None:
                continue
            await self.put_assistant(
                {
                    "assistant_id": graph_id,
                    "name": graph_id,
                    "graph_id": graph_id,
                    "version": "1",
                    "config": "{}",
                    "metadata": "{}",
                    "created_at": now_iso(),
                    "updated_at": now_iso(),
                }
            )

    # ---- threads ----

    async def ensure_thread(self, thread_id: str) -> None:
        exists = await self.redis.exists(f"slg:thread:{thread_id}")
        if not exists:
            await self.redis.hset(
                f"slg:thread:{thread_id}",
                mapping={  # type: ignore[arg-type]
                    "thread_id": thread_id,
                    "status": "idle",
                    "metadata": "{}",
                    "created_at": now_iso(),
                    "updated_at": now_iso(),
                },
            )


def _decode(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _decode_hash(raw: dict[Any, Any]) -> dict[str, Any] | None:
    if not raw:
        return None
    return {_decode(k): _decode(v) for k, v in raw.items()}
