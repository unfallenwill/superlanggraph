# pyright: reportPrivateUsage=false, reportUnknownMemberType=false, reportUnknownVariableType=false
# 守尸测试需短暂触碰 dispatcher 内部（_reap_orphans/_executor），测试为类型边界。

"""守尸恢复集成测试：driver 死亡后，另一个实例认领孤儿 run 并从 checkpoint 恢复。"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from redis.asyncio import Redis

from superlanggraph.broker.redis import RedisBroker
from superlanggraph.broker.runs import RunStore
from superlanggraph.checkpoint.redis_saver import RedisSaver
from superlanggraph.core.envelope import Envelope
from superlanggraph.core.settings import BrokerSettings
from superlanggraph.driver.runner import ControlDispatcher, RunExecutor
from superlanggraph.registry.loader import Registry
from superlanggraph.worker.executor import NodeWorker

SLOW_PY = """
import time
from typing import TypedDict
from langgraph.graph import END, START, StateGraph


class State(TypedDict, total=False):
    count: int


def slow(state):
    time.sleep(1.2)
    return {"count": state.get("count", 0) + 1}


builder = StateGraph(State)
builder.add_node("slow", slow)
builder.add_edge(START, "slow")
builder.add_edge("slow", END)
graph = builder.compile()
"""

HITL_PY = """
from typing import TypedDict
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt


class State(TypedDict, total=False):
    count: int


def ask(state):
    answer = interrupt({"question": "gimme"})
    return {"count": int(answer)}


def bump(state):
    return {"count": state.get("count", 0) + 10}


builder = StateGraph(State)
builder.add_node("ask", ask)
builder.add_node("bump", bump)
builder.add_edge(START, "ask")
builder.add_edge("ask", "bump")
builder.add_edge("bump", END)
graph = builder.compile()
"""


class DriverStack:
    """一个 driver 实例的后台协程与控制入口（测试用组装）。"""

    def __init__(self, settings: BrokerSettings, client: Redis, config_path: Path) -> None:
        self.broker = RedisBroker(settings, client)
        self.runs = RunStore(client, prefix=settings.stream_prefix)
        saver = RedisSaver(client)
        registry = Registry(config_path)
        executor = RunExecutor(registry, saver, self.broker, self.broker, self.broker, self.runs)
        self.dispatcher = ControlDispatcher(executor, self.runs, saver, self.broker)
        worker = NodeWorker(registry, self.broker, self.broker, self.broker)
        self._tasks = [
            asyncio.create_task(self.dispatcher.serve_forever()),
            asyncio.create_task(worker.serve_forever()),
        ]

    async def start_run(self, graph_id: str, thread_id: str, *, input: Any) -> str:
        record = await self.runs.create(
            {
                "thread_id": thread_id,
                "assistant_id": graph_id,
                "graph_id": graph_id,
                "metadata": {},
                "multitask_strategy": "enqueue",
                "input": input,
            }
        )
        run_id = str(record["run_id"])
        await self.broker.publish_control(
            Envelope(
                type="control",
                run_id=run_id,
                payload={
                    "kind": "start",
                    "run_id": run_id,
                    "thread_id": thread_id,
                    "graph_id": graph_id,
                    "input": input,
                    "extra_configurable": {},
                },
            )
        )
        return run_id

    async def wait_terminal(self, run_id: str, timeout: float = 10.0) -> str:
        for _ in range(int(timeout / 0.2)):
            record = await self.runs.get(run_id)
            if record and record["status"] in ("success", "error", "interrupted", "timeout"):
                return str(record["status"])
            await asyncio.sleep(0.2)
        return "timeout"

    async def aclose(self) -> None:
        await self.dispatcher.aclose()
        for task in self._tasks:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task


@pytest.fixture
async def env(tmp_path: Path) -> AsyncIterator[tuple[BrokerSettings, Redis, Path]]:
    settings = BrokerSettings()
    client = Redis.from_url(settings.redis_url)
    try:
        await client.ping()
    except Exception:
        pytest.skip("需要本地 Redis：docker compose up -d")
    (tmp_path / "hitl.py").write_text(HITL_PY, encoding="utf-8")
    (tmp_path / "slow.py").write_text(SLOW_PY, encoding="utf-8")
    (tmp_path / "langgraph.json").write_text(
        '{"graphs": {"hitl": "./hitl.py:graph", "slow": "./slow.py:graph"}}',
        encoding="utf-8",
    )
    settings = BrokerSettings(stream_prefix=f"slg-dbg-{uuid.uuid4().hex[:6]}")
    yield settings, client, tmp_path / "langgraph.json"
    await client.aclose()


async def test_orphan_run_readopted_after_driver_death(
    env: tuple[BrokerSettings, Redis, Path],
) -> None:
    settings, client, config_path = env

    # driver 1：短路 _finish，模拟「瞬间死亡、来不及写任何终态」
    stack1 = DriverStack(settings, client, config_path)

    async def dead_finish(
        run_id: str, thread_id: str, status: str, error: str | None = None
    ) -> None:
        return None

    stack1.dispatcher._executor._finish = dead_finish
    thread_id = f"th-{uuid.uuid4().hex[:8]}"
    run_id = await stack1.start_run("slow", thread_id, input={"count": 0})
    record: dict[str, Any] | None = None
    for _ in range(30):
        record = await stack1.runs.get(run_id)
        if record and record["status"] == "running":
            break
        await asyncio.sleep(0.2)
    assert record is not None and record["status"] == "running"

    # 瞬间死亡：协程全灭 + 心跳删除，不留任何终态
    await stack1.aclose()
    await client.delete(f"{settings.stream_prefix}:hb:{run_id}")

    stack2 = DriverStack(settings, client, config_path)
    ControlDispatcher.REAP_INTERVAL_SECONDS = 0.5
    with contextlib.suppress(Exception):
        await stack2.dispatcher._reap_orphans()
    try:
        final = "timeout"
        for _ in range(40):
            record = await stack2.runs.get(run_id)
            if record and record["status"] in ("success", "interrupted"):
                final = str(record["status"])
                break
            await asyncio.sleep(0.5)
        assert final == "success", "孤儿 run 应被第二个 driver 认领并恢复至 success"
        assert await stack2.runs.heartbeat_alive(run_id), "恢复后的 run 应重新有心跳"
    finally:
        ControlDispatcher.REAP_INTERVAL_SECONDS = 10.0
        await stack2.aclose()
