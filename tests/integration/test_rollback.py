# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
# redis-py 部分方法存根类型不完整；测试为类型边界，src/ 不受影响。

"""精确 rollback 集成测试：B 回滚后，线程状态必须回到 run A 结束时的样子。"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
import redis.exceptions
from redis.asyncio import Redis

from superlanggraph.broker.redis import RedisBroker
from superlanggraph.broker.runs import RunStore
from superlanggraph.checkpoint.redis_saver import RedisSaver
from superlanggraph.core.envelope import Envelope
from superlanggraph.core.settings import BrokerSettings
from superlanggraph.driver.runner import ControlDispatcher, RunExecutor
from superlanggraph.registry.loader import Registry
from superlanggraph.worker.executor import NodeWorker

HITL_PY = """
from typing import TypedDict
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt


class State(TypedDict, total=False):
    count: int


def increment(state):
    return {"count": state.get("count", 0) + 1}


def approve(state):
    answer = interrupt({"question": "批准？"})
    return {"count": int(answer)}


builder = StateGraph(State)
builder.add_node("increment", increment)
builder.add_node("approve", approve)
builder.add_edge(START, "increment")
builder.add_edge("increment", "approve")
builder.add_edge("approve", END)
graph = builder.compile()
"""


class Stack:
    """driver + worker + run 记录的测试组装。"""

    def __init__(self, settings: BrokerSettings, client: Redis, config_path: Path) -> None:
        self.broker = RedisBroker(settings, client)
        self.runs = RunStore(client, prefix=settings.stream_prefix)
        self.saver = RedisSaver(client)
        registry = Registry(config_path)
        executor = RunExecutor(
            registry, self.saver, self.broker, self.broker, self.broker, self.runs
        )
        self.dispatcher = ControlDispatcher(executor, self.runs, self.saver, self.broker)
        worker = NodeWorker(registry, self.broker, self.broker, self.broker)
        self._tasks = [
            asyncio.create_task(self.dispatcher.serve_forever()),
            asyncio.create_task(worker.serve_forever()),
        ]

    async def start_run(self, thread_id: str, *, input: Any, command: Any = None) -> str:
        record = await self.runs.create(
            {
                "thread_id": thread_id,
                "assistant_id": "hitl",
                "graph_id": "hitl",
                "metadata": {},
                "multitask_strategy": "enqueue",
                "input": input,
                "command": command,
            }
        )
        run_id = str(record["run_id"])
        payload: dict[str, Any] = {
            "kind": "start",
            "run_id": run_id,
            "thread_id": thread_id,
            "graph_id": "hitl",
            "input": input,
            "extra_configurable": {},
        }
        if command is not None:
            payload["command"] = command
        await self.broker.publish_control(Envelope(type="control", run_id=run_id, payload=payload))
        return run_id

    async def wait_status(self, run_id: str, terminal: tuple[str, ...], timeout: float) -> str:
        for _ in range(int(timeout / 0.2)):
            try:
                record = await self.runs.get(run_id)
            except redis.exceptions.ConnectionError:
                await asyncio.sleep(0.2)
                continue
            if record and record["status"] in terminal:
                return str(record["status"])
            await asyncio.sleep(0.2)
        return "timeout"

    async def thread_count(self, thread_id: str) -> int:
        tuple_ = await self.saver.aget_tuple(
            {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
        )
        assert tuple_ is not None
        values = tuple_.checkpoint["channel_values"]
        return int(values["count"])

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
    (tmp_path / "langgraph.json").write_text(
        '{"graphs": {"hitl": "./hitl.py:graph"}}', encoding="utf-8"
    )
    settings = BrokerSettings(stream_prefix=f"slg-dbg-{uuid.uuid4().hex[:6]}")
    yield settings, client, tmp_path / "langgraph.json"
    await client.aclose()


async def test_rollback_restores_thread_to_pre_run_state(
    env: tuple[BrokerSettings, Redis, Path],
) -> None:
    settings, client, config_path = env
    stack = Stack(settings, client, config_path)
    thread_id = f"th-{uuid.uuid4().hex[:8]}"

    # run A：increment(→1) → interrupt → resume(100) → count=100，正常完成
    run_a = await stack.start_run(thread_id, input={"count": 0})
    assert await stack.wait_status(run_a, ("interrupted",), 10) == "interrupted"
    resumed = await stack.start_run(thread_id, input=None, command={"resume": 100})
    assert await stack.wait_status(resumed, ("success",), 10) == "success"

    tuple_a = await stack.saver.aget_tuple(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    )
    assert tuple_a is not None
    state = await stack.thread_count(thread_id)
    assert state == 100
    history_after_a = len(
        [
            c
            async for c in stack.saver.alist(
                {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
            )
        ]
    )

    # run B：input 把 count 覆盖为 0 → increment 到 1 → interrupt 挂起
    run_b = await stack.start_run(thread_id, input={"count": 0})
    assert await stack.wait_status(run_b, ("interrupted",), 10) == "interrupted"
    assert await stack.thread_count(thread_id) == 1

    # rollback：精确删除 run B 产生的 checkpoint（base 之后），线程回到 count=100
    await stack.broker.publish_control(
        Envelope(
            type="control",
            run_id=run_b,
            payload={
                "kind": "cancel",
                "run_id": run_b,
                "thread_id": thread_id,
                "action": "rollback",
            },
        )
    )
    await asyncio.sleep(0.8)

    tuple_rollback = await stack.saver.aget_tuple(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    )
    assert tuple_rollback is not None
    values = tuple_rollback.checkpoint["channel_values"]
    assert int(values["count"]) == 100, "回滚后应回到 run A 结束时的状态（count=100）"

    history_after_rollback = len(
        [
            c
            async for c in stack.saver.alist(
                {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
            )
        ]
    )
    assert history_after_rollback == history_after_a, "run A 的 checkpoint 历史必须完好"

    # run B 标记 interrupted（保留记录供审计），但状态影响已撤销
    record = await stack.runs.get(run_b)
    assert record is not None and record["status"] == "interrupted"

    await stack.aclose()
