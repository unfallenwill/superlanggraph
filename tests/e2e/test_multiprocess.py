# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownParameterType=false, reportUnknownArgumentType=false, reportMissingTypeArgument=false, reportIndexIssue=false, reportCallIssue=false, reportUnnecessaryIsInstance=false
# 子进程与 httpx 流式接口的类型噪音；e2e 是类型边界，src/ 不受影响。

"""多进程角色拆分端到端：server / driver / worker 各自独立进程，SDK 全链路验证。

架构 D2 的正式部署形态：HTTP 接入与后台角色完全隔离，仅经 Redis 通信。
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import socket
import subprocess
import sys
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
from langgraph_sdk import get_client
from redis.asyncio import Redis

from superlanggraph.core.settings import BrokerSettings

AGENT_PY = '''
"""测试图：单节点整数变换。"""
from typing import TypedDict
from langgraph.graph import END, START, StateGraph


class State(TypedDict, total=False):
    count: int


def double(state: State) -> dict:
    return {"count": state.get("count", 0) * 2 + 1}


builder = StateGraph(State)
builder.add_node("double", double)
builder.add_edge(START, "double")
builder.add_edge("double", END)
graph = builder.compile()
'''

HITL_PY = '''
"""测试图：interrupt 人工介入。"""
from typing import TypedDict
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt


class State(TypedDict, total=False):
    count: int


def ask(state: State) -> dict:
    answer = interrupt({"question": "给个数字"})
    return {"count": int(answer)}


def bump(state: State) -> dict:
    return {"count": state.get("count", 0) + 10}


builder = StateGraph(State)
builder.add_node("ask", ask)
builder.add_node("bump", bump)
builder.add_edge(START, "ask")
builder.add_edge("ask", "bump")
builder.add_edge("bump", END)
graph = builder.compile()
'''


async def _can_reach_redis(url: str) -> bool:
    client = Redis.from_url(url, socket_connect_timeout=0.5, socket_timeout=0.5)
    try:
        return bool(await client.ping())
    except Exception:
        return False
    finally:
        await client.aclose()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _spawn(role: str, prefix: str, config: Path, port: int | None) -> subprocess.Popen:
    """以独立子进程启动一个角色（真实 CLI 入口，非进程内模拟）。"""
    cmd = [sys.executable, "-m", "superlanggraph.cli", role, "--config", str(config)]
    if port is not None:
        cmd += ["--port", str(port)]
    env = {
        **os.environ,
        "SLG_STREAM_PREFIX": prefix,
    }
    return subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


@pytest.fixture
async def mp_stack(tmp_path: Path) -> AsyncIterator[tuple[str, list[subprocess.Popen]]]:
    if not await _can_reach_redis(BrokerSettings().redis_url):
        pytest.skip("需要本地 Redis：docker compose up -d")

    (tmp_path / "agent.py").write_text(AGENT_PY, encoding="utf-8")
    (tmp_path / "hitl.py").write_text(HITL_PY, encoding="utf-8")
    (tmp_path / "langgraph.json").write_text(
        '{"graphs": {"agent": "./agent.py:graph", "hitl": "./hitl.py:graph"}}',
        encoding="utf-8",
    )
    prefix = f"slg-mp-{uuid.uuid4().hex[:8]}"
    port = _free_port()

    server = _spawn("server", prefix, tmp_path / "langgraph.json", port)
    driver = _spawn("driver", prefix, tmp_path / "langgraph.json", None)
    worker = _spawn("worker", prefix, tmp_path / "langgraph.json", None)
    procs = [server, driver, worker]

    base_url = f"http://127.0.0.1:{port}"
    async with httpx.AsyncClient() as probe:
        for _ in range(150):
            try:
                response = await probe.get(f"{base_url}/health")
                if response.status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.1)
        else:
            for proc in procs:
                proc.kill()
            pytest.fail("多进程 server 未能启动")

    yield base_url, procs

    for proc in procs:
        proc.terminate()
    for proc in procs:
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=5)


async def test_cross_process_stream_and_resume(mp_stack: tuple[str, list[Any]]) -> None:
    """HTTP 在 A 进程、图循环在 B 进程、节点执行在 C 进程——SDK 全程无感。"""
    base_url, _ = mp_stack
    client = get_client(url=base_url, api_key="e2e-secret-key")
    thread_id = (await client.threads.create())["thread_id"]

    # 常规流：driver 与 worker 都在独立进程
    waited = cast(
        "dict[str, Any]",
        await client.runs.wait(thread_id, "agent", input={"count": 2}),
    )
    assert waited["count"] == 5

    # HITL：interrupt 暂停（跨进程中断信号回传）→ resume 续跑
    run = client.runs.stream(thread_id, "hitl", input={"count": 0}, stream_mode="values")
    interrupts: list[Any] = []
    async for chunk in run:
        data = chunk.data
        if isinstance(data, dict) and data.get("__interrupt__"):
            interrupts = list(data["__interrupt__"])
        if chunk.event == "end":
            break
    assert interrupts, "跨进程模式下 values 流应携带 __interrupt__"

    resumed = cast(
        "dict[str, Any]",
        await client.runs.wait(thread_id, "hitl", command={"resume": 7}),
    )
    assert resumed["count"] == 17  # 7 + 10，续跑在独立 worker 完成
