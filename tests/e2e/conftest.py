# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
# redis-py 部分方法存根类型不完整；测试为类型边界，src/ 不受影响。

"""E2E 验收夹具：真实 uvicorn 服务 + 真 Redis + 未修改的 langgraph-sdk。"""

from __future__ import annotations

import asyncio
import json
import socket
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import uvicorn
from langgraph_sdk import get_client
from redis.asyncio import Redis

from superlanggraph.cli import build_app
from superlanggraph.core.settings import AuthSettings, BrokerSettings

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

SLOW_PY = '''
"""慢节点图：制造持续 busy 的线程。"""
import time
from typing import TypedDict
from langgraph.graph import END, START, StateGraph


class State(TypedDict, total=False):
    count: int


def slow(state: State) -> dict:
    time.sleep(1.5)
    return {"count": state.get("count", 0) + 1}


builder = StateGraph(State)
builder.add_node("slow", slow)
builder.add_edge(START, "slow")
builder.add_edge("slow", END)
graph = builder.compile()
'''

AUTH_API_KEY = "e2e-secret-key"


@pytest.fixture
def client(server_url: str) -> Any:
    return get_client(url=server_url, api_key=AUTH_API_KEY)


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


@pytest.fixture
async def server_url(tmp_path: Path) -> AsyncIterator[str]:
    if not await _can_reach_redis(BrokerSettings().redis_url):
        pytest.skip("需要本地 Redis：docker compose up -d")
    config_path = tmp_path / "langgraph.json"
    (tmp_path / "agent.py").write_text(AGENT_PY, encoding="utf-8")
    (tmp_path / "hitl.py").write_text(HITL_PY, encoding="utf-8")
    (tmp_path / "slow.py").write_text(SLOW_PY, encoding="utf-8")
    config_path.write_text(
        json_dumps(
            {
                "graphs": {
                    "agent": "./agent.py:graph",
                    "hitl": "./hitl.py:graph",
                    "slow": "./slow.py:graph",
                }
            }
        ),
        encoding="utf-8",
    )
    settings = BrokerSettings(stream_prefix=f"slg-e2e-{uuid.uuid4().hex[:8]}")
    app = build_app(settings, config_path, auth=AuthSettings(api_keys=AUTH_API_KEY))
    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(app, port=port, log_level="warning"))
    serve_task = asyncio.create_task(server.serve())
    base_url = f"http://127.0.0.1:{port}"
    async with httpx.AsyncClient() as probe:
        for _ in range(100):
            if serve_task.done():
                exc = serve_task.exception()
                pytest.fail(f"uvicorn 启动即失败: {exc!r}")
            try:
                response = await probe.get(f"{base_url}/health")
                if response.status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.05)
        else:
            pytest.fail("server 未能启动")
    yield base_url
    server.should_exit = True
    await serve_task


def json_dumps(data: dict[str, Any]) -> str:

    return json.dumps(data)
