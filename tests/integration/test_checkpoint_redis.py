# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownParameterType=false, reportMissingTypeArgument=false
# langgraph 对未参数化 StateGraph 的泛型推断不完整，测试作为类型边界放宽这几条；
# src/ 不受影响。

"""RedisSaver 集成测试：通过官方编译图机制验证，而非直接打桩。"""

import uuid
from collections.abc import AsyncIterator
from typing import Annotated, Any, TypedDict

import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph
from redis.asyncio import Redis

from superlanggraph.checkpoint import RedisSaver
from superlanggraph.core.settings import BrokerSettings


async def _can_reach_redis(url: str) -> bool:
    client = Redis.from_url(url, socket_connect_timeout=0.5, socket_timeout=0.5)
    try:
        return bool(await client.ping())
    except Exception:
        return False
    finally:
        await client.aclose()


def _add(a: int, b: int) -> int:
    return a + b


class _State(TypedDict):
    total: Annotated[int, _add]


def _build_graph(checkpointer: RedisSaver) -> Any:
    builder = StateGraph(_State)

    async def add_three(state: _State) -> dict[str, int]:
        return {"total": 3}

    builder.add_node("add_three", add_three)
    builder.add_edge(START, "add_three")
    builder.add_edge("add_three", END)
    return builder.compile(checkpointer=checkpointer)


@pytest.fixture
async def saver() -> AsyncIterator[RedisSaver]:
    settings = BrokerSettings()
    if not await _can_reach_redis(settings.redis_url):
        pytest.skip("需要本地 Redis：docker compose up -d")
    client = Redis.from_url(settings.redis_url)
    yield RedisSaver(client, prefix=f"slg-test-ckpt-{uuid.uuid4().hex[:8]}")
    await client.aclose()


async def test_checkpoint_survives_and_accumulates(saver: RedisSaver) -> None:
    graph = _build_graph(saver)
    thread: RunnableConfig = {"configurable": {"thread_id": f"t-{uuid.uuid4().hex[:8]}"}}

    await graph.ainvoke({"total": 1}, thread)
    await graph.ainvoke({"total": 2}, thread)

    state = await graph.aget_state(thread)
    assert state.values["total"] == 9  # run1: 1+3，run2: 4+2 再 +3，跨 run 累积

    history = [snap async for snap in graph.aget_state_history(thread)]
    assert len(history) >= 2
    ids = [str(dict(snap.config["configurable"]).get("checkpoint_id")) for snap in history]
    assert ids == sorted(ids, reverse=True)  # 历史按新到旧排列

    await saver.adelete_thread(str(thread["configurable"]["thread_id"]))
    assert (await graph.aget_state(thread)).values == {}


async def test_get_tuple_with_explicit_checkpoint_id(saver: RedisSaver) -> None:
    graph = _build_graph(saver)
    thread: RunnableConfig = {"configurable": {"thread_id": f"t-{uuid.uuid4().hex[:8]}"}}

    await graph.ainvoke({"total": 1}, thread)
    latest = await saver.aget_tuple(thread)
    assert latest is not None

    fetched = await saver.aget_tuple(latest.config)
    assert fetched is not None
    assert fetched.checkpoint["id"] == latest.checkpoint["id"]

    parent_config = fetched.parent_config
    if parent_config is not None:
        parent = await saver.aget_tuple(parent_config)
        assert parent is not None
        assert parent.checkpoint["id"] != fetched.checkpoint["id"]
