# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
# langgraph-sdk 存根泛型噪音；测试为类型边界。

"""鉴权与动态断点端到端测试。"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from conftest import AUTH_API_KEY
from langgraph_sdk import get_client


async def test_invalid_api_key_rejected(server_url: str) -> None:
    """启用了鉴权的 server：错误 key 必须 401，正确 key 放行。"""
    bad_client = get_client(url=server_url, api_key="wrong-key")
    with pytest.raises(httpx.HTTPStatusError) as exc:
        await bad_client.threads.create()
    assert exc.value.response.status_code == 401

    good_client = get_client(url=server_url, api_key=AUTH_API_KEY)
    thread = await good_client.threads.create()
    assert thread["thread_id"]


async def test_interrupt_after_breakpoint(server_url: str, client: Any) -> None:
    """interrupt_after 在指定节点完成后暂停，resume 后继续到终态。"""
    thread_id = (await client.threads.create())["thread_id"]

    chunks: list[tuple[str, Any]] = []
    run = client.runs.stream(
        thread_id,
        "agent",
        input={"count": 2},
        stream_mode="values",
        interrupt_after=["double"],
    )
    async for chunk in run:
        chunks.append((chunk.event, chunk.data))
        if chunk.event == "end":
            break
    end_data = [data for event, data in chunks if event == "end" and isinstance(data, dict)]
    assert end_data and end_data[-1].get("status") == "interrupted"

    thread = await client.threads.get(thread_id)
    assert thread["status"] == "interrupted"
    assert thread["values"]["count"] == 5  # 断点前的执行结果已保留

    resumed = await client.runs.wait(thread_id, "agent", command={"resume": None})
    assert resumed["count"] == 5  # 已到 END，恢复后无新节点执行


async def test_health_is_exempt_from_auth(server_url: str) -> None:
    """/health 永远放行：探活不依赖鉴权配置。"""
    async with httpx.AsyncClient() as probe:
        response = await probe.get(f"{server_url}/health")
    assert response.status_code == 200
