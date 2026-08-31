"""P1 合同参数行为验证：on_disconnect=cancel、threads ttl、durability 透传。"""

from __future__ import annotations

import asyncio
from typing import Any


async def test_on_disconnect_cancel(client: Any, server_url: str) -> None:
    """on_disconnect=cancel：客户端断开 SSE 后，run 被取消为 interrupted。"""
    thread_id = (await client.threads.create())["thread_id"]
    run = client.runs.stream(
        thread_id,
        "slow",
        input={"count": 0},
        stream_mode="values",
        on_disconnect="cancel",
    )
    async for _chunk in run:
        break  # 收到首帧即断开，模拟客户端掉线
    await run.aclose()

    status = "running"
    for _ in range(50):
        runs = await client.runs.list(thread_id)
        if runs:
            status = runs[0]["status"]
            if status in ("interrupted", "success", "error"):
                break
        await asyncio.sleep(0.2)
    assert status == "interrupted"


async def test_threads_update_ttl_roundtrip(client: Any) -> None:
    thread_id = (await client.threads.create())["thread_id"]
    ttl = {"strategy": "delete", "ttl": 60}
    updated = await client.threads.update(thread_id, metadata={}, ttl=ttl)
    assert updated["ttl"] == ttl


async def test_durability_param_accepted(client: Any, server_url: str) -> None:
    thread_id = (await client.threads.create())["thread_id"]
    waited = await client.runs.wait(thread_id, "agent", input={"count": 3}, durability="sync")
    assert waited["count"] == 7  # 3*2+1，durability 透传不影响结果


async def test_list_select_projection(client: Any) -> None:
    thread_id = (await client.threads.create())["thread_id"]
    run = await client.runs.create(thread_id, "agent", input={"count": 1})
    runs = await client.runs.list(thread_id, select=["run_id", "status"])
    assert runs, "至少有一条 run"
    assert set(runs[0].keys()) <= {"run_id", "status"}
    _ = run
