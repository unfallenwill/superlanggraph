# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownParameterType=false, reportUnknownArgumentType=false, reportMissingTypeArgument=false, reportUntypedFunctionDecorator=false, reportIndexIssue=false, reportCallIssue=false, reportOptionalSubscript=false, reportTypedDictNotRequiredAccess=false, reportGeneralTypeIssues=false, reportUnnecessaryIsInstance=false
# langgraph-sdk 的存根对未参数化泛型推断不完整；e2e 是类型边界，src/ 不受影响。

"""SDK 合同验收：langgraph-sdk 0.4.4 直连，五命名空间逐方法验证。"""

from __future__ import annotations

import uuid
from typing import Any, cast

import httpx
import pytest


async def test_runs_stream_and_wait(server_url: str, client: Any) -> None:
    thread_id = (await client.threads.create())["thread_id"]

    chunks: list[tuple[str, Any]] = []
    run = client.runs.stream(
        thread_id,
        "agent",
        input={"count": 2},
        stream_mode=["values", "updates"],
    )
    async for chunk in run:
        chunks.append((chunk.event, chunk.data))
        if chunk.event == "end":
            break
    events = [event for event, _ in chunks]
    assert events[0] == "metadata"
    assert chunks[0][1]["run_id"]
    assert "values" in events and "updates" in events

    final = max((data for e, data in chunks if e == "values"), key=lambda d: d["count"])
    assert final["count"] == 5  # 2*2+1，在 worker 远程执行

    waited = cast("dict[str, Any]", await client.runs.wait(thread_id, "agent", input={"count": 10}))
    assert waited["count"] == 21


async def test_runs_background_join_list_get(server_url: str, client: Any) -> None:
    thread_id = (await client.threads.create())["thread_id"]
    run = await client.runs.create(thread_id, "agent", input={"count": 1})
    assert run["status"] in ("pending", "running", "success")

    state = await client.runs.join(thread_id, run["run_id"])
    assert state["count"] == 3  # 1*2+1

    runs = await client.runs.list(thread_id)
    assert [r["run_id"] for r in runs] == [run["run_id"]]
    fetched = await client.runs.get(thread_id, run["run_id"])
    assert fetched["status"] == "success"

    await client.runs.delete(thread_id, run["run_id"])
    remaining = await client.runs.list(thread_id)
    assert remaining == []


async def test_runs_multitask_reject(server_url: str, client: Any) -> None:
    thread_id = (await client.threads.create())["thread_id"]
    first = await client.runs.create(thread_id, "slow", input={"count": 1})
    with pytest.raises(httpx.HTTPStatusError) as exc:
        await client.runs.create(thread_id, "slow", input={"count": 2}, multitask_strategy="reject")
    assert exc.value.response.status_code == 409
    await client.runs.cancel(thread_id, first["run_id"], wait=True, action="interrupt")


async def test_threads_lifecycle(server_url: str, client: Any) -> None:
    thread = await client.threads.create(metadata={"team": "slg"})
    thread_id = thread["thread_id"]
    assert thread["metadata"] == {"team": "slg"}

    fetched = await client.threads.get(thread_id)
    assert fetched["thread_id"] == thread_id

    updated = await client.threads.update(thread_id, metadata={"team": "slg", "v": 2})
    assert updated["metadata"]["v"] == 2

    await client.threads.update_state(thread_id, {"count": 7}, as_node="double")
    state = cast("dict[str, Any]", await client.threads.get_state(thread_id))
    assert state["values"]["count"] == 7

    history = await client.threads.get_history(thread_id)
    assert len(history) >= 1
    assert history[0]["checkpoint"]["thread_id"] == thread_id

    fork = await client.threads.get_state(thread_id, checkpoint_id=None)
    assert "values" in fork

    search = await client.threads.search(metadata={"team": "slg"})
    assert thread_id in [t["thread_id"] for t in search]

    count = await client.threads.count(status="idle")
    assert count >= 1

    copied = await client.threads.copy(thread_id)
    assert copied is None or isinstance(copied, dict)

    await client.threads.delete(thread_id)
    with pytest.raises(httpx.HTTPStatusError) as exc:
        await client.threads.get(thread_id)
    assert exc.value.response.status_code == 404


async def test_assistants_lifecycle(server_url: str, client: Any) -> None:
    assistant = await client.assistants.create("agent", metadata={"env": "test"})
    assistant_id = assistant["assistant_id"]
    assert assistant["graph_id"] == "agent"

    fetched = await client.assistants.get(assistant_id)
    assert fetched["name"] or fetched["assistant_id"] == assistant_id

    by_graph = await client.assistants.search(graph_id="agent", metadata={"env": "test"})
    assert assistant_id in [a["assistant_id"] for a in by_graph]

    updated = await client.assistants.update(assistant_id, metadata={"env": "prod"})
    assert updated["metadata"] == {"env": "prod"}

    versions = await client.assistants.get_versions(assistant_id)
    assert len(versions) >= 1

    latest = await client.assistants.set_latest(assistant_id, versions[0]["version"])
    assert latest["assistant_id"] == assistant_id

    schemas = await client.assistants.get_schemas(assistant_id)
    assert "input_schema" in schemas

    graph_schema = await client.assistants.get_graph(assistant_id)
    assert "nodes" in graph_schema and "edges" in graph_schema

    subgraphs = await client.assistants.get_subgraphs(assistant_id)
    assert isinstance(subgraphs, dict)

    counted = await client.assistants.count(metadata={"env": "prod"})
    assert counted >= 1

    await client.assistants.delete(assistant_id)
    with pytest.raises(httpx.HTTPStatusError) as exc:
        await client.assistants.get(assistant_id)
    assert exc.value.response.status_code == 404


async def test_store_lifecycle(server_url: str, client: Any) -> None:
    namespace = ["slg", "e2e", uuid.uuid4().hex]

    await client.store.put_item(namespace, "item-1", {"kind": "a", "score": 1})
    await client.store.put_item(namespace, "item-2", {"kind": "b", "score": 2})

    item = await client.store.get_item(namespace, "item-1")
    assert item["value"]["kind"] == "a"

    searched = await client.store.search_items(namespace, filter={"kind": "b"})
    assert [i["key"] for i in searched["items"]] == ["item-2"]

    namespaces = await client.store.list_namespaces(prefix=["slg"])
    assert namespace in namespaces

    await client.store.delete_item(namespace, "item-1")
    with pytest.raises(httpx.HTTPStatusError):
        await client.store.get_item(namespace, "item-1")


async def test_crons_lifecycle(server_url: str, client: Any) -> None:
    cron = await client.crons.create("agent", schedule="* * * * *", input={"count": 1})
    cron_id = cron["cron_id"]
    assert cron["schedule"] == "* * * * *"

    found = await client.crons.search(assistant_id="agent")
    assert cron_id in [c["cron_id"] for c in found]

    counted = await client.crons.count(assistant_id="agent")
    assert counted >= 1

    updated = await client.crons.update(cron_id, schedule="*/5 * * * *")
    assert updated["schedule"] == "*/5 * * * *"

    await client.crons.delete(cron_id)
    found_after = await client.crons.search()
    assert cron_id not in [c["cron_id"] for c in found_after]
