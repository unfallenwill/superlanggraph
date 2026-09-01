"""threads 路由：线程 CRUD、状态、历史与 fork。"""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any, cast

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from superlanggraph.core.serde import dumps_json_text
from superlanggraph.server.runtime import ServerRuntime
from superlanggraph.server.sse import sse_frame

router = APIRouter()


def _runtime(request: Request) -> ServerRuntime:
    return request.app.state.runtime


def _decode(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _thread_key(thread_id: str) -> str:
    return f"slg:thread:{thread_id}"


async def _get_thread_record(runtime: ServerRuntime, thread_id: str) -> dict[str, Any]:
    raw = await runtime.redis.hgetall(_thread_key(thread_id))
    if not raw:
        raise HTTPException(404, f"thread 不存在: {thread_id}")
    return {_decode(k): _decode(v) for k, v in raw.items()}


def _to_thread_state(snapshot: Any, thread_id: str) -> dict[str, Any]:
    """官方 StateSnapshot → SDK ThreadState 形状。"""
    checkpoint = {
        "thread_id": thread_id,
        "checkpoint_ns": _configurable(snapshot.config).get("checkpoint_ns", ""),
        "checkpoint_id": _configurable(snapshot.config).get("checkpoint_id"),
    }
    parent = snapshot.parent_config
    return {
        "values": json.loads(dumps_json_text(snapshot.values)),
        "next": list(snapshot.next),
        "checkpoint": checkpoint,
        "metadata": json.loads(dumps_json_text(snapshot.metadata or {})),
        "created_at": snapshot.created_at,
        "parent_checkpoint": (
            {
                "thread_id": thread_id,
                "checkpoint_ns": _configurable(parent).get("checkpoint_ns", ""),
                "checkpoint_id": _configurable(parent).get("checkpoint_id"),
            }
            if parent
            else None
        ),
        "tasks": [
            {
                "id": task.id,
                "name": task.name,
                "path": list(task.path),
                "error": repr(task.error) if task.error else None,
                "interrupts": [_interrupt_json(i) for i in task.interrupts],
                "state": None,
                "result": None,
            }
            for task in snapshot.tasks
        ],
        "interrupts": [_interrupt_json(i) for task in snapshot.tasks for i in task.interrupts],
    }


def _configurable(config: Any) -> dict[str, Any]:
    return dict(config.get("configurable") or {})


def _interrupt_json(interrupt: Any) -> dict[str, Any]:
    return {"value": json.loads(dumps_json_text(interrupt.value)), "id": interrupt.id}


def _thread_config(
    runtime: ServerRuntime, thread_id: str, checkpoint: Any, checkpoint_id: str | None
) -> dict[str, Any]:
    configurable: dict[str, Any] = {"thread_id": thread_id, "checkpoint_ns": ""}
    if checkpoint_id:
        configurable["checkpoint_id"] = checkpoint_id
    elif checkpoint:
        configurable["checkpoint_id"] = checkpoint.get("checkpoint_id")
    return {"configurable": configurable}


@router.post("/threads")
async def create_thread(request: Request) -> dict[str, Any]:
    runtime = _runtime(request)
    payload = await request.json()
    thread_id = str(payload.get("thread_id") or uuid.uuid4().hex)
    if payload.get("if_exists") == "raise" and await runtime.redis.exists(_thread_key(thread_id)):
        raise HTTPException(409, f"thread 已存在: {thread_id}")
    record = {
        "thread_id": thread_id,
        "status": "idle",
        "metadata": json.dumps(payload.get("metadata") or {}),
        "created_at": _now(),
        "updated_at": _now(),
    }
    await runtime.redis.hset(_thread_key(thread_id), mapping=record)  # type: ignore[arg-type]
    supersteps = payload.get("supersteps")
    if supersteps:
        await _import_supersteps(runtime, thread_id, supersteps)
    return _thread_json(runtime, thread_id, record)


async def _import_supersteps(runtime: ServerRuntime, thread_id: str, supersteps: Any) -> None:
    """线程导入：把 supersteps 序列逐条回放进检查点链。

    每条 update（values + as_node）经官方 ``aupdate_state`` 落成真实
    checkpoint——后续 resume / 时间旅行 / get_history 全部走官方机制。
    """
    if not isinstance(supersteps, list):
        raise HTTPException(422, "supersteps 必须是列表")
    graph_id = await _graph_id_for_thread(runtime, thread_id)
    graph = runtime.registry.compile_local(graph_id, checkpointer=runtime.saver)
    config: dict[str, Any] = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    try:
        steps = cast("list[dict[str, Any]]", supersteps)
        for step in steps:
            updates = cast("list[dict[str, Any]]", step.get("updates") or [])
            for update in updates:
                values = cast("dict[str, Any] | None", update.get("values"))
                if values is None:
                    continue
                as_node = cast("str | None", update.get("as_node"))
                await graph.aupdate_state(config, values, as_node=as_node)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(422, f"supersteps 导入失败: {exc}") from exc


def _thread_json(runtime: ServerRuntime, thread_id: str, record: dict[str, Any]) -> dict[str, Any]:
    return {
        "thread_id": thread_id,
        "status": record.get("status", "idle"),
        "metadata": json.loads(record.get("metadata") or "{}"),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        "values": None,
        "interrupts": None,
        "config": None,
        "ttl": json.loads(record["ttl"]) if record.get("ttl") else None,
    }


@router.get("/threads/{thread_id}")
async def get_thread(thread_id: str, request: Request) -> dict[str, Any]:
    runtime = _runtime(request)
    record = await _get_thread_record(runtime, thread_id)
    return await _thread_with_state(runtime, thread_id, record)


async def _thread_with_state(
    runtime: ServerRuntime, thread_id: str, record: dict[str, Any]
) -> dict[str, Any]:
    graph_id = await _graph_id_for_thread(runtime, thread_id)
    body = _thread_json(runtime, thread_id, record)
    try:
        graph = runtime.registry.compile_local(graph_id, checkpointer=runtime.saver)
        snapshot = await graph.aget_state({"configurable": {"thread_id": thread_id}})
        body["values"] = json.loads(dumps_json_text(snapshot.values))
        active = await runtime.runs.get_active(thread_id)
        # 静态断点（interrupt_after 等）挂起时 tasks 无 interrupts，
        # 以最近一次 run 的终态作为线程 interrupted 的补充判定。
        recent = await runtime.runs.list_for_thread(thread_id, limit=1)
        last_status = recent[0]["status"] if recent else "idle"
        interrupted = any(task.interrupts for task in snapshot.tasks) or last_status == (
            "interrupted"
        )
        body["status"] = "busy" if active else ("interrupted" if interrupted else "idle")
        body["interrupts"] = [
            _interrupt_json(i) for task in snapshot.tasks for i in task.interrupts
        ]
    except Exception as exc:
        # 状态读取失败时保留基础字段，不阻塞响应。
        print(f"thread state 读取失败: {exc!r}", flush=True)
    return body


async def _graph_id_for_thread(runtime: ServerRuntime, thread_id: str) -> str:
    recorded = await runtime.redis.get(f"slg:thread-graph:{thread_id}")
    if recorded is None:
        return runtime.registry.default_graph_id()
    return _decode(recorded)


@router.patch("/threads/{thread_id}")
async def update_thread(thread_id: str, request: Request) -> dict[str, Any]:
    runtime = _runtime(request)
    payload = await request.json()
    record = await _get_thread_record(runtime, thread_id)
    if payload.get("metadata") is not None:
        record["metadata"] = json.dumps(payload["metadata"])
    ttl = payload.get("ttl")
    if ttl is not None:
        record["ttl"] = json.dumps(ttl)
    record["updated_at"] = _now()
    await runtime.redis.hset(_thread_key(thread_id), mapping=record)  # type: ignore[arg-type]
    return _thread_json(runtime, thread_id, record)


@router.delete("/threads/{thread_id}")
async def delete_thread(thread_id: str, request: Request) -> None:
    runtime = _runtime(request)
    await _get_thread_record(runtime, thread_id)
    graph_id = await _graph_id_for_thread(runtime, thread_id)
    graph = runtime.registry.compile_local(graph_id, checkpointer=runtime.saver)
    await graph.checkpointer.adelete_thread(thread_id)  # type: ignore[union-attr]
    await runtime.redis.delete(_thread_key(thread_id), f"slg:thread-graph:{thread_id}")


@router.post("/threads/search")
async def search_threads(request: Request) -> list[dict[str, Any]]:
    runtime = _runtime(request)
    payload = await request.json()
    limit = int(payload.get("limit", 10))
    offset = int(payload.get("offset", 0))
    wanted_metadata = cast("dict[str, Any]", payload.get("metadata") or {})
    wanted_values = cast("dict[str, Any]", payload.get("values") or {})
    wanted_status = payload.get("status")
    pattern = "slg:thread:*"
    matches: list[dict[str, Any]] = []
    async for raw_key in runtime.redis.scan_iter(match=pattern):  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        key = _decode(raw_key)
        if key.startswith("slg:thread-") or key == "slg:thread-graph":
            continue
        thread_id = key.rsplit(":", 1)[-1]
        record = await _get_thread_record(runtime, thread_id)
        metadata = json.loads(record.get("metadata") or "{}")
        if wanted_metadata and any(metadata.get(k) != v for k, v in wanted_metadata.items()):
            continue
        if wanted_status and record.get("status") != wanted_status:
            continue
        body = await _thread_with_state(runtime, thread_id, record)
        if wanted_values and any(
            body.get("values", {}).get(k) != v for k, v in wanted_values.items()
        ):
            continue
        matches.append(body)
    matches.sort(key=lambda body: str(body.get("created_at")), reverse=True)
    return matches[offset : offset + limit]


@router.post("/threads/count")
async def count_threads(request: Request) -> int:
    runtime = _runtime(request)
    payload = await request.json()
    found = await search_threads_for_count(runtime, payload)
    return found


async def search_threads_for_count(runtime: ServerRuntime, payload: dict[str, Any]) -> int:
    bodies = await _collect_thread_bodies(runtime, payload)
    return len(bodies)


async def _collect_thread_bodies(
    runtime: ServerRuntime, payload: dict[str, Any]
) -> list[dict[str, Any]]:
    pattern = "slg:thread:*"
    result: list[dict[str, Any]] = []
    async for raw_key in runtime.redis.scan_iter(match=pattern):  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        key = _decode(raw_key)
        if key.startswith("slg:thread-"):
            continue
        thread_id = key.rsplit(":", 1)[-1]
        record = await _get_thread_record(runtime, thread_id)
        result.append(await _thread_with_state(runtime, thread_id, record))
    wanted_status = payload.get("status")
    if wanted_status:
        result = [b for b in result if b.get("status") == wanted_status]
    return result


@router.post("/threads/{thread_id}/copy")
async def copy_thread(thread_id: str, request: Request) -> None:
    """复制线程元数据；状态随 checkpoint 层实现演进（v1 仅复制记录）。"""
    runtime = _runtime(request)
    record = await _get_thread_record(runtime, thread_id)
    new_id = uuid.uuid4().hex
    record["thread_id"] = new_id
    record["created_at"] = _now()
    record["updated_at"] = _now()
    await runtime.redis.hset(_thread_key(new_id), mapping=record)  # type: ignore[arg-type]
    graph_id = await _graph_id_for_thread(runtime, thread_id)
    await runtime.redis.set(f"slg:thread-graph:{new_id}", graph_id)


@router.post("/threads/prune")
async def prune_threads(request: Request) -> list[str]:
    """按策略清理线程；v1 支持 delete（全删）与 keep_latest（仅删记录）。"""
    runtime = _runtime(request)
    payload = await request.json()
    strategy = str(payload.get("strategy", "delete"))
    pruned: list[str] = []
    for thread_id in payload.get("thread_ids", []):
        record = await runtime.redis.hgetall(_thread_key(str(thread_id)))
        if not record:
            continue
        if strategy == "delete":
            graph_id = await _graph_id_for_thread(runtime, str(thread_id))
            graph = runtime.registry.compile_local(graph_id, checkpointer=runtime.saver)
            await graph.checkpointer.adelete_thread(str(thread_id))  # type: ignore[union-attr]
        await runtime.redis.delete(_thread_key(str(thread_id)))
        pruned.append(str(thread_id))
    return pruned


@router.get("/threads/{thread_id}/state")
async def get_thread_state(
    thread_id: str, request: Request, subgraphs: bool = False
) -> dict[str, Any]:
    runtime = _runtime(request)
    await _get_thread_record(runtime, thread_id)
    graph_id = await _graph_id_for_thread(runtime, thread_id)
    graph = runtime.registry.compile_local(graph_id, checkpointer=runtime.saver)
    snapshot = await graph.aget_state({"configurable": {"thread_id": thread_id}})
    return _to_thread_state(snapshot, thread_id)


@router.get("/threads/{thread_id}/state/{checkpoint_id}")
async def get_thread_state_at(
    thread_id: str, checkpoint_id: str, request: Request
) -> dict[str, Any]:
    runtime = _runtime(request)
    await _get_thread_record(runtime, thread_id)
    graph_id = await _graph_id_for_thread(runtime, thread_id)
    graph = runtime.registry.compile_local(graph_id, checkpointer=runtime.saver)
    snapshot = await graph.aget_state(
        {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_id": checkpoint_id,
            }
        }
    )
    return _to_thread_state(snapshot, thread_id)


@router.post("/threads/{thread_id}/state/checkpoint")
async def get_thread_state_by_checkpoint(thread_id: str, request: Request) -> dict[str, Any]:
    runtime = _runtime(request)
    payload = await request.json()
    await _get_thread_record(runtime, thread_id)
    graph_id = await _graph_id_for_thread(runtime, thread_id)
    graph = runtime.registry.compile_local(graph_id, checkpointer=runtime.saver)
    snapshot = await graph.aget_state(
        _thread_config(runtime, thread_id, payload.get("checkpoint"), None)
    )
    return _to_thread_state(snapshot, thread_id)


@router.post("/threads/{thread_id}/state")
async def update_thread_state(thread_id: str, request: Request) -> dict[str, Any]:
    runtime = _runtime(request)
    payload = await request.json()
    await _get_thread_record(runtime, thread_id)
    graph_id = await _graph_id_for_thread(runtime, thread_id)
    graph = runtime.registry.compile_local(graph_id, checkpointer=runtime.saver)
    checkpoint = payload.get("checkpoint")
    config: dict[str, Any] = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    if checkpoint and checkpoint.get("checkpoint_id"):
        config["configurable"]["checkpoint_id"] = checkpoint["checkpoint_id"]
    result = await graph.aupdate_state(
        config, payload.get("values"), as_node=payload.get("as_node")
    )
    new_config = _configurable(result)
    return {
        "checkpoint": {
            "thread_id": thread_id,
            "checkpoint_ns": new_config.get("checkpoint_ns", ""),
            "checkpoint_id": new_config.get("checkpoint_id"),
        }
    }


@router.post("/threads/{thread_id}/history")
async def get_thread_history(thread_id: str, request: Request) -> list[dict[str, Any]]:
    runtime = _runtime(request)
    payload = await request.json()
    await _get_thread_record(runtime, thread_id)
    graph_id = await _graph_id_for_thread(runtime, thread_id)
    graph = runtime.registry.compile_local(graph_id, checkpointer=runtime.saver)
    before = payload.get("before")
    before_config: dict[str, Any] | None = None
    if isinstance(before, dict):
        before_obj = cast("dict[str, Any]", before)
        before_config = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_id": cast("str | None", before_obj.get("checkpoint_id")),
            }
        }
    elif isinstance(before, str):
        before_config = {"configurable": {"thread_id": thread_id, "checkpoint_id": before}}
    states: list[dict[str, Any]] = []
    async for snapshot in graph.aget_state_history(
        {"configurable": {"thread_id": thread_id}},
        limit=payload.get("limit", 10),
        filter=payload.get("metadata"),
        before=before_config,
    ):
        states.append(_to_thread_state(snapshot, thread_id))
    return states


@router.post("/threads/{thread_id}/stream")
async def stream_thread(thread_id: str, request: Request) -> StreamingResponse:
    """订阅线程级事件（lifecycle / state_update / run_modes）。"""
    runtime = _runtime(request)
    payload = await request.json()
    modes = payload.get("stream_mode") or ["lifecycle"]
    requested = {modes} if isinstance(modes, str) else set(modes)
    await _get_thread_record(runtime, thread_id)
    generator = _tail_thread_events(runtime, thread_id, requested)
    return StreamingResponse(
        generator,
        media_type="text/event-stream",
        headers={"Cache-Control": "no-store"},
    )


async def _tail_thread_events(
    runtime: ServerRuntime, thread_id: str, requested: set[str]
) -> AsyncIterator[str]:
    async for entry_id, envelope in runtime.events.read_events(f"thread:{thread_id}", last_id="0"):
        payload = envelope.payload
        event = str(payload.get("event", ""))
        if event == "end":
            yield sse_frame("end", str(payload.get("data") or "{}"))
            return
        base = event.split("|", maxsplit=1)[0]
        if base in requested or event in requested:
            yield sse_frame(event, str(payload.get("data") or "null"), event_id=entry_id)
