"""assistants / store / crons 路由。"""

from __future__ import annotations

import json
import urllib.parse
import uuid
from datetime import UTC, datetime
from typing import Any, cast

from fastapi import APIRouter, HTTPException, Request

from superlanggraph.cron.scheduler import CronRecord, new_cron_id, next_after, utc_now
from superlanggraph.server.body import json_body
from superlanggraph.server.runtime import ServerRuntime

router = APIRouter()


def _runtime(request: Request) -> ServerRuntime:
    return request.app.state.runtime


def _decode(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _now() -> str:
    return datetime.now(UTC).isoformat()


# ---- Assistants ----


@router.post("/assistants")
async def create_assistant(request: Request) -> dict[str, Any]:
    runtime = _runtime(request)
    payload = await json_body(request)
    graph_id = str(payload.get("graph_id") or "")
    if not runtime.registry.has(graph_id):
        raise HTTPException(404, f"图不存在: {graph_id}")
    assistant_id = str(payload.get("assistant_id") or uuid.uuid4().hex)
    name = str(payload.get("name") or assistant_id)
    record = {
        "assistant_id": assistant_id,
        "name": name,
        "graph_id": graph_id,
        "version": "1",
        "config": json.dumps(payload.get("config") or {}),
        "context": json.dumps(payload.get("context") or {}),
        "metadata": json.dumps(payload.get("metadata") or {}),
        "created_at": _now(),
        "updated_at": _now(),
    }
    await runtime.put_assistant(record)
    return _assistant_json(record)


@router.get("/assistants/{assistant_id}")
async def get_assistant(assistant_id: str, request: Request) -> dict[str, Any]:
    runtime = _runtime(request)
    record = await _find_assistant(runtime, assistant_id)
    if record is None:
        raise HTTPException(404, f"assistant 不存在: {assistant_id}")
    return record


async def _find_assistant(runtime: ServerRuntime, assistant_id: str) -> dict[str, Any] | None:
    record = await runtime.get_assistant(assistant_id)
    if record is None:
        record = await runtime.get_assistant_by_name(assistant_id)
    return record


def _assistant_json(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "assistant_id": record["assistant_id"],
        "graph_id": record["graph_id"],
        "name": record.get("name") or record["assistant_id"],
        "version": int(record.get("version") or 1),
        "config": json.loads(record.get("config") or "{}"),
        "context": json.loads(record.get("context") or "{}"),
        "metadata": json.loads(record.get("metadata") or "{}"),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
    }


@router.patch("/assistants/{assistant_id}")
async def update_assistant(assistant_id: str, request: Request) -> dict[str, Any]:
    runtime = _runtime(request)
    record = await _find_assistant(runtime, assistant_id)
    if record is None:
        raise HTTPException(404, f"assistant 不存在: {assistant_id}")
    payload = await json_body(request)
    version = int(record.get("version") or 1) + 1
    updated = dict(record)
    updated["version"] = str(version)
    if payload.get("graph_id"):
        updated["graph_id"] = str(payload["graph_id"])
    if payload.get("name"):
        updated["name"] = str(payload["name"])
    if payload.get("config") is not None:
        updated["config"] = json.dumps(payload["config"])
    if payload.get("context") is not None:
        updated["context"] = json.dumps(payload["context"])
    if payload.get("metadata") is not None:
        updated["metadata"] = json.dumps(payload["metadata"])
    updated["updated_at"] = _now()
    await runtime.put_assistant(updated)
    return _assistant_json(updated)


@router.delete("/assistants/{assistant_id}")
async def delete_assistant(assistant_id: str, request: Request) -> None:
    runtime = _runtime(request)
    record = await _find_assistant(runtime, assistant_id)
    if record is None:
        raise HTTPException(404, f"assistant 不存在: {assistant_id}")
    await runtime.redis.delete(f"slg:assistant:{record['assistant_id']}")
    await runtime.redis.delete(f"slg:assistant-name:{record['name']}")


@router.post("/assistants/search")
async def search_assistants(request: Request) -> list[dict[str, Any]]:
    runtime = _runtime(request)
    payload = await json_body(request)
    limit = int(payload.get("limit", 10))
    offset = int(payload.get("offset", 0))
    found = await _collect_assistants(runtime, payload)
    found.sort(key=lambda r: r["created_at"], reverse=True)
    return [_assistant_json(r) for r in found[offset : offset + limit]]


async def _collect_assistants(
    runtime: ServerRuntime, payload: dict[str, Any]
) -> list[dict[str, Any]]:
    pattern = "slg:assistant:*"
    wanted_metadata = cast("dict[str, Any]", payload.get("metadata") or {})
    wanted_graph = payload.get("graph_id")
    wanted_name = payload.get("name")
    found: list[dict[str, Any]] = []
    seen: set[str] = set()
    async for raw_key in runtime.redis.scan_iter(match=pattern):  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        key = _decode(raw_key)
        if key.startswith("slg:assistant-name:"):
            continue
        assistant_id = key.rsplit(":", 1)[-1]
        if assistant_id in seen:
            continue
        seen.add(assistant_id)
        record = await runtime.get_assistant(assistant_id)
        if record is None:
            continue
        if wanted_graph and record.get("graph_id") != wanted_graph:
            continue
        if wanted_name and record.get("name") != wanted_name:
            continue
        metadata = json.loads(record.get("metadata") or "{}")
        if wanted_metadata and any(metadata.get(k) != v for k, v in wanted_metadata.items()):
            continue
        found.append(record)
    return found


@router.post("/assistants/count")
async def count_assistants(request: Request) -> int:
    runtime = _runtime(request)
    payload = await json_body(request)
    return len(await _collect_assistants(runtime, payload))


@router.post("/assistants/{assistant_id}/versions")
async def get_assistant_versions(assistant_id: str, request: Request) -> list[dict[str, Any]]:
    runtime = _runtime(request)
    record = await _find_assistant(runtime, assistant_id)
    if record is None:
        raise HTTPException(404, f"assistant 不存在: {assistant_id}")
    latest = int(record.get("version") or 1)
    limit = int((await json_body(request)).get("limit", 10))
    versions: list[dict[str, Any]] = []
    for version in range(latest, max(latest - limit, 0), -1):
        history = dict(record)
        history["version"] = str(version)
        versions.append(
            {
                "assistant_id": record["assistant_id"],
                "version": version,
                "created_at": record.get("created_at"),
            }
        )
    return versions


@router.post("/assistants/{assistant_id}/latest")
async def set_latest_version(assistant_id: str, request: Request) -> dict[str, Any]:
    runtime = _runtime(request)
    payload = await json_body(request)
    record = await _find_assistant(runtime, assistant_id)
    if record is None:
        raise HTTPException(404, f"assistant 不存在: {assistant_id}")
    record["version"] = str(int(payload.get("version", 1)))
    await runtime.put_assistant(record)
    return _assistant_json(record)


@router.get("/assistants/{assistant_id}/graph")
async def get_assistant_graph(
    assistant_id: str, request: Request, xray: bool | int = False
) -> dict[str, Any]:
    runtime = _runtime(request)
    record = await _find_assistant(runtime, assistant_id)
    if record is None:
        raise HTTPException(404, f"assistant 不存在: {assistant_id}")
    graph = runtime.registry.compile_local(str(record["graph_id"]), checkpointer=runtime.saver)
    return graph.get_graph(xray=bool(xray)).to_json()


@router.get("/assistants/{assistant_id}/schemas")
async def get_assistant_schemas(assistant_id: str, request: Request) -> dict[str, Any]:
    runtime = _runtime(request)
    record = await _find_assistant(runtime, assistant_id)
    if record is None:
        raise HTTPException(404, f"assistant 不存在: {assistant_id}")
    graph = runtime.registry.compile_local(str(record["graph_id"]), checkpointer=runtime.saver)
    return {
        "graph_id": record["graph_id"],
        "input_schema": _safe_schema(graph.get_input_jsonschema),
        "output_schema": _safe_schema(graph.get_output_jsonschema),
        "state_schema": _safe_schema(graph.get_input_jsonschema),
        "config_schema": _safe_schema(lambda: cast("dict[str, Any]", {})),
        "context_schema": _safe_schema(lambda: cast("dict[str, Any]", {})),
    }


def _safe_schema(getter: Any) -> dict[str, Any] | None:
    try:
        schema = getter()
        return json.loads(json.dumps(schema))
    except Exception:
        return None


@router.get("/assistants/{assistant_id}/subgraphs")
async def get_assistant_subgraphs(assistant_id: str, request: Request) -> dict[str, Any]:
    runtime = _runtime(request)
    record = await _find_assistant(runtime, assistant_id)
    if record is None:
        raise HTTPException(404, f"assistant 不存在: {assistant_id}")
    graph = runtime.registry.compile_local(str(record["graph_id"]), checkpointer=runtime.saver)
    subgraphs = graph.get_subgraphs()
    return {
        namespace: {"graph_id": record["graph_id"], "input_schema": None, "output_schema": None}
        for namespace, _graph in subgraphs
    }


@router.get("/assistants/{assistant_id}/subgraphs/{namespace}")
async def get_assistant_subgraph(
    assistant_id: str, namespace: str, request: Request, recurse: bool = False
) -> dict[str, Any]:
    runtime = _runtime(request)
    record = await _find_assistant(runtime, assistant_id)
    if record is None:
        raise HTTPException(404, f"assistant 不存在: {assistant_id}")
    graph = runtime.registry.compile_local(str(record["graph_id"]), checkpointer=runtime.saver)
    subgraphs = dict(graph.get_subgraphs())
    if namespace not in subgraphs:
        raise HTTPException(404, f"subgraph 不存在: {namespace}")
    return {
        namespace: {
            "graph_id": record["graph_id"],
            "input_schema": None,
            "output_schema": None,
        }
    }


# ---- Store ----


@router.put("/store/items")
async def put_store_item(request: Request) -> None:
    runtime = _runtime(request)
    payload = await json_body(request)
    await runtime.kv.aput(
        tuple(payload["namespace"]),
        str(payload["key"]),
        payload["value"],
        index=payload.get("index"),
    )


@router.get("/store/items")
async def get_store_item(request: Request) -> dict[str, Any]:
    runtime = _runtime(request)
    namespace = request.query_params.get("namespace", "")
    key = request.query_params.get("key", "")
    item = await runtime.kv.aget(_parse_namespace(namespace), key)
    if item is None:
        raise HTTPException(404, f"store item 不存在: {namespace}/{key}")
    return {
        "value": item.value,
        "namespace": list(item.namespace),
        "key": item.key,
        "created_at": item.created_at.isoformat(),
        "updated_at": item.updated_at.isoformat(),
    }


@router.delete("/store/items")
async def delete_store_item(request: Request) -> None:
    runtime = _runtime(request)
    payload = cast("dict[str, Any]", await json_body(request))
    namespace = cast("list[str]", payload.get("namespace") or [])
    key = str(payload.get("key") or "")
    await runtime.kv.adelete(tuple(namespace), key)


@router.post("/store/items/search")
async def search_store_items(request: Request) -> dict[str, Any]:
    runtime = _runtime(request)
    payload = await json_body(request)
    items = await runtime.kv.asearch(
        tuple(payload.get("namespace_prefix") or []),
        filter=payload.get("filter"),
        limit=int(payload.get("limit", 10)),
        offset=int(payload.get("offset", 0)),
        query=payload.get("query"),
    )
    return {
        "items": [
            {
                "value": item.value,
                "namespace": list(item.namespace),
                "key": item.key,
                "created_at": item.created_at.isoformat(),
                "updated_at": item.updated_at.isoformat(),
            }
            for item in items
        ]
    }


@router.post("/store/namespaces")
async def list_store_namespaces(request: Request) -> list[list[str]]:
    runtime = _runtime(request)
    payload = await json_body(request)
    namespaces = await runtime.kv.alist_namespaces(
        prefix=payload.get("prefix"),
        suffix=payload.get("suffix"),
        limit=int(payload.get("limit", 100)),
        offset=int(payload.get("offset", 0)),
    )
    return [list(ns) for ns in namespaces]


def _parse_namespace(text: str) -> tuple[str, ...]:
    decoded = urllib.parse.unquote(text)
    return tuple(part for part in decoded.split(".") if part) if decoded else ()


# ---- Crons ----


@router.post("/runs/crons")
async def create_cron(request: Request) -> dict[str, Any]:
    runtime = _runtime(request)
    payload = await json_body(request)
    assistant_id = str(payload["assistant_id"])
    assistant = await _find_assistant(runtime, assistant_id)
    if assistant is None:
        raise HTTPException(404, f"assistant 不存在: {assistant_id}")
    schedule = str(payload["schedule"])
    record = CronRecord(
        cron_id=new_cron_id(),
        assistant_id=assistant["assistant_id"],
        graph_id=str(assistant["graph_id"]),
        thread_id=payload.get("thread_id"),
        schedule=schedule,
        input=payload.get("input") or {},
        metadata=payload.get("metadata") or {},
        next_run_at=next_after(schedule, utc_now()).isoformat(),
    )
    await runtime.crons.create(record)
    return _cron_json(record)


@router.post("/threads/{thread_id}/runs/crons")
async def create_thread_cron(thread_id: str, request: Request) -> dict[str, Any]:
    payload = await json_body(request)
    payload["thread_id"] = thread_id
    runtime = _runtime(request)
    assistant_id = str(payload["assistant_id"])
    assistant = await _find_assistant(runtime, assistant_id)
    if assistant is None:
        raise HTTPException(404, f"assistant 不存在: {assistant_id}")
    schedule = str(payload["schedule"])
    record = CronRecord(
        cron_id=new_cron_id(),
        assistant_id=assistant["assistant_id"],
        graph_id=str(assistant["graph_id"]),
        thread_id=thread_id,
        schedule=schedule,
        input=payload.get("input") or {},
        metadata=payload.get("metadata") or {},
        next_run_at=next_after(schedule, utc_now()).isoformat(),
    )
    await runtime.crons.create(record)
    return _cron_json(record)


@router.delete("/runs/crons/{cron_id}")
async def delete_cron(cron_id: str, request: Request) -> None:
    runtime = _runtime(request)
    if not await runtime.crons.delete(cron_id):
        raise HTTPException(404, f"cron 不存在: {cron_id}")


@router.patch("/runs/crons/{cron_id}")
async def update_cron(cron_id: str, request: Request) -> dict[str, Any]:
    runtime = _runtime(request)
    payload = await json_body(request)
    fields: dict[str, Any] = {}
    if payload.get("schedule"):
        fields["schedule"] = str(payload["schedule"])
        fields["next_run_at"] = next_after(str(payload["schedule"]), utc_now()).isoformat()
    if payload.get("input") is not None:
        fields["input"] = payload["input"]
    if payload.get("metadata") is not None:
        fields["metadata"] = payload["metadata"]
    if payload.get("enabled") is not None:
        fields["enabled"] = bool(payload["enabled"])
    record = await runtime.crons.update(cron_id, **fields)
    if record is None:
        raise HTTPException(404, f"cron 不存在: {cron_id}")
    return _cron_json(record)


@router.post("/runs/crons/search")
async def search_crons(request: Request) -> list[dict[str, Any]]:
    runtime = _runtime(request)
    payload = await json_body(request)
    records = await runtime.crons.all()
    wanted = payload.get("assistant_id")
    thread_id = payload.get("thread_id")
    found = [
        record
        for record in records
        if (wanted is None or record.assistant_id == wanted)
        and (thread_id is None or record.thread_id == thread_id)
    ]
    limit = int(payload.get("limit", 10))
    offset = int(payload.get("offset", 0))
    return [_cron_json(record) for record in found[offset : offset + limit]]


@router.post("/runs/crons/count")
async def count_crons(request: Request) -> int:
    runtime = _runtime(request)
    payload = await json_body(request)
    records = await runtime.crons.all()
    wanted = payload.get("assistant_id")
    if wanted is None:
        return len(records)
    return len([r for r in records if r.assistant_id == wanted])


def _cron_json(record: CronRecord) -> dict[str, Any]:
    return {
        "cron_id": record.cron_id,
        "assistant_id": record.assistant_id,
        "thread_id": record.thread_id,
        "schedule": record.schedule,
        "input": record.input,
        "metadata": record.metadata,
        "enabled": record.enabled,
        "created_at": _now(),
        "updated_at": _now(),
        "next_run_date": record.next_run_at,
    }
