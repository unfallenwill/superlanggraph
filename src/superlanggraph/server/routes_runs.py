"""runs 路由：run 的创建、流式、等待、加入、取消与查询。"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from typing import Any, cast

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from superlanggraph.core.envelope import Envelope
from superlanggraph.core.serde import dumps_json_text
from superlanggraph.server.runtime import RunRejectedError, ServerRuntime
from superlanggraph.server.sse import tail_events

router = APIRouter()

_TERMINAL = ("success", "error", "timeout", "interrupted")


def _runtime(request: Request) -> ServerRuntime:
    return request.app.state.runtime


def _ephemeral_thread_id() -> str:
    return f"ephemeral-{uuid.uuid4().hex}"


async def _stream_response(
    request: Request,
    runtime: ServerRuntime,
    run_id: str,
    stream_mode: Any,
    metadata_event: dict[str, Any],
    on_disconnect: str | None = None,
    thread_id: str | None = None,
) -> StreamingResponse:
    modes = (
        {stream_mode}
        if isinstance(stream_mode, str)
        else set(stream_mode)
        if stream_mode
        else {"values"}
    )
    last_event_id = request.headers.get("last-event-id")

    background_tasks: set[asyncio.Task[None]] = set()

    async def generator() -> AsyncIterator[str]:
        try:
            async for frame in tail_events(
                runtime.events,
                run_id,
                requested_modes=modes,
                last_event_id=last_event_id,
                metadata_event=metadata_event,
            ):
                yield frame
        finally:
            # on_disconnect=cancel：客户端断开（生成器被关闭）即发取消指令；
            # GeneratorExit 场景不能 await，用独立 task 投递，dispatcher 幂等。
            if on_disconnect == "cancel":
                task = asyncio.ensure_future(
                    runtime.control.publish_control(
                        _cancel_envelope(run_id, thread_id or "", "interrupt")
                    )
                )
                background_tasks.add(task)
                task.add_done_callback(background_tasks.discard)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-store"},
    )


async def _await_terminal(
    runtime: ServerRuntime, run_id: str, timeout: float = 120.0
) -> dict[str, Any]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        record = await runtime.runs.get(run_id)
        if record is None:
            raise HTTPException(404, f"run 不存在: {run_id}")
        if record["status"] in _TERMINAL:
            return record
        await asyncio.sleep(0.05)
    raise HTTPException(504, f"run 超时未完成: {run_id}")


async def _start_stream_run(
    request: Request,
    thread_id: str | None,
    payload: dict[str, Any],
) -> StreamingResponse:
    runtime = _runtime(request)
    thread_id = thread_id or _ephemeral_thread_id()
    await runtime.ensure_thread(thread_id)
    try:
        record = await runtime.create_run(
            thread_id=thread_id,
            assistant_id=str(payload.get("assistant_id") or ""),
            input=payload.get("input"),
            command=payload.get("command"),
            metadata=payload.get("metadata"),
            multitask_strategy=str(payload.get("multitask_strategy") or "reject"),
            extra_configurable=_extra_config(payload),
            interrupt_before=_as_optional_nodes(payload.get("interrupt_before")),
            interrupt_after=_as_optional_nodes(payload.get("interrupt_after")),
        )
    except RunRejectedError as exc:
        raise HTTPException(409, str(exc)) from exc
    return await _stream_response(
        request,
        runtime,
        str(record["run_id"]),
        payload.get("stream_mode"),
        {"run_id": record["run_id"], "thread_id": thread_id},
        on_disconnect=str(payload.get("on_disconnect") or "continue"),
        thread_id=thread_id,
    )


def _as_str_or_none(value: Any) -> str | None:
    return str(value) if value is not None else None


def _as_optional_nodes(value: Any) -> tuple[str, ...] | None:
    """节点名列表 / 单个名字 / "*" 的归一化。"""
    if value is None:
        return None
    if isinstance(value, str):
        return (value,)
    return tuple(str(item) for item in value)


def _extra_config(payload: dict[str, Any]) -> dict[str, Any]:
    extra: dict[str, Any] = {}
    if payload.get("config"):
        extra.update(payload["config"].get("configurable") or {})
    return extra


@router.post("/threads/{thread_id}/runs/stream")
async def stream_thread_run(thread_id: str, request: Request) -> StreamingResponse:
    """创建 run 并以 SSE 返回其事件流。"""
    payload = await request.json()
    return await _start_stream_run(request, thread_id, payload)


@router.post("/runs/stream")
async def stream_run(request: Request) -> StreamingResponse:
    """threadless 版本的流式 run。"""
    payload = await request.json()
    return await _start_stream_run(request, None, payload)


@router.post("/threads/{thread_id}/runs")
async def create_thread_run(thread_id: str, request: Request) -> dict[str, Any]:
    """创建后台 run，立即返回 run 记录。"""
    runtime = _runtime(request)
    payload = await request.json()
    await runtime.ensure_thread(thread_id)
    try:
        record = await runtime.create_run(
            thread_id=thread_id,
            assistant_id=str(payload.get("assistant_id") or ""),
            input=payload.get("input"),
            command=payload.get("command"),
            metadata=payload.get("metadata"),
            multitask_strategy=str(payload.get("multitask_strategy") or "reject"),
            extra_configurable=_extra_config(payload),
            interrupt_before=_as_optional_nodes(payload.get("interrupt_before")),
            interrupt_after=_as_optional_nodes(payload.get("interrupt_after")),
        )
    except RunRejectedError as exc:
        raise HTTPException(409, str(exc)) from exc
    return record


@router.post("/runs/batch")
async def create_run_batch(request: Request) -> list[dict[str, Any]]:
    """批量创建后台 run。"""
    runtime = _runtime(request)
    payloads = await request.json()
    records: list[dict[str, Any]] = []
    for payload in payloads:
        thread_id = str(payload["thread_id"])
        await runtime.ensure_thread(thread_id)
        record = await runtime.create_run(
            thread_id=thread_id,
            assistant_id=str(payload.get("assistant_id") or ""),
            input=payload.get("input"),
            command=payload.get("command"),
            metadata=payload.get("metadata"),
            multitask_strategy=str(payload.get("multitask_strategy") or "enqueue"),
        )
        records.append(record)
    return records


@router.post("/threads/{thread_id}/runs/wait")
async def wait_thread_run(thread_id: str, request: Request) -> Any:
    """创建 run 并阻塞到终态，返回线程最终 state values。"""
    runtime = _runtime(request)
    payload = await request.json()
    await runtime.ensure_thread(thread_id)
    try:
        record = await runtime.create_run(
            thread_id=thread_id,
            assistant_id=str(payload.get("assistant_id") or ""),
            input=payload.get("input"),
            command=payload.get("command"),
            metadata=payload.get("metadata"),
            multitask_strategy=str(payload.get("multitask_strategy") or "enqueue"),
            extra_configurable=_extra_config(payload),
            interrupt_before=_as_optional_nodes(payload.get("interrupt_before")),
            interrupt_after=_as_optional_nodes(payload.get("interrupt_after")),
            durability=_as_str_or_none(payload.get("durability")),
        )
    except RunRejectedError as exc:
        raise HTTPException(409, str(exc)) from exc
    final = await _await_terminal(runtime, str(record["run_id"]))
    if final["status"] == "error":
        raise HTTPException(500, final.get("error") or "run 执行失败")
    return await _thread_values(runtime, thread_id)


@router.post("/runs/wait")
async def wait_run(request: Request) -> Any:
    """threadless 版本的同步 run。"""
    payload = await request.json()
    thread_id = _ephemeral_thread_id()
    runtime = _runtime(request)
    await runtime.ensure_thread(thread_id)
    record = await runtime.create_run(
        thread_id=thread_id,
        assistant_id=str(payload.get("assistant_id") or ""),
        input=payload.get("input"),
        command=payload.get("command"),
        metadata=payload.get("metadata"),
        multitask_strategy="enqueue",
    )
    await _await_terminal(runtime, str(record["run_id"]))
    return await _thread_values(runtime, thread_id)


async def _thread_values(runtime: ServerRuntime, thread_id: str) -> Any:
    graph_id = await _graph_id_for_thread(runtime, thread_id)
    graph = runtime.registry.compile_local(graph_id, checkpointer=runtime.saver)
    snapshot = await graph.aget_state({"configurable": {"thread_id": thread_id}})
    return json.loads(dumps_json_text(snapshot.values))


async def _graph_id_for_thread(runtime: ServerRuntime, thread_id: str) -> str:
    recorded = await runtime.redis.get(f"slg:thread-graph:{thread_id}")
    if recorded is None:
        return runtime.registry.default_graph_id()
    return _decode(recorded)


def _decode(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


@router.get("/threads/{thread_id}/runs")
async def list_runs(
    thread_id: str,
    request: Request,
    limit: int = 10,
    offset: int = 0,
    status: str | None = None,
    select: str | None = None,
) -> list[dict[str, Any]]:
    records = await _runtime(request).runs.list_for_thread(
        thread_id, limit=limit, offset=offset, status=status
    )
    if select:
        wanted = {field.strip() for field in select.split(",")}
        records = [
            {key: value for key, value in record.items() if key in wanted} for record in records
        ]
    return records


@router.get("/threads/{thread_id}/runs/{run_id}")
async def get_run(thread_id: str, run_id: str, request: Request) -> dict[str, Any]:
    record = await _runtime(request).runs.get(run_id)
    if record is None or record["thread_id"] != thread_id:
        raise HTTPException(404, f"run 不存在: {run_id}")
    return record


@router.post("/threads/{thread_id}/runs/{run_id}/cancel")
async def cancel_run(
    thread_id: str, run_id: str, request: Request, wait: bool = False, action: str = "interrupt"
) -> dict[str, Any]:
    runtime = _runtime(request)
    record = await runtime.runs.get(run_id)
    if record is None:
        raise HTTPException(404, f"run 不存在: {run_id}")
    await runtime.control.publish_control(_cancel_envelope(run_id, thread_id, action))
    if wait:
        return await _await_terminal(runtime, run_id)
    return await runtime.runs.get(run_id) or record


@router.post("/runs/cancel")
async def cancel_many_runs(request: Request) -> list[dict[str, Any]]:
    """按条件批量取消（thread_id / run_ids / status 的交集）。"""
    runtime = _runtime(request)
    payload = await request.json()
    thread_id = payload.get("thread_id")
    run_ids = cast("list[Any]", payload.get("run_ids") or [])
    status = payload.get("status")
    if thread_id:
        candidates = await runtime.runs.list_for_thread(str(thread_id), limit=1000, status=status)
    else:
        candidates: list[dict[str, Any]] = []
        for run_id in run_ids:
            record = await runtime.runs.get(str(run_id))
            if record is not None and (status is None or record["status"] == status):
                candidates.append(record)
    cancelled: list[dict[str, Any]] = []
    for record in candidates:
        if record["status"] in ("pending", "running"):
            await runtime.control.publish_control(
                _cancel_envelope(str(record["run_id"]), str(record["thread_id"]), "interrupt")
            )
            cancelled.append(record)
    return cancelled


def _cancel_envelope(run_id: str, thread_id: str, action: str) -> Envelope:
    return Envelope(
        type="control",
        run_id=run_id,
        payload={
            "kind": "cancel",
            "run_id": run_id,
            "thread_id": thread_id,
            "action": action,
        },
    )


@router.get("/threads/{thread_id}/runs/{run_id}/join")
async def join_run(thread_id: str, run_id: str, request: Request) -> Any:
    runtime = _runtime(request)
    await _await_terminal(runtime, run_id)
    return await _thread_values(runtime, thread_id)


@router.post("/threads/{thread_id}/runs/{run_id}/stream")
async def join_stream(thread_id: str, run_id: str, request: Request) -> StreamingResponse:
    """加入一个已存在 run 的事件流（可断点续读）。"""
    runtime = _runtime(request)
    payload = await request.json()
    record = await runtime.runs.get(run_id)
    if record is None:
        raise HTTPException(404, f"run 不存在: {run_id}")
    return await _stream_response(
        request,
        runtime,
        run_id,
        payload.get("stream_mode"),
        {"run_id": run_id, "thread_id": thread_id},
    )


@router.delete("/threads/{thread_id}/runs/{run_id}")
async def delete_run(thread_id: str, run_id: str, request: Request) -> None:
    runtime = _runtime(request)
    record = await runtime.runs.delete(run_id)
    if record is None:
        raise HTTPException(404, f"run 不存在: {run_id}")
