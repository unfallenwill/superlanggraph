"""监控 API 路由：REST 快照/序列/节点/run/worker/事件端点与 SSE 实时流。

store 一律经 ``request.app.state.monitor`` 获取（组合根注入），本模块不 import
server 包，保持基础设施层与接入层的既有分层方向（import-linter 约束）。
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any, cast

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from superlanggraph.monitoring.store import (
    MetricStore,
    MinutePoint,
    MonitorSnapshot,
    SequencedEvent,
    WorkerInfo,
)

router = APIRouter(prefix="/monitor", tags=["monitoring"])

HEARTBEAT_SECONDS = 15.0  # SSE 心跳间隔；模块级常量便于测试注入缩短
_QUEUE_SIZE = 256  # SSE 泵队列上限；满则丢弃并置脏，由心跳时 snapshot 帧重同步兜底
_SPANS_LIMIT = 200  # with_spans=true 时每图最近原始样本条数


def _store(request: Request) -> MetricStore:
    return cast("MetricStore", request.app.state.monitor)


def _sse_frame(event: str, data_text: str, event_id: str | None = None) -> str:
    """构造一帧 SSE 文本。格式与 server/sse.py 的 sse_frame 相同，
    因分层禁反向 import（monitoring 不得依赖 server）而本地复制。"""
    prefix = f"id: {event_id}\n" if event_id else ""
    return f"{prefix}event: {event}\ndata: {data_text}\n\n"


def _parse_last_id(raw: str | None) -> int | None:
    """Last-Event-ID 头 / last_event_id 查询参数 → seq；空或非法视为无续读位点。"""
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


@router.get("/overview")
async def overview(request: Request, window_minutes: int = 60) -> MonitorSnapshot:
    """窗口内总体快照：状态分布、队列、令牌、错误率与节点 top。"""
    return await _store(request).overview(window_minutes=window_minutes)


@router.get("/series")
async def series(request: Request, minutes: int = 60) -> dict[str, list[MinutePoint]]:
    """逐分钟时间序列，缺口补零、按 bucket_ts 升序。"""
    return {"points": await _store(request).minute_series(minutes=minutes)}


@router.get("/nodes")
async def nodes(
    request: Request,
    graph_id: str | None = None,
    minutes: int = 60,
    limit: int = 50,
    with_spans: bool = False,
) -> dict[str, Any]:
    """节点聚合统计；with_spans 时附带每图最近原始样本（近端分位计算数据源）。"""
    store = _store(request)
    result: dict[str, Any] = {
        "stats": await store.node_stats(graph_id=graph_id, minutes=minutes, limit=limit),
    }
    if with_spans:
        result["spans"] = await store.recent_spans(graph_id=graph_id, limit=_SPANS_LIMIT)
    return result


@router.get("/runs/{run_id}")
async def run_detail(request: Request, run_id: str) -> dict[str, Any]:
    """单 run 汇总与节点时间线；无监控记录返回 404。"""
    summary = await _store(request).run_summary(run_id)
    if summary is None:
        raise HTTPException(404, f"run 无监控记录: {run_id}")
    return summary


@router.get("/workers")
async def workers(request: Request) -> list[WorkerInfo]:
    """worker 列表，取自最近探针快照；探针未运行时为空列表。"""
    snapshot = await _store(request).overview()
    return list(snapshot.queue.workers) if snapshot.queue is not None else []


@router.get("/events/recent")
async def recent_events(
    request: Request,
    limit: int = 200,
    after_seq: int | None = None,
) -> dict[str, list[SequencedEvent]]:
    """事件日志读取；after_seq 给定时仅返回 seq 更大者，升序。"""
    events = await _store(request).recent_events(limit=limit, after_seq=after_seq)
    return {"events": events}


@router.get("/stream")
async def stream(request: Request) -> StreamingResponse:
    """SSE 实时流：首帧 snapshot 全量，实时帧按事件类型推送，15s 心跳。

    断点续读：``Last-Event-ID`` 头（或 ``last_event_id`` 查询参数）给出已见
    最大 seq，先回放其后历史事件再进实时订阅；回放与订阅间窄窗口可能重复，
    客户端按 seq 幂等去重（至少一次语义，与 broker 一致）。
    """
    store = _store(request)
    after_seq = _parse_last_id(
        request.headers.get("last-event-id") or request.query_params.get("last_event_id")
    )

    async def generator() -> AsyncIterator[str]:
        yield _sse_frame("snapshot", (await store.overview()).model_dump_json())
        queue: asyncio.Queue[SequencedEvent] = asyncio.Queue(maxsize=_QUEUE_SIZE)
        dirty = False  # 背压丢弃标记；置位后下一次心跳以 snapshot 帧全量重同步

        def forward(event: SequencedEvent) -> None:
            nonlocal dirty
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                dirty = True

        async def pump() -> None:
            # 先回放历史（Last-Event-ID 之后），再进实时订阅
            if after_seq is not None:
                for event in await store.recent_events(after_seq=after_seq):
                    forward(event)
            async for event in store.subscribe():
                forward(event)

        task = asyncio.create_task(pump())
        try:
            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=HEARTBEAT_SECONDS)
                except TimeoutError:
                    if dirty:
                        dirty = False
                        snapshot = await store.overview()
                        yield _sse_frame("snapshot", snapshot.model_dump_json())
                    yield ": ping\n\n"
                    continue
                yield _sse_frame(
                    item.event.type, item.event.model_dump_json(), event_id=str(item.seq)
                )
        finally:
            task.cancel()

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
    )
