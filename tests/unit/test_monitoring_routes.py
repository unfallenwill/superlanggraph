"""监控路由测试：REST JSON 投影形状、404 分支与 SSE 帧格式（§10）。

用 InMemoryMetricStore + httpx ASGITransport 直测 router，不经 server 组合根。
"""

import asyncio
import contextlib
import json
import time
from collections.abc import AsyncIterator, Callable
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from starlette.requests import Request

from superlanggraph.monitoring import routes
from superlanggraph.monitoring.events import (
    RUN_STATUS,
    NodeSpanEvent,
    RunLifecycleEvent,
)
from superlanggraph.monitoring.routes import (
    HEARTBEAT_SECONDS,
    _sse_frame,  # pyright: ignore[reportPrivateUsage]
    router,
)
from superlanggraph.monitoring.store import InMemoryMetricStore, QueueSnapshot, WorkerInfo


def _span(
    ts: float,
    *,
    run_id: str = "r1",
    graph_id: str = "g1",
    node: str = "n1",
    duration_ms: float = 100.0,
) -> NodeSpanEvent:
    return NodeSpanEvent(
        event_id=f"span-{ts}-{node}-{duration_ms}",
        ts=ts,
        source="worker",
        run_id=run_id,
        thread_id="th",
        graph_id=graph_id,
        task_id=f"task-{node}",
        node=node,
        queue_ms=1.0,
        duration_ms=duration_ms,
        outcome="return",
        error=None,
    )


def _lifecycle(run_id: str, status: RUN_STATUS, ts: float) -> RunLifecycleEvent:
    return RunLifecycleEvent(
        event_id=f"ev-{run_id}-{status}",
        ts=ts,
        source="driver",
        run_id=run_id,
        thread_id="th",
        graph_id="g1",
        status=status,
    )


@pytest.fixture
def store() -> InMemoryMetricStore:
    return InMemoryMetricStore()


@pytest.fixture
async def client(store: InMemoryMetricStore) -> AsyncIterator[httpx.AsyncClient]:
    app = FastAPI()
    app.include_router(router)
    app.state.monitor = store
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as c:
        yield c


# ---- REST 端点 ----


async def test_overview_json_shape(
    client: httpx.AsyncClient, store: InMemoryMetricStore
) -> None:
    await store.record_event(_lifecycle("r1", "running", time.time() - 5))
    await store.record_event(_span(time.time() - 5, duration_ms=200.0))
    await store.record_event(_lifecycle("r2", "error", time.time() - 4))
    response = await client.get("/monitor/overview", params={"window_minutes": 30})
    assert response.status_code == 200
    body = response.json()
    assert set(body) == {
        "generated_at",
        "window_minutes",
        "status_counts",
        "queue",
        "token_totals",
        "error_summary",
        "node_top",
    }
    assert body["window_minutes"] == 30
    assert set(body["status_counts"]) == {
        "pending", "running", "error", "success", "timeout", "interrupted",
    }
    assert body["status_counts"]["running"] == 1
    assert body["status_counts"]["error"] == 1
    assert body["queue"] is None
    assert body["token_totals"] == {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    assert body["error_summary"]["errors"] == 1
    assert body["error_summary"]["runs_finished"] == 1
    assert body["error_summary"]["rate"] == pytest.approx(1.0)
    assert body["node_top"][0]["node"] == "n1"


async def test_series_json_shape(
    client: httpx.AsyncClient, store: InMemoryMetricStore
) -> None:
    await store.record_event(_lifecycle("r1", "running", time.time() - 5))
    response = await client.get("/monitor/series", params={"minutes": 3})
    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"points"}
    points = body["points"]
    assert len(points) == 3
    bucket_ts_list = [p["bucket_ts"] for p in points]
    assert bucket_ts_list == sorted(bucket_ts_list)
    assert sum(p["runs_started"] for p in points) == 1


async def test_nodes_stats_spans_and_filter(
    client: httpx.AsyncClient, store: InMemoryMetricStore
) -> None:
    base = time.time() - 5
    await store.record_event(_span(base, node="a", duration_ms=100.0))
    await store.record_event(_span(base + 1, node="a", duration_ms=300.0))
    await store.record_event(_span(base + 2, graph_id="g2", node="c", duration_ms=500.0))

    plain = (await client.get("/monitor/nodes")).json()
    assert set(plain) == {"stats"}
    assert [(s["graph_id"], s["node"]) for s in plain["stats"]] == [
        ("g2", "c"), ("g1", "a"),
    ]
    node_a = plain["stats"][1]
    assert node_a["count"] == 2
    assert node_a["total_ms"] == pytest.approx(400.0)
    assert node_a["avg_ms"] == pytest.approx(200.0)
    assert node_a["max_ms"] == pytest.approx(300.0)

    filtered = (
        await client.get("/monitor/nodes", params={"graph_id": "g2"})
    ).json()
    assert [s["node"] for s in filtered["stats"]] == ["c"]

    with_spans = (
        await client.get("/monitor/nodes", params={"with_spans": "true"})
    ).json()
    assert set(with_spans) == {"stats", "spans"}
    assert len(with_spans["spans"]) == 3
    durations = [s["duration_ms"] for s in with_spans["spans"]]
    assert durations == sorted(durations, reverse=True)  # recent_spans 降序


async def test_run_detail_and_404(
    client: httpx.AsyncClient, store: InMemoryMetricStore
) -> None:
    ts = time.time() - 5
    await store.record_event(_lifecycle("r1", "running", ts))
    await store.record_event(_span(ts + 1, run_id="r1", node="a", duration_ms=10.0))
    await store.record_event(_span(ts + 2, run_id="r1", node="b", duration_ms=20.0))
    response = await client.get("/monitor/runs/r1")
    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"summary", "spans"}
    assert body["summary"]["run_id"] == "r1"
    assert body["summary"]["status"] == "running"
    assert [span["node"] for span in body["spans"]] == ["a", "b"]  # 升序
    missing = await client.get("/monitor/runs/no-such")
    assert missing.status_code == 404


async def test_workers_endpoint_from_probe(
    client: httpx.AsyncClient, store: InMemoryMetricStore
) -> None:
    empty = await client.get("/monitor/workers")
    assert empty.status_code == 200
    assert empty.json() == []
    await store.record_probe(
        QueueSnapshot(
            ts=time.time(),
            tasks_length=0,
            ctrl_length=0,
            pending_tasks=0,
            oldest_pending_ms=0.0,
            active_runs=0,
            workers=[
                WorkerInfo(
                    worker_id="w-1", inflight=1, concurrency=2, last_seen_ts=1.0, alive=True
                )
            ],
        )
    )
    listed = (await client.get("/monitor/workers")).json()
    assert listed == [
        {
            "worker_id": "w-1",
            "inflight": 1,
            "concurrency": 2,
            "last_seen_ts": 1.0,
            "alive": True,
        }
    ]


async def test_recent_events_endpoint_and_after_seq(
    client: httpx.AsyncClient, store: InMemoryMetricStore
) -> None:
    for index in range(3):
        await store.record_event(_lifecycle("r1", "running", time.time() - 5 + index))
    body = (await client.get("/monitor/events/recent")).json()
    assert set(body) == {"events"}
    assert [e["seq"] for e in body["events"]] == [1, 2, 3]
    resumed = (
        await client.get("/monitor/events/recent", params={"after_seq": 2, "limit": 10})
    ).json()
    assert [e["seq"] for e in resumed["events"]] == [3]


# ---- SSE 帧构造 ----


def test_sse_frame_format_with_id() -> None:
    assert _sse_frame("node_span", "{}", "7") == "id: 7\nevent: node_span\ndata: {}\n\n"


def test_sse_frame_format_without_id() -> None:
    assert _sse_frame("snapshot", "{}") == "event: snapshot\ndata: {}\n\n"


# ---- SSE 流：直接消费 StreamingResponse.body_iterator（截断验证） ----
#
# 不经 httpx：ASGITransport 下无限流的请求协程要到响应结束才返回，
# 无法边开边推事件地交互式消费；改为直连 routes.stream 拿 body_iterator。


def _stream_app(store: InMemoryMetricStore) -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    app.state.monitor = store
    return app


def _stream_request(
    app: FastAPI, *, last_event_id: str | None = None, query_string: bytes = b""
) -> Request:
    """构造直连 routes.stream 的 Request；app.state.monitor 已注入 store。"""
    headers: list[tuple[bytes, bytes]] = []
    if last_event_id is not None:
        headers.append((b"last-event-id", last_event_id.encode()))
    scope: dict[str, Any] = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/monitor/stream",
        "raw_path": b"/monitor/stream",
        "query_string": query_string,
        "root_path": "",
        "server": ("testserver", 80),
        "client": ("testclient", 50000),
        "headers": headers,
        "app": app,
    }
    return Request(scope)


async def _until(predicate: Callable[[], bool], timeout: float = 2.0) -> None:
    """轮询等待流中出现目标帧；超时断言失败，避免测试挂死。"""
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("等待 SSE 帧超时")
        await asyncio.sleep(0.01)


def _snapshot_payload(frames: list[str]) -> dict[str, Any] | None:
    for frame in frames:
        if frame.startswith("event: snapshot\n"):
            raw = frame[len("event: snapshot\ndata: "):][: -len("\n\n")]
            payload: dict[str, Any] = json.loads(raw)
            return payload
    return None


def _frame_data(frames: list[str], seq: str) -> dict[str, Any] | None:
    """取 ``id: <seq>`` 帧的 data JSON；帧未到达返回 None。"""
    prefix = f"id: {seq}\n"
    for frame in frames:
        if frame.startswith(prefix):
            _, _, data_part = frame.partition("\ndata: ")
            payload: dict[str, Any] = json.loads(data_part[: -len("\n\n")])
            return payload
    return None


async def _drive(response: StreamingResponse, frames: list[str]) -> asyncio.Task[None]:
    """后台持续拉帧：驱动生成器推进（pump 启动、心跳与实时帧产出）。"""
    iterator: AsyncIterator[str] = response.body_iterator  # type: ignore[assignment]

    async def _consume() -> None:
        try:
            async for frame in iterator:
                frames.append(frame)
        except Exception:  # 关闭响应时读取中断属预期
            pass

    return asyncio.create_task(_consume())


async def _stop_drive(task: asyncio.Task[None], response: StreamingResponse) -> None:
    await asyncio.sleep(0.05)  # 留出最后一帧的到达时间
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await task
    closer: Any = response.body_iterator
    with contextlib.suppress(Exception):
        await closer.aclose()  # 触发生成器 finally，取消 pump 任务


async def test_stream_snapshot_then_realtime_then_heartbeat(
    store: InMemoryMetricStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(routes, "HEARTBEAT_SECONDS", 0.05)
    assert HEARTBEAT_SECONDS == 15.0  # 常量默认值本身即契约
    response = await routes.stream(_stream_request(_stream_app(store)))
    assert response.media_type == "text/event-stream"
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-accel-buffering"] == "no"
    frames: list[str] = []
    driver = await _drive(response, frames)
    try:
        # 首帧：snapshot 全量 JSON（无 id: 前缀）
        await _until(lambda: _snapshot_payload(frames) is not None)
        snapshot = _snapshot_payload(frames)
        assert snapshot is not None
        assert snapshot["status_counts"]["pending"] == 0
        # 心跳注释帧出现 ⇒ pump 已启动并完成订阅注册，此后投递的事件必达
        await _until(lambda: ": ping\n\n" in frames)
        await store.record_event(_span(time.time(), duration_ms=42.0))
        await _until(lambda: _frame_data(frames, "1") is not None)
        payload = _frame_data(frames, "1")
        assert payload is not None
        assert payload["type"] == "node_span"
        assert payload["duration_ms"] == pytest.approx(42.0)
        realtime = next(f for f in frames if f.startswith("id: 1\n"))
        assert realtime.startswith("id: 1\nevent: node_span\ndata: ")
        assert realtime.endswith("\n\n")
    finally:
        await _stop_drive(driver, response)


async def test_stream_replays_after_last_event_id(
    store: InMemoryMetricStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(routes, "HEARTBEAT_SECONDS", 0.05)
    base = time.time() - 5
    for index in range(3):
        await store.record_event(_lifecycle("r1", "running", base + index))
    request = _stream_request(_stream_app(store), last_event_id="1")
    response = await routes.stream(request)
    frames: list[str] = []
    driver = await _drive(response, frames)
    try:
        await _until(lambda: _frame_data(frames, "3") is not None)
    finally:
        await _stop_drive(driver, response)
    assert any(f.startswith("id: 2\n") for f in frames)
    assert any(f.startswith("id: 3\n") for f in frames)
    assert not any(f.startswith("id: 1\n") for f in frames)  # 续放仅含 seq 更大者


async def test_stream_replay_via_query_param(
    store: InMemoryMetricStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(routes, "HEARTBEAT_SECONDS", 0.05)
    base = time.time() - 5
    for index in range(2):
        await store.record_event(_lifecycle("r1", "running", base + index))
    request = _stream_request(_stream_app(store), query_string=b"last_event_id=1")
    response = await routes.stream(request)
    frames: list[str] = []
    driver = await _drive(response, frames)
    try:
        await _until(lambda: _frame_data(frames, "2") is not None)
    finally:
        await _stop_drive(driver, response)
    assert any(f.startswith("id: 2\n") for f in frames)
    assert not any(f.startswith("id: 1\n") for f in frames)
