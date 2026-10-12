# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
# redis-py 与 httpx 流式接口部分方法存根类型不完整；集成测试作为类型边界放宽此三条，src/ 不受影响。

"""监控 API 集成测试：RedisMetricStore + 真 Redis + httpx ASGITransport（设计 §10.2）。

需要本地 Redis（docker compose up -d），不可达时自动跳过。store 键使用独立前缀
``slg:mon:test:{uuid}:*``，用例结束整体清理，不污染 ``slg:*`` 常规前缀。

app 一律以 ``create_app(runtime, run_background=False, monitor_store=store)`` 构造
（与组合根接线方式一致）；ASGITransport 不执行 lifespan，runtime 仅承担装配契约。
"""

from __future__ import annotations

import asyncio
import re
import time
import uuid
from collections.abc import AsyncIterator, Callable, MutableMapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from redis.asyncio import Redis

from superlanggraph.broker.redis import RedisBroker
from superlanggraph.broker.runs import RunStore
from superlanggraph.checkpoint.redis_saver import RedisSaver
from superlanggraph.core.settings import AuthSettings, BrokerSettings
from superlanggraph.cron.scheduler import CronTable
from superlanggraph.driver.runner import ControlDispatcher, RunExecutor
from superlanggraph.monitoring import routes as monitor_routes
from superlanggraph.monitoring.events import NodeSpanEvent, RunLifecycleEvent, TokenUsageEvent
from superlanggraph.monitoring.store import RedisMetricStore
from superlanggraph.registry.loader import Registry
from superlanggraph.server.app import create_app
from superlanggraph.server.runtime import ServerRuntime
from superlanggraph.store.redis_store import RedisStore
from superlanggraph.worker.executor import NodeWorker

_AGENT_PY = '''\
"""占位图：Registry 仅解析声明不加载，文件存在即可。"""
'''

_FRAME_TIMEOUT = 5.0  # SSE 单帧读取安全网；正常路径远小于该值


async def _can_reach_redis(url: str) -> bool:
    client = Redis.from_url(url, socket_connect_timeout=0.5, socket_timeout=0.5)
    try:
        return bool(await client.ping())
    except Exception:
        return False
    finally:
        await client.aclose()


def _runtime(client: Redis, config_path: Path, settings: BrokerSettings) -> ServerRuntime:
    """照组合根 cli.build_runtime 精简装配：仅 create_app 类型契约所需，无后台角色。"""
    broker = RedisBroker(settings, client)
    saver = RedisSaver(client)
    runs = RunStore(client, prefix=settings.stream_prefix)
    kv = RedisStore(client, prefix=settings.stream_prefix)
    crons = CronTable(client, prefix=settings.stream_prefix)
    registry = Registry(config_path)
    executor = RunExecutor(registry, saver, broker, broker, broker, runs)
    return ServerRuntime(
        redis=client,
        registry=registry,
        saver=saver,
        runs=runs,
        crons=crons,
        kv=kv,
        tasks=broker,
        results=broker,
        events=broker,
        control=broker,
        executor=executor,
        dispatcher=ControlDispatcher(executor, runs, saver, broker),
        worker=NodeWorker(registry, broker, broker, broker),
    )


@dataclass(slots=True)
class MonitorEnv:
    """单用例集成环境：HTTP 客户端、store 与可复用的 runtime/app（auth 用例二次构 app）。"""

    http: httpx.AsyncClient
    store: RedisMetricStore
    redis: Redis
    runtime: ServerRuntime
    app: FastAPI


@pytest.fixture
async def env(tmp_path: Path) -> AsyncIterator[MonitorEnv]:
    settings = BrokerSettings(stream_prefix=f"slg-mon-it-{uuid.uuid4().hex[:8]}")
    if not await _can_reach_redis(settings.redis_url):
        pytest.skip("需要本地 Redis：docker compose up -d")
    client = Redis.from_url(settings.redis_url)
    prefix = f"slg:mon:test:{uuid.uuid4().hex[:8]}"
    store = RedisMetricStore(client, prefix=prefix, broker_prefix=settings.stream_prefix)
    (tmp_path / "agent.py").write_text(_AGENT_PY, encoding="utf-8")
    config_path = tmp_path / "langgraph.json"
    config_path.write_text('{"graphs": {"demo": "./agent.py:graph"}}', encoding="utf-8")
    runtime = _runtime(client, config_path, settings)
    app = create_app(runtime, run_background=False, monitor_store=store)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        yield MonitorEnv(http=http, store=store, redis=client, runtime=runtime, app=app)
    keys = [key async for key in client.scan_iter(match=f"{prefix}:*")]
    keys.append(prefix)
    await client.delete(*set(keys))
    await client.aclose()


# ---- 事件构造助手（source 按产生端：lifecycle/mono driver 侧，span/token worker 侧） ----


def _lifecycle(
    run_id: str,
    status: str,
    ts: float,
    *,
    graph_id: str = "g1",
    error: str | None = None,
    duration_ms: float | None = None,
) -> RunLifecycleEvent:
    return RunLifecycleEvent(
        event_id=uuid.uuid4().hex,
        ts=ts,
        source="driver",
        run_id=run_id,
        thread_id=f"thread-{run_id}",
        graph_id=graph_id,
        status=status,  # pyright: ignore[reportArgumentType]
        error=error,
        duration_ms=duration_ms,
    )


def _span(
    run_id: str,
    node: str,
    ts: float,
    duration_ms: float,
    *,
    graph_id: str = "g1",
    outcome: str = "return",
    error: str | None = None,
) -> NodeSpanEvent:
    return NodeSpanEvent(
        event_id=uuid.uuid4().hex,
        ts=ts,
        source="worker",
        run_id=run_id,
        thread_id=f"thread-{run_id}",
        graph_id=graph_id,
        task_id=uuid.uuid4().hex,
        node=node,
        queue_ms=1.0,
        duration_ms=duration_ms,
        outcome=outcome,  # pyright: ignore[reportArgumentType]
        error=error,
    )


def _tokens(
    run_id: str, ts: float, input_tokens: int, output_tokens: int, *, graph_id: str = "g1"
) -> TokenUsageEvent:
    return TokenUsageEvent(
        event_id=uuid.uuid4().hex,
        ts=ts,
        source="worker",
        run_id=run_id,
        thread_id=f"thread-{run_id}",
        graph_id=graph_id,
        node="chat",
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        total_tokens=input_tokens + output_tokens,
    )


async def test_overview_shape_and_counts(env: MonitorEnv) -> None:
    """预写 3 run 生命周期 + 2 span + 2 token 事件，overview 各段数值精确。"""
    now = time.time()
    run_running, run_done, run_fail = "run-a", "run-b", "run-c"
    await env.store.record_event(_lifecycle(run_running, "running", now))
    await env.store.record_event(_lifecycle(run_done, "running", now))
    await env.store.record_event(_lifecycle(run_done, "success", now))
    await env.store.record_event(
        _lifecycle(run_fail, "running", now)
    )
    await env.store.record_event(
        _lifecycle(run_fail, "error", now, error="boom", duration_ms=5.0)
    )
    await env.store.record_event(_span(run_done, "chat", now, 100.0))
    await env.store.record_event(_span(run_done, "chat", now, 300.0))
    await env.store.record_event(_tokens(run_done, now, 10, 5))
    await env.store.record_event(_tokens(run_fail, now, 20, 8))

    response = await env.http.get("/monitor/overview")
    assert response.status_code == 200
    body = response.json()
    assert body["status_counts"] == {
        "pending": 0,
        "running": 1,
        "success": 1,
        "error": 1,
        "timeout": 0,
        "interrupted": 0,
    }
    assert body["token_totals"] == {
        "input_tokens": 30,
        "output_tokens": 13,
        "total_tokens": 43,
    }
    assert body["error_summary"]["errors"] == 1
    assert body["error_summary"]["runs_finished"] == 2
    assert body["error_summary"]["rate"] == 0.5
    [recent] = body["error_summary"]["recent"]
    assert recent["run_id"] == run_fail
    assert recent["message"] == "boom"
    assert recent["node"] is None
    [top] = body["node_top"]
    assert (top["graph_id"], top["node"]) == ("g1", "chat")
    assert top["count"] == 2
    assert top["total_ms"] == 400.0
    assert top["avg_ms"] == 200.0
    assert top["max_ms"] == 300.0
    assert top["errors"] == 0
    assert body["queue"] is None  # 探针未运行
    assert body["window_minutes"] == 60


async def test_series_buckets(env: MonitorEnv) -> None:
    """跨两个分钟桶写事件：点连续升序、跨桶数值正确、缺口补零。"""
    align = int(time.time() // 60) * 60
    prev_bucket_ts = align - 60
    now = time.time()
    in_prev = float(prev_bucket_ts) + 30.0  # 上一分钟桶中点
    in_current = align + max((now - align) / 2, 0.001)  # 当前分钟桶内中点，防跨界漂移

    run_prev, run_cur = "run-prev", "run-cur"
    await env.store.record_event(_lifecycle(run_prev, "running", in_prev))
    await env.store.record_event(_lifecycle(run_prev, "success", in_prev))
    await env.store.record_event(_tokens(run_prev, in_prev, 10, 2))
    await env.store.record_event(_lifecycle(run_cur, "running", in_current))
    await env.store.record_event(_lifecycle(run_cur, "error", in_current, error="x"))
    await env.store.record_event(_tokens(run_cur, in_current, 7, 1))

    response = await env.http.get("/monitor/series", params={"minutes": 5})
    assert response.status_code == 200
    points = response.json()["points"]
    assert [p["bucket_ts"] for p in points] == [
        align - 240,
        align - 180,
        align - 120,
        prev_bucket_ts,
        align,
    ]

    by_ts = {p["bucket_ts"]: p for p in points}
    prev_point = by_ts[prev_bucket_ts]
    assert prev_point["runs_started"] == 1
    assert prev_point["runs_finished"] == 1
    assert prev_point["errors"] == 0
    assert prev_point["input_tokens"] == 10
    assert prev_point["output_tokens"] == 2
    cur_point = by_ts[align]
    assert cur_point["runs_started"] == 1
    assert cur_point["runs_finished"] == 1
    assert cur_point["errors"] == 1
    assert cur_point["input_tokens"] == 7
    assert cur_point["output_tokens"] == 1
    for gap_ts in (align - 240, align - 180, align - 120):  # 窗口内无事件分钟补零
        gap = by_ts[gap_ts]
        assert gap["runs_started"] == 0
        assert gap["runs_finished"] == 0
        assert gap["errors"] == 0
        assert gap["input_tokens"] == 0


async def test_nodes_stats_and_spans(env: MonitorEnv) -> None:
    """节点桶跨分钟聚合 + graph_id 过滤 + with_spans 原始样本降序。"""
    align = int(time.time() // 60) * 60
    in_prev = float(align - 60) + 30.0
    now = time.time()

    await env.store.record_event(_span("run-n1", "chat", in_prev, 100.0))
    await env.store.record_event(_span("run-n1", "chat", now + 0.001, 300.0))
    await env.store.record_event(
        _span("run-n1", "chat", now + 0.002, 200.0, outcome="error", error="node boom")
    )
    await env.store.record_event(_span("run-n2", "chat", now, 1000.0, graph_id="g2"))

    response = await env.http.get("/monitor/nodes")
    assert response.status_code == 200
    stats = response.json()["stats"]
    assert [(s["graph_id"], s["node"]) for s in stats] == [("g2", "chat"), ("g1", "chat")]
    assert stats[0]["total_ms"] == 1000.0
    chat = stats[1]
    assert chat["count"] == 3  # 跨两个分钟桶合并
    assert chat["total_ms"] == 600.0
    assert chat["avg_ms"] == 200.0
    assert chat["max_ms"] == 300.0
    assert chat["errors"] == 1

    filtered = (await env.http.get("/monitor/nodes", params={"graph_id": "g1"})).json()
    assert [(s["graph_id"], s["node"]) for s in filtered["stats"]] == [("g1", "chat")]
    assert "spans" not in filtered  # 缺省 with_spans=false 不带原始样本

    with_spans = (
        await env.http.get("/monitor/nodes", params={"graph_id": "g1", "with_spans": "true"})
    ).json()
    spans = with_spans["spans"]
    assert [s["duration_ms"] for s in spans] == [200.0, 300.0, 100.0]  # ts 降序
    assert all(s["type"] == "node_span" and s["node"] == "chat" for s in spans)


async def test_run_summary(env: MonitorEnv) -> None:
    """单 run 汇总（状态/计数/时长）与时间线升序；未知 run 404。"""
    base = time.time()
    run_id = f"run-{uuid.uuid4().hex[:8]}"
    await env.store.record_event(_lifecycle(run_id, "running", base))
    await env.store.record_event(_span(run_id, "chat", base + 0.001, 100.0))
    await env.store.record_event(_span(run_id, "extract", base + 0.002, 50.0))
    await env.store.record_event(_tokens(run_id, base + 0.003, 10, 4))
    await env.store.record_event(
        _lifecycle(run_id, "success", base + 0.004, duration_ms=550.0)
    )

    response = await env.http.get(f"/monitor/runs/{run_id}")
    assert response.status_code == 200
    body = response.json()
    summary = body["summary"]
    assert summary["run_id"] == run_id
    assert summary["graph_id"] == "g1"
    assert summary["status"] == "success"
    assert summary["node_count"] == 2
    assert summary["input_tokens"] == 10
    assert summary["output_tokens"] == 4
    assert summary["total_tokens"] == 14
    assert summary["duration_ms"] == 550.0
    assert [s["node"] for s in body["spans"]] == ["chat", "extract"]  # 时间线升序

    missing = await env.http.get(f"/monitor/runs/run-missing-{uuid.uuid4().hex[:8]}")
    assert missing.status_code == 404


def _parse_sse_frame(chunk: str) -> dict[str, str] | None:
    """解析单个 SSE 帧（不含结尾空行）；注释帧以 ``comment`` 键返回。"""
    fields: dict[str, str] = {}
    for line in chunk.split("\n"):
        if line.startswith(":"):
            fields["comment"] = line.removeprefix(": ")
            continue
        key, sep, value = line.partition(": ")
        fields[key] = value if sep else ""
    return fields or None


class _SseStream:
    """手工驱动 ASGI app 的 SSE 流收集器：``aclose`` 以 http.disconnect 结束连接。

    httpx ASGITransport 会缓冲完整响应（等 more_body=False 才返回），无法消费
    无限 SSE 流，故绕开 httpx 直发 scope/receive/send——与真实服务器的客户端
    断连语义一致。
    """

    def __init__(self, app: FastAPI, path: str, headers: dict[str, str]) -> None:
        self.status = 0
        self.response_headers: dict[str, str] = {}
        self.frames: list[dict[str, str]] = []
        self._buffer = ""
        self._request_sent = False
        self._disconnected = asyncio.Event()
        self._task = asyncio.create_task(
            app(self._scope(path, headers), self._receive, self._send)
        )

    @staticmethod
    def _scope(path: str, headers: dict[str, str]) -> dict[str, Any]:
        return {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "root_path": "",
            "headers": [
                (key.lower().encode(), value.encode()) for key, value in headers.items()
            ],
            "client": ("testclient", 50000),
            "server": ("testserver", 80),
        }

    async def _receive(self) -> dict[str, Any]:
        if not self._request_sent:
            self._request_sent = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await self._disconnected.wait()
        return {"type": "http.disconnect"}

    async def _send(self, message: MutableMapping[str, Any]) -> None:
        if message["type"] == "http.response.start":
            self.status = int(message["status"])
            self.response_headers = {
                str(key, "utf-8").lower(): str(value, "utf-8")
                for key, value in message.get("headers", [])
            }
            return
        self._buffer += bytes(message.get("body", b"")).decode()
        while "\n\n" in self._buffer:
            chunk, self._buffer = self._buffer.split("\n\n", 1)
            frame = _parse_sse_frame(chunk)
            if frame is not None:
                self.frames.append(frame)

    async def wait_until(self, predicate: Callable[[], bool], what: str) -> None:
        """轮询等待帧条件成立；超时抛出并附已收帧便于诊断。"""
        deadline = time.monotonic() + _FRAME_TIMEOUT
        while not predicate():
            if time.monotonic() > deadline:
                raise AssertionError(f"SSE 等待超时（{what}）：已收到帧 {self.frames}")
            await asyncio.sleep(0.02)

    async def aclose(self) -> None:
        self._disconnected.set()
        await asyncio.wait_for(self._task, timeout=_FRAME_TIMEOUT)


async def test_stream_sse(env: MonitorEnv, monkeypatch: pytest.MonkeyPatch) -> None:
    """首帧 snapshot、Last-Event-ID 回放续读、id 单调不重复、心跳注释帧。"""
    for index in range(3):
        await env.store.record_event(
            _span("run-s", "chat", time.time() + index * 0.001, 10.0 + index)
        )
    # 缩短心跳间隔，避免用例等待 15s（模块级常量即为此预留）
    monkeypatch.setattr(monitor_routes, "HEARTBEAT_SECONDS", 0.2)

    stream = _SseStream(env.app, "/monitor/stream", {"last-event-id": "1"})
    try:
        # 等待期可能穿插心跳注释帧，一律按内容断言、不按帧位置
        await stream.wait_until(lambda: len(stream.frames) >= 3, "首帧与回放帧")
        assert stream.status == 200
        assert stream.response_headers["content-type"].startswith("text/event-stream")
        assert stream.response_headers["cache-control"] == "no-store"

        snapshot = stream.frames[0]
        assert snapshot["event"] == "snapshot"
        assert "id" not in snapshot  # 首帧无序号
        assert "status_counts" in snapshot["data"]
        assert [frame["id"] for frame in stream.frames if "id" in frame] == ["2", "3"]
        assert all(frame["event"] == "node_span" for frame in stream.frames if "id" in frame)

        # 泵已完成回放并进入订阅，此后写入的事件走实时通道
        await asyncio.sleep(0.5)
        await env.store.record_event(_span("run-s", "chat", time.time(), 99.0))

        await stream.wait_until(
            lambda: "4" in [frame.get("id") for frame in stream.frames], "实时帧 seq=4"
        )
        # 等待窗口大于心跳间隔，注释帧应已出现
        assert any(frame.get("comment") == "ping" for frame in stream.frames)
        seen = [frame["id"] for frame in stream.frames if "id" in frame]
        assert seen == ["2", "3", "4"]  # 单调递增、无 seq ≤ 1 的重复帧
    finally:
        await stream.aclose()


async def test_static_mount(env: MonitorEnv) -> None:
    """静态页可访问：index.html 引用带 hash 的 assets 资源且资源可 200。"""
    response = await env.http.get("/monitor/ui/")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    match = re.search(r'src="\./?(assets/[\w.-]+\.js)"', response.text)
    assert match is not None, "index.html 未引用 assets/ 下带 hash 的脚本"
    asset = await env.http.get(f"/monitor/ui/{match.group(1)}")
    assert asset.status_code == 200


async def test_auth_guard(env: MonitorEnv) -> None:
    """启用 AuthSettings 后 /monitor REST 与既有路由同一守卫：无 key 401，带 key 200。"""
    app = create_app(
        env.runtime,
        run_background=False,
        monitor_store=env.store,
        auth=AuthSettings(api_keys="k1"),
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        assert (await http.get("/monitor/overview")).status_code == 401
        assert (await http.get("/monitor/series")).status_code == 401
        ok = await http.get("/monitor/overview", headers={"x-api-key": "k1"})
        assert ok.status_code == 200
