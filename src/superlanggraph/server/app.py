"""HTTP 接入层：应用工厂。

create_app 只接收装配完成的 ServerRuntime（由组合根构造），挂载全部路由并
管理后台角色生命周期。单进程模式下 driver / worker / cron 循环随应用启停。
"""

from __future__ import annotations

from types import TracebackType
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from superlanggraph.core.settings import AuthSettings
from superlanggraph.server.routes_misc import router as misc_router
from superlanggraph.server.routes_runs import router as runs_router
from superlanggraph.server.routes_threads import router as threads_router
from superlanggraph.server.runtime import ServerRuntime
from superlanggraph.server.schemas import (
    ROUTE_REQUEST_SCHEMAS,
    ROUTE_RESPONSE_SCHEMAS,
    request_body,
    response_schema,
)


async def health() -> dict[str, str]:
    """健康检查：进程存活即返回 ok。"""
    return {"status": "ok"}


def _make_api_key_guard(auth: AuthSettings):
    """构造 x-api-key 校验依赖；未启用鉴权时为直通。"""

    async def guard(request: Request) -> None:
        if not auth.enabled:
            return
        provided = request.headers.get("x-api-key", "")
        if provided not in auth.valid_keys:
            raise HTTPException(401, "无效的 api key")

    return guard


class RuntimeLifespan:
    """把 runtime 的后台角色启停挂到 FastAPI lifespan 的上下文管理器。"""

    def __init__(
        self,
        runtime: ServerRuntime,
        run_background: bool,
        background_roles: set[str] | None = None,
    ) -> None:
        self._runtime = runtime
        self._run_background = run_background
        self._background_roles = background_roles

    async def __aenter__(self) -> None:
        await self._runtime.seed_assistants()
        if self._run_background:
            await self._runtime.start_background(self._background_roles)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._run_background:
            await self._runtime.stop_background()

    def __call__(self, app: FastAPI) -> RuntimeLifespan:
        return self


def _operation_for(spec: dict[str, Any], key: str) -> dict[str, Any] | None:
    method, _, path = key.partition(" ")
    return spec.get("paths", {}).get(path, {}).get(method)


def _attach_openapi_schemas(app: FastAPI) -> None:
    """把 schemas.py 的请求/响应模型注入 OpenAPI（Swagger 展示与 Try-it-out）。

    路由用裸 Request 手工解析 body（容错空 body），FastAPI 无法自行推断；
    模型对照 langgraph-sdk 0.4.4 的 payload 合同，注入时已有的响应声明保留。
    """
    spec = app.openapi()
    for key, model in ROUTE_REQUEST_SCHEMAS.items():
        operation = _operation_for(spec, key)
        if operation is not None:
            operation["requestBody"] = request_body(model)
    for key, model in ROUTE_RESPONSE_SCHEMAS.items():
        operation = _operation_for(spec, key)
        if operation is not None:
            responses = operation.setdefault("responses", {})
            responses["200"] = response_schema(model)


def create_app(
    runtime: ServerRuntime,
    *,
    run_background: bool = True,
    background_roles: set[str] | None = None,
    auth: AuthSettings | None = None,
) -> FastAPI:
    """创建应用实例；依赖注入在组合根完成，路由层不自取全局状态。

    ``background_roles``：本进程内嵌的后台循环集合（driver/worker/cron）；
    None 表示单机全角色模式。
    """
    auth_settings = auth or AuthSettings()
    guard = _make_api_key_guard(auth_settings)
    dependencies = [Depends(guard)] if auth_settings.enabled else []
    app = FastAPI(
        title="superlanggraph",
        version="0.1.0",
        lifespan=RuntimeLifespan(runtime, run_background, background_roles),
    )
    app.state.runtime = runtime
    app.add_api_route("/health", health)
    app.include_router(runs_router, dependencies=dependencies)
    app.include_router(threads_router, dependencies=dependencies)
    app.include_router(misc_router, dependencies=dependencies)
    _attach_openapi_schemas(app)
    FastAPIInstrumentor.instrument_app(app)
    return app
