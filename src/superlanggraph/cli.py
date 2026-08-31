"""组合根与角色入口。

这是全仓库唯一允许把具体实现绑定到协议的地方（组合根模式）。

角色模型（架构 D2 的正式形态）：

- ``server``：HTTP 接入 + 可选内嵌后台循环（``--roles`` 控制），单机模式默认全角色；
- ``driver`` / ``worker`` / ``cron``：无 HTTP 的纯后台进程，独立部署、独立伸缩。

    superlanggraph server [--roles driver,worker,cron]
    superlanggraph driver   # 纯调度进程
    superlanggraph worker   # 纯执行进程
    superlanggraph cron     # 纯定时调度进程
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import redis.asyncio as aioredis
import uvicorn

from superlanggraph.broker.redis import RedisBroker
from superlanggraph.broker.runs import RunStore
from superlanggraph.checkpoint.redis_saver import RedisSaver
from superlanggraph.core.settings import AuthSettings, BrokerSettings
from superlanggraph.cron.scheduler import CronTable
from superlanggraph.driver.runner import ControlDispatcher, RunExecutor
from superlanggraph.registry.loader import Registry
from superlanggraph.server.app import create_app
from superlanggraph.server.runtime import ServerRuntime
from superlanggraph.store.redis_store import RedisStore
from superlanggraph.worker.executor import NodeWorker

_ROLES = ("server", "driver", "worker", "cron")


def main(argv: Sequence[str] | None = None) -> int:
    """解析角色参数并启动对应服务。"""
    parser = argparse.ArgumentParser(prog="superlanggraph", description="自托管 LangGraph 运行时")
    parser.add_argument("role", choices=_ROLES, help="要启动的角色")
    parser.add_argument("--host", default="127.0.0.1", help="server 角色的监听地址")
    parser.add_argument("--port", type=int, default=8000, help="server 角色的监听端口")
    parser.add_argument("--config", default="langgraph.json", help="langgraph.json 路径")
    parser.add_argument(
        "--roles",
        default="driver,worker,cron",
        help="server 进程内嵌的后台循环，逗号分隔（默认全部内嵌，单机模式）",
    )
    args = parser.parse_args(argv)

    settings = BrokerSettings()
    config_path = Path(args.config)

    if args.role == "server":
        roles = {part.strip() for part in args.roles.split(",") if part.strip()}
        app = build_app(settings, config_path, background_roles=roles)
        uvicorn.run(app, host=args.host, port=args.port)
        return 0
    if args.role in ("driver", "worker", "cron"):
        asyncio.run(serve_background_role(settings, config_path, args.role))
        return 0
    parser.error(f"未知角色: {args.role}")
    return 1


def build_runtime(settings: BrokerSettings, config_path: Path) -> ServerRuntime:
    """组合根装配：绑定全部具体实现，构造 ServerRuntime。"""
    client = aioredis.from_url(settings.redis_url, socket_connect_timeout=5.0)
    broker = RedisBroker(settings, client)
    saver = RedisSaver(client)
    runs = RunStore(client, prefix=settings.stream_prefix)
    kv = RedisStore(client, prefix=settings.stream_prefix)
    crons = CronTable(client, prefix=settings.stream_prefix)
    registry = Registry(config_path)
    executor = RunExecutor(registry, saver, broker, broker, broker, runs)
    dispatcher = ControlDispatcher(executor, runs, saver, broker)
    worker = NodeWorker(registry, broker, broker, broker)
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
        dispatcher=dispatcher,
        worker=worker,
    )


def build_app(
    settings: BrokerSettings,
    config_path: Path,
    *,
    background_roles: set[str] | None = None,
    auth: AuthSettings | None = None,
) -> Any:
    """组合根：装配 ServerRuntime 并返回带生命周期管理的 HTTP 应用。

    ``background_roles`` 指定本进程内嵌的后台循环；传空集合即为纯 HTTP 接入
    （后台角色由独立进程承担）。
    """
    runtime = build_runtime(settings, config_path)
    return create_app(
        runtime,
        run_background=True,
        background_roles=background_roles,
        auth=auth,
    )


async def serve_background_role(settings: BrokerSettings, config_path: Path, role: str) -> None:
    """纯后台角色的入口：无 HTTP，信号触发优雅停机。"""
    runtime = build_runtime(settings, config_path)
    await runtime.serve_roles({role})


if __name__ == "__main__":
    raise SystemExit(main())
