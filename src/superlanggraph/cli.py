"""组合根与角色入口。

这是全仓库唯一允许把具体实现绑定到协议的地方（组合根模式）。

角色模型（架构 D2 的正式形态）：

- ``server``：HTTP 接入 + 可选内嵌后台循环（``--roles`` 控制），单机模式默认全角色；
- ``driver`` / ``worker`` / ``cron``：无 HTTP 的纯后台进程，独立部署、独立伸缩。
- ``up``：Docker Compose 一键部署（对齐官方 ``langgraph up``）。

    superlanggraph server [--roles driver,worker,cron]
    superlanggraph up      # Docker Compose 一键部署
    superlanggraph driver  # 纯调度进程
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

import redis.asyncio as aioredis
import uvicorn
from dotenv import dotenv_values

from superlanggraph.broker.redis import RedisBroker
from superlanggraph.broker.runs import RunStore
from superlanggraph.checkpoint.redis_saver import RedisSaver
from superlanggraph.core.settings import AuthSettings, BrokerSettings
from superlanggraph.cron.scheduler import CronTable
from superlanggraph.driver.runner import ControlDispatcher, RunExecutor
from superlanggraph.observability import setup_otel
from superlanggraph.registry.loader import Registry
from superlanggraph.server.app import create_app
from superlanggraph.server.runtime import ServerRuntime
from superlanggraph.store.redis_store import RedisStore
from superlanggraph.worker.executor import NodeWorker

_ROLES = ("server", "driver", "worker", "cron", "up")


def _install_dependencies(config_path: Path) -> None:
    """读取 langgraph.json 的 dependencies 数组，安装第三方依赖到当前环境。

    对齐官方 langgraph.json 的 dependencies 字段语义。
    优先用 uv pip install（项目实际使用 uv 管理依赖），pip 兜底。
    """
    if not config_path.exists():
        return
    config = json.loads(config_path.read_text(encoding="utf-8"))
    deps = config.get("dependencies", [])
    if not deps:
        return

    for dep in deps:
        print(f"  安装依赖: {dep}", flush=True)
        result = subprocess.run(
            ["uv", "pip", "install", dep],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            print(f"  警告: {dep} 安装失败: {result.stderr[:200]}", file=sys.stderr)


def load_config_env(config_path: Path) -> None:
    """对齐官方 langgraph.json 的 ``env`` 字段，把变量注入当前进程环境。

    与官方 dev 行为一致：字符串形态是 .env 文件路径（相对 langgraph.json 所在
    目录解析），字典形态是内联键值映射；加载的值覆盖已有环境变量。因此必须在
    构造 settings、导入用户图之前调用。字段缺省时不做任何事（不自动读 .env）。
    """
    if not config_path.exists():
        return
    env_field = json.loads(config_path.read_text(encoding="utf-8")).get("env")
    if env_field is None:
        return

    if isinstance(env_field, str):
        env_path = (config_path.parent / env_field).resolve()
        if not env_path.exists():
            print(f"  警告: langgraph.json 声明的 env 文件不存在: {env_path}", file=sys.stderr)
            return
        values = {k: v for k, v in dotenv_values(env_path).items() if v is not None}
    elif isinstance(env_field, dict):
        mapping = cast("dict[str, Any]", env_field)
        values = {str(k): str(v) for k, v in mapping.items()}
    else:
        msg = f"langgraph.json 的 env 字段仅支持文件路径或键值映射，收到: {env_field!r}"
        raise ValueError(msg)

    for key, value in values.items():
        os.environ[key] = value


def main(argv: Sequence[str] | None = None) -> int:
    """解析角色参数并启动对应服务。"""
    parser = argparse.ArgumentParser(prog="superlanggraph", description="自托管 LangGraph 运行时")
    sub = parser.add_subparsers(dest="command")

    server_parser = sub.add_parser("server", help="启动 HTTP 服务（可内嵌后台角色）")
    server_parser.add_argument("--host", default="0.0.0.0", help="监听地址")
    server_parser.add_argument("--port", type=int, default=8000, help="监听端口")
    server_parser.add_argument("--config", default="langgraph.json", help="langgraph.json 路径")
    server_parser.add_argument(
        "--roles",
        default="driver,worker,cron",
        help="内嵌后台循环，逗号分隔（默认全部内嵌，单机模式）",
    )

    up_parser = sub.add_parser("up", help="Docker Compose 一键部署（对齐 langgraph up）")
    up_parser.add_argument("--port", type=int, default=8000, help="对外端口")
    up_parser.add_argument("--detach", action="store_true", help="后台运行")
    up_parser.add_argument(
        "--compose-file",
        default=str(Path(__file__).resolve().parents[3] / "deploy" / "docker-compose.yml"),
        help="部署用 compose 文件路径",
    )

    for role in ("driver", "worker", "cron"):
        role_parser = sub.add_parser(role, help=f"纯 {role} 后台进程")
        role_parser.add_argument("--config", default="langgraph.json", help="langgraph.json 路径")

    args = parser.parse_args(argv)

    if args.command == "server":
        config_path = Path(args.config)
        _install_dependencies(config_path)
        load_config_env(config_path)
        settings = BrokerSettings()
        roles = {part.strip() for part in args.roles.split(",") if part.strip()}
        app = build_app(settings, config_path, background_roles=roles)
        uvicorn.run(app, host=args.host, port=args.port)
        return 0
    if args.command == "up":
        return _run_up(args)
    if args.command in ("driver", "worker", "cron"):
        config_path = Path(args.config)
        _install_dependencies(config_path)
        load_config_env(config_path)
        settings = BrokerSettings()
        asyncio.run(serve_background_role(settings, config_path, args.command))
        return 0
    parser.print_help()
    return 1


def _run_up(args: argparse.Namespace) -> int:
    """Docker Compose 一键部署：封装 compose up 并透传端口/配置。"""
    config_path = Path(args.config).resolve()
    if not config_path.exists():
        print(f"找不到 {config_path}，请先在项目根目录准备 langgraph.json", file=sys.stderr)
        return 1

    compose_file = Path(args.compose_file).resolve()
    if not compose_file.exists():
        print(f"找不到 compose 文件: {compose_file}", file=sys.stderr)
        return 1

    cmd: list[str] = [
        "docker",
        "compose",
        "-f",
        str(compose_file),
        "-p",
        "superlanggraph",
        "--project-directory",
        str(config_path.parent),
        "up",
    ]
    if args.detach:
        cmd.append("-d")

    print(f"superlanggraph up（config: {config_path}）", flush=True)
    result = subprocess.run(cmd, check=False)
    return result.returncode


def build_runtime(settings: BrokerSettings, config_path: Path) -> ServerRuntime:
    """组合根装配：绑定全部具体实现，构造 ServerRuntime。"""
    client = aioredis.from_url(
        settings.redis_url,
        socket_connect_timeout=5.0,
        max_connections=settings.redis_max_connections,
    )
    broker = RedisBroker(settings, client)
    saver = RedisSaver(client)
    runs = RunStore(client, prefix=settings.stream_prefix)
    kv = RedisStore(client, prefix=settings.stream_prefix)
    crons = CronTable(client, prefix=settings.stream_prefix)
    registry = Registry(config_path)
    executor = RunExecutor(registry, saver, broker, broker, broker, runs)
    dispatcher = ControlDispatcher(executor, runs, saver, broker)
    worker = NodeWorker(registry, broker, broker, broker, concurrency=settings.worker_concurrency)
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
    setup_otel("server")
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
