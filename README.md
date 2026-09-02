# superlanggraph

自托管、生产就绪的 LangGraph 运行时：以 Redis 为中枢，把 LangGraph 的「调度」与
「执行」拆分为可独立扩缩容的服务，对外提供与 LangGraph Platform 兼容的 HTTP API。

**为什么自托管**：官方 Platform 把调度和执行都管起来，但你的图、checkpoint、store、
事件流也存在别人的云上。superlanggraph 把这套 API 原样搬回你自己的机器——数据全部
落在自己的 Redis 里；driver 跑官方 Pregel 循环，调度语义 100% 官方；worker 无状态，
吞吐不够就加进程。

**验收标准**：官方 `langgraph-sdk` 不改一行代码直连本系统，全部功能可用。对齐的
可执行定义是 SDK 的 5 个命名空间 46 个方法逐条可用（见 `docs/architecture.md`
与 `tests/e2e/` 验收套件）。

## 前置条件

- Python 3.12+ 与 [uv](https://docs.astral.sh/uv/)
- Docker（Redis 与可选的可观测性组件）

克隆本仓库后执行 `uv sync` 安装依赖。

## 快速开始

```bash
# 1. 启动 Redis（开 AOF）
docker compose up -d redis

# 2. 启动服务（加载仓库自带示例图；换成你自己的 langgraph.json 亦可）
uv run superlanggraph server --config examples/demo/langgraph.json

# 3. 另开一个终端：官方 SDK 直连，跑通流式 / 同步 / HITL / store 全流程
uv run python examples/demo/client.py
```

`langgraph.json` 与官方格式一致，指向你自己的图只需改 `--config`：

```json
{"graphs": {"agent": "./agent.py:graph"}}
```

最小直连示例（未启用鉴权时 `api_key` 任填）：

```python
import asyncio

from langgraph_sdk import get_client

async def main() -> None:
    client = get_client(url="http://127.0.0.1:8000", api_key="demo-key")
    thread = await client.threads.create()
    async for chunk in client.runs.stream(thread["thread_id"], "agent", input={"count": 2}):
        print(chunk.event, chunk.data)

asyncio.run(main())
```

## 部署

按规模选形态，四种都出自同一个 CLI：

| 形态 | 命令 | 说明 |
| --- | --- | --- |
| 单机全角色（默认） | `uv run superlanggraph server` | server 内嵌 driver + worker + cron，开发与小规模首选 |
| 纯后台角色 | `uv run superlanggraph driver`（`worker` / `cron` 同理） | 与 server 分进程部署，按角色伸缩 |
| Docker 一键部署 | `uv run superlanggraph up` | 对齐官方 `langgraph up` 体验 |
| 多容器生产 | `deploy/docker-compose.yml` | server 纯 HTTP 接入 + driver/worker 独立扩缩 |

多容器模式：在包含 `langgraph.json` 与图源码的项目目录下运行。compose 默认单容器
全角色，取消其中注释即可拆出独立 driver / worker（worker 支持 `deploy.replicas`）：

```bash
docker compose -f deploy/docker-compose.yml up -d
```

## 架构一页图

```
cli（组合根，唯一绑定具体实现处）
  └─▶ server / driver / worker / cron / registry     服务层
        └─▶ broker / checkpoint / cache / store      基础设施层
              └─▶ api                                HTTP 契约
                    └─▶ core                         契约底座（封套/异常/协议/配置）
```

- **driver** 跑官方 Pregel 循环，节点经 `RemoteNodeProxy` 远程执行——语义 100% 官方；
- **worker** 无状态横向扩，执行真实节点并把 token/custom 事件实时写回事件流；
- **checkpointer / store / cache** 均为 Redis 实现（`BaseCheckpointSaver` 等官方接口）。

详细设计：`docs/architecture.md`（架构与决策）、`docs/api-contract.md`（HTTP 合同）、
`docs/engineering.md`（工程规范与魔法条款）。

## 可观测性（OpenTelemetry）

设置 `OTEL_EXPORTER_OTLP_ENDPOINT` 即启用 traces + metrics 导出（OTLP/HTTP）；
未设置时整库 no-op，零开销。每个角色进程是独立的 `service.name`
（`superlanggraph-server` / `-driver` / `-worker`），W3C TraceContext 经
Envelope 跨进程贯通——HTTP 入口到远程节点执行是一条完整 trace。

本地起 collector + Prometheus（P2 可观测）：

```bash
docker compose --profile observability up -d otel-collector
# Prometheus 抓取端点: http://127.0.0.1:8889/metrics
```

## 配置

环境变量前缀 `SLG_`，经 `.env` 读取（定义见 `src/superlanggraph/core/settings.py`）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SLG_REDIS_URL` | `redis://localhost:6379/0` | Redis 连接地址 |
| `SLG_REDIS_MAX_CONNECTIONS` | `2000` | 连接池上限 |
| `SLG_WORKER_CONCURRENCY` | `32` | worker 单进程节点执行并发 |
| `SLG_STREAM_PREFIX` | `slg` | Redis Stream key 前缀 |
| `SLG_EVENT_MAXLEN` | `10000` | 事件流最大长度 |
| `SLG_LEASE_SECONDS` | `30` | 任务租约时长（故障接管节奏） |
| `SLG_AUTH_API_KEYS` | 空 | 逗号分隔；为空则不启用鉴权 |

敏感信息只放 `.env`，不入库不入 git。

## 质量门禁（PR 必须全绿）

```bash
uv run ruff format --check src tests
uv run ruff check src tests
uv run pyright        # strict 全仓
uv run lint-imports   # 分层依赖方向合同
uv run pytest         # 单元 + 集成（需 docker compose up -d redis）+ e2e 验收
```

## 项目导航

| 路径 | 内容 |
| --- | --- |
| `docs/architecture.md` | 架构与决策，含对齐定义（SDK 5 命名空间 46 方法逐条对应） |
| `docs/api-contract.md` | HTTP 合同：SSE 线上协议 + 全部路由 |
| `docs/engineering.md` | 工程规范与魔法条款 |
| `examples/demo/` | 示例图（`agent.py` / `hitl.py` / `slow.py`）、SDK 全流程演示（`client.py`）、压测（`benchmark.py`）、故障恢复演示（`recovery_demo.py`） |
| `deploy/docker-compose.yml` | 部署编排（单容器 / 多容器两套模式） |

## License

[MIT](LICENSE)
