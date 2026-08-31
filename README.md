# superlanggraph

自托管、生产就绪的 LangGraph 运行时：以 Redis 为中枢，把 LangGraph 的「调度」与
「执行」拆分为可独立扩缩容的服务，对外提供与 LangGraph Platform 兼容的 HTTP API。

**验收标准**：官方 `langgraph-sdk` 不改一行代码直连本系统，全部功能可用
（见 `tests/e2e/` 验收套件）。

## 快速开始

```bash
# 1. 启动外部服务（Redis 开 AOF）
docker compose up -d redis

# 2. 声明图（与官方 langgraph.json 格式一致）
cat > langgraph.json <<'JSON'
{"graphs": {"agent": "./agent.py:graph"}}
JSON

# 3. 启动服务（单进程全角色模式）
uv run superlanggraph server --port 8000
```

SDK 直连（零改动）：

```python
from langgraph_sdk import get_client

client = get_client(url="http://127.0.0.1:8000", api_key="...")
async for chunk in client.runs.stream(thread_id, "agent", input={"count": 2}):
    print(chunk.event, chunk.data)
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

## 质量门禁（PR 必须全绿）

```bash
uv run ruff format --check src tests
uv run ruff check src tests
uv run pyright        # strict 全仓
uv run lint-imports   # 分层依赖方向合同
uv run pytest         # 单元 + 集成（需 docker compose up -d redis）+ e2e 验收
```

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

环境变量前缀 `SLG_`（见 `core/settings.py`）：`SLG_REDIS_URL`、`SLG_STREAM_PREFIX`、
`SLG_EVENT_MAXLEN`、`SLG_LEASE_SECONDS`；鉴权：`SLG_AUTH_API_KEYS`（逗号分隔，
为空则不启用）。敏感信息只放 `.env`，不入库不入 git。
