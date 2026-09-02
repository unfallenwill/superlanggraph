"""POST/PATCH 端点的请求体模型：供 OpenAPI（Swagger / Try-it-out）展示。

字段以 langgraph-sdk 0.4.4 实际发送的 payload 为准（SDK 客户端即合同）；
路由当前消费其中的一个子集，官方声明而服务端暂未消费的字段同样保留，
保证文档与官方合同一致。解析逻辑仍在路由内（容错空 body），模型只做文档。
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, TypeAdapter

Model = type[Any]


def _json_schema(model: type[Any]) -> dict[str, Any]:
    """统一转 schema：模型类 / list[模型] / dict[str, 模型] 都支持。"""
    return TypeAdapter(model).json_schema()


def request_body(model: type[Any]) -> dict[str, Any]:
    """把请求模型转成 OpenAPI 的 requestBody 声明。"""
    return {"required": False, "content": {"application/json": {"schema": _json_schema(model)}}}


def response_schema(model: type[Any]) -> dict[str, Any]:
    """把响应模型转成 OpenAPI 的 200 响应声明。"""
    return {
        "description": "Successful Response",
        "content": {"application/json": {"schema": _json_schema(model)}},
    }


# ---- runs ----


class RunCreateRequest(BaseModel):
    """创建 run（stream / wait / background 共用）。"""

    assistant_id: str = ""
    input: Any = None
    command: dict[str, Any] | None = None
    config: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    stream_mode: str | list[str] | None = None
    interrupt_before: str | list[str] | None = None
    interrupt_after: str | list[str] | None = None
    checkpoint: dict[str, Any] | None = None
    multitask_strategy: str = "reject"
    on_disconnect: str | None = None
    durability: str | None = None


class RunBatchItemRequest(RunCreateRequest):
    thread_id: str = ""


class RunCancelRequest(BaseModel):
    thread_id: str | None = None
    run_ids: list[str] | None = None
    status: str | None = None


class JoinStreamRequest(BaseModel):
    stream_mode: str | list[str] | None = None


# ---- threads ----


class ThreadCreateRequest(BaseModel):
    thread_id: str | None = None
    metadata: dict[str, Any] | None = None
    if_exists: str | None = None
    supersteps: list[dict[str, Any]] | None = None
    ttl: int | dict[str, Any] | None = None


class ThreadUpdateRequest(BaseModel):
    metadata: dict[str, Any] | None = None
    ttl: int | dict[str, Any] | None = None


class ThreadSearchRequest(BaseModel):
    limit: int = 10
    offset: int = 0
    metadata: dict[str, Any] | None = None
    values: dict[str, Any] | None = None
    ids: list[str] | None = None
    status: str | None = None
    sort_by: str | None = None
    sort_order: str | None = None
    select: list[str] | None = None
    extract: bool | None = None


class ThreadCountRequest(BaseModel):
    metadata: dict[str, Any] | None = None
    values: dict[str, Any] | None = None
    status: str | None = None


class ThreadPruneRequest(BaseModel):
    thread_ids: list[str] = []
    strategy: str = "delete"


class ThreadStateUpdateRequest(BaseModel):
    values: Any = None
    checkpoint_id: str | None = None
    checkpoint: dict[str, Any] | None = None
    as_node: str | None = None


class ThreadStateByCheckpointRequest(BaseModel):
    checkpoint: dict[str, Any] | None = None


class ThreadHistoryRequest(BaseModel):
    limit: int = 10
    before: str | dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    checkpoint: dict[str, Any] | None = None


class ThreadStreamRequest(BaseModel):
    stream_mode: str | list[str] | None = None


# ---- assistants ----


class AssistantCreateRequest(BaseModel):
    graph_id: str = ""
    assistant_id: str | None = None
    name: str | None = None
    description: str | None = None
    if_exists: str | None = None
    config: dict[str, Any] | None = None
    context: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None


class AssistantUpdateRequest(BaseModel):
    graph_id: str | None = None
    name: str | None = None
    description: str | None = None
    config: dict[str, Any] | None = None
    context: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None


class AssistantSearchRequest(BaseModel):
    limit: int = 10
    offset: int = 0
    metadata: dict[str, Any] | None = None
    graph_id: str | None = None
    name: str | None = None
    sort_by: str | None = None
    sort_order: str | None = None
    select: list[str] | None = None


class AssistantVersionsRequest(BaseModel):
    limit: int = 10


class AssistantSetLatestRequest(BaseModel):
    version: int = 1


# ---- store ----


class StorePutRequest(BaseModel):
    namespace: list[str] = []
    key: str = ""
    value: Any = None
    index: dict[str, Any] | None = None
    ttl: int | dict[str, Any] | None = None


class StoreDeleteRequest(BaseModel):
    namespace: list[str] = []
    key: str = ""


class StoreSearchRequest(BaseModel):
    namespace_prefix: list[str] = []
    filter: dict[str, Any] | None = None
    limit: int = 10
    offset: int = 0
    query: str | None = None
    refresh_ttl: bool | None = None


class StoreListNamespacesRequest(BaseModel):
    prefix: list[str] | None = None
    suffix: list[str] | None = None
    max_depth: int | None = None
    limit: int = 100
    offset: int = 0


# ---- crons ----


class CronCreateRequest(BaseModel):
    assistant_id: str = ""
    schedule: str = ""
    input: dict[str, Any] | None = None
    thread_id: str | None = None
    config: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    end_time: str | None = None
    enabled: bool | None = None
    timezone: str | None = None
    multitask_strategy: str | None = None
    durability: str | None = None


class CronUpdateRequest(BaseModel):
    schedule: str | None = None
    input: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    end_time: str | None = None
    enabled: bool | None = None
    timezone: str | None = None
    durability: str | None = None


class CronSearchRequest(BaseModel):
    assistant_id: str | None = None
    thread_id: str | None = None
    metadata: dict[str, Any] | None = None
    limit: int = 10
    offset: int = 0


class CronCountRequest(BaseModel):
    assistant_id: str | None = None
    thread_id: str | None = None
    metadata: dict[str, Any] | None = None


# 路径 -> 请求模型。键形如 "method path"；list 表示数组 body（/runs/batch）。
ROUTE_REQUEST_SCHEMAS: dict[str, type[Any]] = {
    "post /assistants": AssistantCreateRequest,
    "patch /assistants/{assistant_id}": AssistantUpdateRequest,
    "post /assistants/search": AssistantSearchRequest,
    "post /assistants/count": AssistantSearchRequest,
    "post /assistants/{assistant_id}/versions": AssistantVersionsRequest,
    "post /assistants/{assistant_id}/latest": AssistantSetLatestRequest,
    "post /threads": ThreadCreateRequest,
    "patch /threads/{thread_id}": ThreadUpdateRequest,
    "post /threads/search": ThreadSearchRequest,
    "post /threads/count": ThreadCountRequest,
    "post /threads/prune": ThreadPruneRequest,
    "post /threads/{thread_id}/state": ThreadStateUpdateRequest,
    "post /threads/{thread_id}/state/checkpoint": ThreadStateByCheckpointRequest,
    "post /threads/{thread_id}/history": ThreadHistoryRequest,
    "post /threads/{thread_id}/stream": ThreadStreamRequest,
    "post /threads/{thread_id}/runs": RunCreateRequest,
    "post /threads/{thread_id}/runs/stream": RunCreateRequest,
    "post /threads/{thread_id}/runs/wait": RunCreateRequest,
    "post /runs/stream": RunCreateRequest,
    "post /runs/wait": RunCreateRequest,
    "post /runs/batch": list[RunBatchItemRequest],
    "post /runs/cancel": RunCancelRequest,
    "post /threads/{thread_id}/runs/{run_id}/stream": JoinStreamRequest,
    "put /store/items": StorePutRequest,
    "delete /store/items": StoreDeleteRequest,
    "post /store/items/search": StoreSearchRequest,
    "post /store/namespaces": StoreListNamespacesRequest,
    "post /runs/crons": CronCreateRequest,
    "post /threads/{thread_id}/runs/crons": CronCreateRequest,
    "patch /runs/crons/{cron_id}": CronUpdateRequest,
    "post /runs/crons/search": CronSearchRequest,
    "post /runs/crons/count": CronCountRequest,
}


class GraphResponse(BaseModel):
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []


class AssistantSchemas(BaseModel):
    graph_id: str
    input_schema: dict[str, Any] | None = None
    output_schema: dict[str, Any] | None = None
    state_schema: dict[str, Any] | None = None
    config_schema: dict[str, Any] | None = None
    context_schema: dict[str, Any] | None = None


class SubgraphInfo(BaseModel):
    graph_id: str
    input_schema: dict[str, Any] | None = None
    output_schema: dict[str, Any] | None = None


class HealthResponse(BaseModel):
    status: str


# ---- 响应模型 ----


class Assistant(BaseModel):
    assistant_id: str
    graph_id: str
    name: str
    version: int
    config: dict[str, Any] = {}
    context: dict[str, Any] = {}
    metadata: dict[str, Any] = {}
    created_at: str | None = None
    updated_at: str | None = None


class AssistantVersion(BaseModel):
    assistant_id: str
    version: int
    created_at: str | None = None


class Thread(BaseModel):
    thread_id: str
    status: str
    metadata: dict[str, Any] = {}
    created_at: str | None = None
    updated_at: str | None = None
    values: Any = None
    interrupts: Any = None
    config: Any = None
    ttl: Any = None


class Checkpoint(BaseModel):
    thread_id: str
    checkpoint_ns: str = ""
    checkpoint_id: str | None = None


class ThreadTask(BaseModel):
    id: str
    name: str
    path: list[str] = []
    error: str | None = None
    interrupts: list[dict[str, Any]] = []
    state: Any = None
    result: Any = None


class ThreadState(BaseModel):
    values: Any = None
    next: list[str] = []
    checkpoint: Checkpoint
    metadata: dict[str, Any] = {}
    created_at: str | None = None
    parent_checkpoint: Checkpoint | None = None
    tasks: list[ThreadTask] = []
    interrupts: list[dict[str, Any]] = []


class ThreadStateUpdateResponse(BaseModel):
    checkpoint: Checkpoint


class Run(BaseModel):
    run_id: str
    thread_id: str
    assistant_id: str | None = None
    status: str
    metadata: dict[str, Any] = {}
    input: Any = None
    command: Any = None
    created_at: str | None = None
    updated_at: str | None = None


class StoreItem(BaseModel):
    value: Any = None
    namespace: list[str] = []
    key: str = ""
    created_at: str | None = None
    updated_at: str | None = None


class StoreSearchResponse(BaseModel):
    items: list[StoreItem] = []


class Cron(BaseModel):
    cron_id: str
    assistant_id: str
    thread_id: str | None = None
    schedule: str
    input: Any = None
    metadata: dict[str, Any] = {}
    enabled: bool | None = None
    created_at: str | None = None
    updated_at: str | None = None
    next_run_date: str | None = None


# "方法 路径" -> 200 响应模型。SSE（stream）与自由形状（wait/join 返回线程
# values）无法用固定模型描述，保持 FastAPI 推断的泛型声明。
ROUTE_RESPONSE_SCHEMAS: dict[str, Any] = {
    "get /health": HealthResponse,
    "post /assistants": Assistant,
    "patch /assistants/{assistant_id}": Assistant,
    "post /assistants/search": list[Assistant],
    "post /assistants/{assistant_id}/latest": Assistant,
    "post /assistants/{assistant_id}/versions": list[AssistantVersion],
    "get /assistants/{assistant_id}": Assistant,
    "get /assistants/{assistant_id}/graph": GraphResponse,
    "get /assistants/{assistant_id}/schemas": AssistantSchemas,
    "get /assistants/{assistant_id}/subgraphs": dict[str, SubgraphInfo],
    "get /assistants/{assistant_id}/subgraphs/{namespace}": dict[str, SubgraphInfo],
    "get /threads/{thread_id}": Thread,
    "post /threads": Thread,
    "patch /threads/{thread_id}": Thread,
    "post /threads/search": list[Thread],
    "get /threads/{thread_id}/state": ThreadState,
    "get /threads/{thread_id}/state/{checkpoint_id}": ThreadState,
    "post /threads/{thread_id}/state/checkpoint": ThreadState,
    "post /threads/{thread_id}/state": ThreadStateUpdateResponse,
    "post /threads/{thread_id}/history": list[ThreadState],
    "get /threads/{thread_id}/runs": list[Run],
    "get /threads/{thread_id}/runs/{run_id}": Run,
    "post /threads/{thread_id}/runs": Run,
    "post /threads/{thread_id}/runs/{run_id}/cancel": Run,
    "post /runs/batch": list[Run],
    "post /runs/cancel": list[Run],
    "get /store/items": StoreItem,
    "post /store/items/search": StoreSearchResponse,
    "post /runs/crons": Cron,
    "post /threads/{thread_id}/runs/crons": Cron,
    "patch /runs/crons/{cron_id}": Cron,
    "post /runs/crons/search": list[Cron],
}
