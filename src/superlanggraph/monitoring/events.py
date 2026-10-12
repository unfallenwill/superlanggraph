"""监控事件模型:pydantic v2 判别联合,driver / worker / monitor 三端共用的事件合同。

事件由采集器（MonitoringCollector）产生，经 store 落库并通过 SSE 广播；
``event_id`` 为采集端 uuid（溯源用），SSE 序号 ``seq`` 由 store 落库时分配
（Redis INCR），不进事件模型。``error`` 类字段入库前由 store 截断至 500 字符。
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field, TypeAdapter

# 分层约束：不 import broker.runs，字面量与 RUN_STATUSES 保持同步，单测交叉断言一致。
RUN_STATUS = Literal["pending", "running", "success", "error", "timeout", "interrupted"]
"""run 生命周期六态，与 broker.runs.RUN_STATUSES 逐项一致。"""

OUTCOME = Literal["return", "error", "interrupt"]
"""节点任务的结束方式：正常返回 / 执行报错 / 官方 interrupt 暂停。"""


class MonitorEventBase(BaseModel):
    """全部监控事件的公共字段。"""

    event_id: str  # uuid4().hex，采集端生成
    ts: float  # time.time()，事件发生时刻（unix 秒）
    source: Literal["driver", "worker", "monitor"]


class RunLifecycleEvent(MonitorEventBase):
    """run 生命周期迁移；run 级错误由 status="error" + error 字段承载。"""

    type: Literal["run_lifecycle"] = "run_lifecycle"
    run_id: str
    thread_id: str
    graph_id: str
    status: RUN_STATUS  # 本次迁移到的状态
    error: str | None = None  # status == "error" 时非空
    duration_ms: float | None = None  # 终态时由 driver 计算（monotonic 差）


class NodeSpanEvent(MonitorEventBase):
    """单个节点任务的一次执行区间；节点级错误由 outcome="error" + error 字段承载。"""

    type: Literal["node_span"] = "node_span"
    run_id: str
    thread_id: str
    graph_id: str
    task_id: str
    node: str
    queue_ms: float  # 排队等待：worker 收到时刻 - Envelope.ts（driver 入队时刻）
    duration_ms: float  # 节点执行时长：worker 内 monotonic 差
    outcome: OUTCOME
    error: str | None = None


class TokenUsageEvent(MonitorEventBase):
    """一次任务内的令牌用量；采集端按 task_id 做 max 合并后于 task_end 时 flush。"""

    type: Literal["token_usage"] = "token_usage"
    run_id: str
    thread_id: str
    graph_id: str
    node: str | None = None
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    total_tokens: int = Field(ge=0)


class WorkerHeartbeatEvent(MonitorEventBase):
    """worker 心跳；store 据此维护注册表与带 TTL 的心跳键。"""

    type: Literal["worker_heartbeat"] = "worker_heartbeat"
    worker_id: str
    inflight: int = Field(ge=0)  # 当前在飞任务数 len(NodeWorker._inflight)
    concurrency: int = Field(ge=0)  # 信号量容量


MonitorEvent = Annotated[
    RunLifecycleEvent | NodeSpanEvent | TokenUsageEvent | WorkerHeartbeatEvent,
    Field(discriminator="type"),
]
"""监控事件联合；序列化/反序列化经 TypeAdapter(MonitorEvent)。"""

_EVENT_ADAPTER: TypeAdapter[MonitorEvent] = TypeAdapter(MonitorEvent)


def parse_monitor_event(raw: str | bytes) -> MonitorEvent:
    """从 JSON 反序列化；形状非法抛 pydantic.ValidationError。"""
    return _EVENT_ADAPTER.validate_json(raw)
