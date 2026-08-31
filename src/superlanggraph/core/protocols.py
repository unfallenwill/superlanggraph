"""基础设施协议：依赖倒置的枢纽。

服务层（driver / worker / server）只依赖本模块声明的接口；
具体实现位于 broker 包，由 cli（组合根）负责绑定。
新增后端 = 实现这些协议 + 在组合根注册，服务层零改动。
"""

from collections.abc import AsyncGenerator
from typing import Any, Protocol

from superlanggraph.core.envelope import Envelope


class ControlBus(Protocol):
    """run 生命周期控制消息：start / cancel / resume。"""

    async def publish_control(self, envelope: Envelope) -> None:
        """发布一条控制消息。"""
        ...

    def consume_control(self) -> AsyncGenerator[Envelope, None]:
        """持续消费控制消息（尾随语义，不会自然结束）。"""
        ...


class TaskQueue(Protocol):
    """节点任务队列：至少一次投递，凭据（receipt）确认。"""

    async def enqueue_task(self, envelope: Envelope) -> None:
        """入队一个节点任务。"""
        ...

    async def dequeue_task(self, lease_seconds: float) -> tuple[str, Envelope] | None:
        """领取一个任务；返回 (凭据, 任务)，超时无任务返回 None。

        凭据必须通过 ack_task 释放；超时未确认的任务会被重新投递。
        """
        ...

    async def ack_task(self, receipt: str) -> None:
        """确认任务已完成；未知凭据显式报错。"""
        ...

    async def reclaim_stale(self, min_idle_ms: int) -> list[tuple[str, Any]]:
        """认领停滞任务（worker 死亡遗留的 PEL 消息），返回 (凭据, 任务)。

        driver 按 task_id 去重，重复执行的结果会被安全丢弃。
        """
        ...


class ResultBus(Protocol):
    """worker 到 driver 的执行结果流，按 run 隔离。"""

    async def publish_result(self, run_id: str, envelope: Envelope) -> None:
        """向指定 run 的结果流写入一条结果。"""
        ...

    def read_results(
        self, run_id: str, last_id: str = "0"
    ) -> AsyncGenerator[tuple[str, Envelope], None]:
        """尾随读取指定 run 的结果流，返回 (条目 ID, 封套)。"""
        ...


class EventBus(Protocol):
    """流式事件总线：单写者追加、按序读取、支持断点续读。"""

    async def append_event(self, run_id: str, envelope: Envelope) -> str:
        """追加事件，返回流条目 ID（即 SSE 的 id: 字段）。"""
        ...

    def read_events(
        self, run_id: str, last_id: str = "0"
    ) -> AsyncGenerator[tuple[str, Envelope], None]:
        """尾随读取指定 run 的事件流；凭 last_id 断点续读。"""
        ...
