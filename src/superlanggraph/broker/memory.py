"""进程内 broker 实现：单元测试与本地联调使用，不依赖任何外部服务。

语义与 Redis 实现对齐：任务至少一次（凭据确认）、事件按单调 ID 追加与续读、
读接口均为尾随语义（不会自然结束）。
"""

import asyncio
import uuid
from collections import defaultdict
from collections.abc import AsyncGenerator

from superlanggraph.core.envelope import Envelope
from superlanggraph.core.errors import BrokerError

StreamEntry = tuple[str, Envelope]


class MemoryBroker:
    """core.protocols 四类通道的进程内实现。"""

    def __init__(self) -> None:
        self._control: asyncio.Queue[Envelope] = asyncio.Queue()
        self._tasks: asyncio.Queue[Envelope] = asyncio.Queue()
        self._inflight: dict[str, Envelope] = {}
        self._results: dict[str, list[StreamEntry]] = defaultdict(list)
        self._events: dict[str, list[StreamEntry]] = defaultdict(list)
        self._results_changed: dict[str, asyncio.Event] = defaultdict(asyncio.Event)
        self._events_changed: dict[str, asyncio.Event] = defaultdict(asyncio.Event)

    async def publish_control(self, envelope: Envelope) -> None:
        await self._control.put(envelope)

    def consume_control(self) -> AsyncGenerator[Envelope, None]:
        return self._pump(self._control)

    async def enqueue_task(self, envelope: Envelope) -> None:
        await self._tasks.put(envelope)

    async def dequeue_task(self, lease_seconds: float) -> tuple[str, Envelope] | None:
        try:
            envelope = await asyncio.wait_for(self._tasks.get(), timeout=lease_seconds)
        except TimeoutError:
            return None
        receipt = uuid.uuid4().hex
        self._inflight[receipt] = envelope
        return receipt, envelope

    async def ack_task(self, receipt: str) -> None:
        if self._inflight.pop(receipt, None) is None:
            raise BrokerError(f"未知任务凭据: {receipt}")

    async def reclaim_stale(self, min_idle_ms: int) -> list[tuple[str, Envelope]]:
        """进程内实现没有孤儿消息，无需回收。"""
        return []

    async def publish_result(self, run_id: str, envelope: Envelope) -> None:
        self._append(self._results, run_id, envelope)
        self._results_changed[run_id].set()

    def read_results(self, run_id: str, last_id: str = "0") -> AsyncGenerator[StreamEntry, None]:
        return self._tail(self._results, self._results_changed, run_id, last_id)

    async def append_event(self, run_id: str, envelope: Envelope) -> str:
        entry_id = self._append(self._events, run_id, envelope)
        self._events_changed[run_id].set()
        return entry_id

    def read_events(self, run_id: str, last_id: str = "0") -> AsyncGenerator[StreamEntry, None]:
        return self._tail(self._events, self._events_changed, run_id, last_id)

    @staticmethod
    def _append(streams: dict[str, list[StreamEntry]], run_id: str, envelope: Envelope) -> str:
        entries = streams[run_id]
        entry_id = str(len(entries) + 1)
        entries.append((entry_id, envelope))
        return entry_id

    async def _pump(self, queue: asyncio.Queue[Envelope]) -> AsyncGenerator[Envelope, None]:
        while True:
            yield await queue.get()

    async def _tail(
        self,
        streams: dict[str, list[StreamEntry]],
        changed: dict[str, asyncio.Event],
        run_id: str,
        last_id: str,
    ) -> AsyncGenerator[StreamEntry, None]:
        if not last_id.isdigit():
            raise BrokerError(f"非法的流位置: {last_id}")
        position = int(last_id)
        changed_event = changed[run_id]
        while True:
            entries = streams[run_id]
            while position < len(entries):
                yield entries[position]
                position += 1
            changed_event.clear()
            if len(streams[run_id]) > position:
                continue
            await changed_event.wait()
