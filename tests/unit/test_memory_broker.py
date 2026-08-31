"""MemoryBroker 行为与协议符合性测试。"""

import asyncio
from collections.abc import AsyncGenerator

import pytest

from superlanggraph.broker import MemoryBroker
from superlanggraph.core.envelope import Envelope
from superlanggraph.core.errors import BrokerError
from superlanggraph.core.protocols import ControlBus, EventBus, ResultBus, TaskQueue


def _task(task_id: str) -> Envelope:
    return Envelope(type="task", run_id="run-1", payload={}, task_id=task_id)


def _event(index: int) -> Envelope:
    return Envelope(type="event", run_id="run-1", payload={"i": index})


def test_memory_broker_conforms_to_protocols() -> None:
    control: ControlBus = MemoryBroker()
    tasks: TaskQueue = MemoryBroker()
    results: ResultBus = MemoryBroker()
    events: EventBus = MemoryBroker()
    assert control is not None
    assert tasks is not None
    assert results is not None
    assert events is not None


async def test_task_roundtrip_and_ack() -> None:
    broker = MemoryBroker()
    await broker.enqueue_task(_task("t1"))

    lease = await broker.dequeue_task(lease_seconds=1.0)
    assert lease is not None
    receipt, got = lease
    assert got.task_id == "t1"

    await broker.ack_task(receipt)
    assert await broker.dequeue_task(lease_seconds=0.01) is None


async def test_unknown_receipt_is_rejected() -> None:
    broker = MemoryBroker()
    with pytest.raises(BrokerError):
        await broker.ack_task("no-such-receipt")


async def test_event_tail_and_resume() -> None:
    broker = MemoryBroker()
    event_one = _event(1)
    event_two = _event(2)
    first_id = await broker.append_event("run-1", event_one)
    second_id = await broker.append_event("run-1", event_two)

    iterator: AsyncGenerator[tuple[str, Envelope], None] = broker.read_events("run-1")
    entry_one = await iterator.__anext__()
    entry_two = await iterator.__anext__()
    assert entry_one == (first_id, event_one)
    assert entry_two == (second_id, event_two)
    await iterator.aclose()

    resumed: AsyncGenerator[tuple[str, Envelope], None] = broker.read_events(
        "run-1", last_id=first_id
    )
    resumed_entry = await resumed.__anext__()
    assert resumed_entry == (second_id, event_two)
    await resumed.aclose()


async def test_result_stream_notifies_waiters() -> None:
    broker = MemoryBroker()

    async def publisher() -> None:
        await broker.publish_result("run-1", Envelope(type="result", run_id="run-1", payload={}))

    iterator = broker.read_results("run-1")
    publish_task = asyncio.create_task(publisher())
    entry = await iterator.__anext__()
    assert entry[1].type == "result"
    await iterator.aclose()
    await publish_task
