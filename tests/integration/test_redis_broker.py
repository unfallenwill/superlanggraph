# pyright: reportUnknownMemberType=false
# redis-py 部分方法签名含未类型化 kwargs，集成测试作为类型边界放宽此一条；src/ 不受影响。

"""RedisBroker 集成测试：需要本地 Redis（docker compose up -d），不可达时自动跳过。"""

import uuid
from collections.abc import AsyncIterator

import pytest
from redis.asyncio import Redis

from superlanggraph.broker import RedisBroker
from superlanggraph.core.envelope import Envelope
from superlanggraph.core.settings import BrokerSettings


async def _can_reach_redis(url: str) -> bool:
    client = Redis.from_url(url, socket_connect_timeout=0.5, socket_timeout=0.5)
    try:
        return bool(await client.ping())
    except Exception:
        return False
    finally:
        await client.aclose()


@pytest.fixture
async def broker() -> AsyncIterator[RedisBroker]:
    settings = BrokerSettings(stream_prefix=f"slg-test-{uuid.uuid4().hex[:8]}")
    if not await _can_reach_redis(settings.redis_url):
        pytest.skip("需要本地 Redis：docker compose up -d")
    client = Redis.from_url(settings.redis_url)
    yield RedisBroker(settings, client)
    await client.aclose()


def _task(task_id: str) -> Envelope:
    return Envelope(type="task", run_id="run-it", payload={}, task_id=task_id)


async def test_task_roundtrip_and_ack(broker: RedisBroker) -> None:
    await broker.enqueue_task(_task("t1"))

    lease = await broker.dequeue_task(lease_seconds=2.0)
    assert lease is not None
    receipt, got = lease
    assert got.task_id == "t1"

    await broker.ack_task(receipt)


async def test_event_tail_returns_history_in_order(broker: RedisBroker) -> None:
    run_id = f"run-{uuid.uuid4().hex[:8]}"
    first_id = await broker.append_event(
        run_id, Envelope(type="event", run_id=run_id, payload={"i": 1})
    )
    second_id = await broker.append_event(
        run_id, Envelope(type="event", run_id=run_id, payload={"i": 2})
    )

    iterator = broker.read_events(run_id)
    entries = [await iterator.__anext__(), await iterator.__anext__()]
    await iterator.aclose()

    assert [entry_id for entry_id, _ in entries] == [first_id, second_id]
    assert [env.payload["i"] for _, env in entries] == [1, 2]
