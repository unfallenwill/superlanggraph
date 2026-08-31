"""定时调度器：cron 表达式到 run 的桥。

cron 记录存 Redis（``{prefix}:crons`` Hash，field=cron_id），调度协程每秒
扫描到点任务并创建 run（走与 API 完全相同的创建路径，保证语义一致）。
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from croniter import croniter
from redis.asyncio import Redis


@dataclass(frozen=True, slots=True)
class CronRecord:
    """一条定时任务。"""

    cron_id: str
    assistant_id: str
    graph_id: str
    thread_id: str | None
    schedule: str
    input: dict[str, Any] = field(default_factory=dict[str, Any])
    metadata: dict[str, Any] = field(default_factory=dict[str, Any])
    enabled: bool = True
    next_run_at: str | None = None


def next_after(schedule: str, base: datetime) -> datetime:
    """计算 cron 表达式的下一次触发时间（UTC）。"""
    return croniter(schedule, base).get_next(datetime)  # type: ignore[return-value]


class CronTable:
    """cron 记录的 Redis 存储。"""

    def __init__(self, client: Redis, prefix: str = "slg") -> None:
        self._client = client
        self._prefix = prefix

    async def create(self, record: CronRecord) -> CronRecord:
        await self._client.hset(self._key(), record.cron_id, json.dumps(_to_json(record)))
        return record

    async def update(self, cron_id: str, **fields: Any) -> CronRecord | None:
        existing = await self.get(cron_id)
        if existing is None:
            return None
        data = _to_json(existing)
        for key, value in fields.items():
            data[key] = value
        updated = _from_json(data)
        await self.create(updated)
        return updated

    async def delete(self, cron_id: str) -> bool:
        return bool(await self._client.hdel(self._key(), cron_id))

    async def get(self, cron_id: str) -> CronRecord | None:
        raw = await self._client.hget(self._key(), cron_id)
        if raw is None:
            return None
        return _from_json(json.loads(_decode(raw)))

    async def all(self) -> list[CronRecord]:
        raw = await self._client.hgetall(self._key())
        records = [_from_json(json.loads(_decode(value))) for value in raw.values()]
        return [record for record in records if record.enabled]

    def _key(self) -> str:
        return f"{self._prefix}:crons"


def _to_json(record: CronRecord) -> dict[str, Any]:
    data = {
        "cron_id": record.cron_id,
        "assistant_id": record.assistant_id,
        "graph_id": record.graph_id,
        "thread_id": record.thread_id,
        "schedule": record.schedule,
        "input": record.input,
        "metadata": record.metadata,
        "enabled": record.enabled,
        "next_run_at": record.next_run_at,
    }
    return data


def _from_json(data: dict[str, Any]) -> CronRecord:
    return CronRecord(
        cron_id=str(data["cron_id"]),
        assistant_id=str(data["assistant_id"]),
        graph_id=str(data["graph_id"]),
        thread_id=data.get("thread_id"),
        schedule=str(data["schedule"]),
        input=data.get("input") or {},
        metadata=data.get("metadata") or {},
        enabled=bool(data.get("enabled", True)),
        next_run_at=data.get("next_run_at"),
    )


def _decode(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode()
    if isinstance(value, str):
        return json.loads(value)
    return value


def new_cron_id() -> str:
    return str(uuid.uuid4())


def utc_now() -> datetime:
    return datetime.now(UTC)
