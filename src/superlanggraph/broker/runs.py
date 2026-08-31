"""run 记录存储：Redis 上的 run 生命周期真相源。

run 的所有字段（状态、时间戳、线程归属）都存 Redis，任何角色随时可被杀，
状态不丢。key 布局：``{prefix}:run:{run_id}`` Hash、
``{prefix}:thread-runs:{thread_id}`` ZSet（时间序索引）、
``{prefix}:thread-active:{thread_id}`` String（活动 run 标记）、
``{prefix}:hb:{run_id}`` String + TTL（driver 心跳，守尸据此识别孤儿）、
``{prefix}:recover:{run_id}`` String + TTL（守尸认领锁，防多实例重复恢复）。
"""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

from redis.asyncio import Redis

RUN_STATUSES = ("pending", "running", "error", "success", "timeout", "interrupted")


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def new_run_id() -> str:
    return str(uuid.uuid4())


class RunStore:
    """run 记录的 Redis 实现；组合根注入。"""

    def __init__(self, client: Redis, prefix: str = "slg") -> None:
        self._client = client
        self._prefix = prefix

    async def create(self, record: dict[str, Any]) -> dict[str, Any]:
        run_id = record.get("run_id") or new_run_id()
        full = {
            "run_id": run_id,
            "thread_id": record["thread_id"],
            "assistant_id": record["assistant_id"],
            "graph_id": record["graph_id"],
            "status": record.get("status", "pending"),
            "metadata": json.dumps(record.get("metadata") or {}),
            "multitask_strategy": record.get("multitask_strategy", "reject"),
            "input": json.dumps(record.get("input")) if record.get("input") is not None else "",
            "command": json.dumps(record.get("command")) if record.get("command") else "",
            "created_at": record.get("created_at") or now_iso(),
            "updated_at": now_iso(),
        }
        pipeline = self._client.pipeline()
        pipeline.hset(self._run_key(run_id), mapping=full)  # type: ignore[arg-type]
        pipeline.zadd(
            self._thread_runs_key(str(record["thread_id"])),
            {run_id: _ts_score(full["created_at"])},
        )
        await pipeline.execute()
        return full

    async def get(self, run_id: str) -> dict[str, Any] | None:
        raw = await self._client.hgetall(self._run_key(run_id))
        if not raw:
            return None
        return _decode(raw)

    async def update(self, run_id: str, **fields: Any) -> None:
        fields["updated_at"] = now_iso()
        await self._client.hset(self._run_key(run_id), mapping=_encode(fields))  # type: ignore[arg-type]

    async def delete(self, run_id: str) -> dict[str, Any] | None:
        record = await self.get(run_id)
        if record is None:
            return None
        pipeline = self._client.pipeline()
        pipeline.delete(self._run_key(run_id))
        pipeline.zrem(self._thread_runs_key(str(record["thread_id"])), run_id)
        await pipeline.execute()
        return record

    async def list_for_thread(
        self,
        thread_id: str,
        *,
        limit: int = 10,
        offset: int = 0,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        run_ids = await self._client.zrevrange(  # pyright: ignore[reportUnknownMemberType]
            self._thread_runs_key(thread_id), offset, offset + limit - 1
        )
        records: list[dict[str, Any]] = []
        for raw_id in run_ids:
            run_id = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
            record = await self.get(run_id)
            if record is not None and (status is None or record["status"] == status):
                records.append(record)
        return records

    async def set_active(self, thread_id: str, run_id: str) -> None:
        await self._client.set(self._active_key(thread_id), run_id)

    async def get_active(self, thread_id: str) -> str | None:
        raw = await self._client.get(self._active_key(thread_id))
        if raw is None:
            return None
        return raw.decode() if isinstance(raw, bytes) else str(raw)

    async def clear_active(self, thread_id: str, run_id: str) -> None:
        current = await self.get_active(thread_id)
        if current == run_id:
            await self._client.delete(self._active_key(thread_id))

    async def count_thread_runs(self, thread_id: str, status: str | None = None) -> int:
        if status is None:
            return int(await self._client.zcard(self._thread_runs_key(thread_id)))
        records = await self.list_for_thread(thread_id, limit=1000, status=status)
        return len(records)

    def _run_key(self, run_id: str) -> str:
        return f"{self._prefix}:run:{run_id}"

    def _thread_runs_key(self, thread_id: str) -> str:
        return f"{self._prefix}:thread-runs:{thread_id}"

    def _active_key(self, thread_id: str) -> str:
        return f"{self._prefix}:thread-active:{thread_id}"

    def _hb_key(self, run_id: str) -> str:
        return f"{self._prefix}:hb:{run_id}"

    def _recover_key(self, run_id: str) -> str:
        return f"{self._prefix}:recover:{run_id}"

    # ---- 心跳与守尸 ----

    async def heartbeat(self, run_id: str, ttl: int = 15) -> None:
        """driver 心跳：周期续期，死亡后自然过期。"""
        await self._client.set(self._hb_key(run_id), "1", ex=ttl)

    async def heartbeat_alive(self, run_id: str) -> bool:
        return bool(await self._client.exists(self._hb_key(run_id)))

    async def try_claim_recovery(self, run_id: str, ttl: int = 30) -> bool:
        """守尸认领锁：多实例只有一个能认领同一个孤儿 run。"""
        claimed = await self._client.set(self._recover_key(run_id), "1", nx=True, ex=ttl)
        return bool(claimed)

    async def list_active_runs(self) -> AsyncIterator[tuple[str, str]]:
        """扫描全部活动 run，返回 (thread_id, run_id)。"""
        pattern = f"{self._prefix}:thread-active:*"
        async for raw_key in self._client.scan_iter(match=pattern):  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            key_text = _decode_key(raw_key)
            thread_id = key_text.rsplit(":", 1)[-1]
            raw_id = await self._client.get(key_text)
            if raw_id is None:
                continue
            run_id = _decode_key(raw_id)
            yield thread_id, run_id


def _decode_key(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _ts_score(iso: str) -> float:
    return datetime.fromisoformat(iso).timestamp()


def _encode(fields: dict[str, Any]) -> dict[str, str]:
    return {
        key: value if isinstance(value, str) else json.dumps(value) for key, value in fields.items()
    }


def _decode(raw: dict[Any, Any]) -> dict[str, Any]:
    record: dict[str, Any] = {}
    for key, value in raw.items():
        key_text = key.decode() if isinstance(key, bytes) else str(key)
        value_text = value.decode() if isinstance(value, bytes) else str(value)
        if key_text in ("metadata", "input", "command"):
            record[key_text] = (
                json.loads(value_text) if value_text else ({} if key_text == "metadata" else None)
            )
        else:
            record[key_text] = value_text
    return record
