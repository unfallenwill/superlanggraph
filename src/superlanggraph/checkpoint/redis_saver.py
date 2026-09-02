"""Redis 版 BaseCheckpointSaver：与官方 MemorySaver 语义逐条对齐。

存储模型（与 MemorySaver 三张表一一对应，键按 thread 域化）：

- ``{prefix}:storage:{thread_id}``  Hash  field = ``{ns}\\x00{checkpoint_id}``
  value = JSON ``[cp_type, cp_b64, md_type, md_b64, parent_id]``
- ``{prefix}:blobs:{thread_id}``    Hash  field = ``{ns}\\x00{channel}\\x00{version}``
  value = JSON ``[type, b64]``，空值写 ``["empty", ""]``
- ``{prefix}:writes:{thread_id}``   Hash  field = ``{ns}\\x00{ckpt_id}\\x00{task_id}\\x00{idx}``
  value = JSON ``[task_id, channel, type, b64, task_path]``
- ``{prefix}:refs:{thread_id}``     Hash  field = 同 blobs field，value = 引用计数

CoW 语义：``aput`` 只写 ``new_versions`` 涉及的 blob，未变化 channel 由
checkpoint 清单引用既有 blob 复用。blob 生命周期由引用计数管理：checkpoint
写入时对其引用的全部 blob 递增，删除时（回滚）经 Lua 原子扣减，减到零即删；
``gc_thread`` 提供 mark-sweep 兜底修正计数漂移。失效方向是泄漏而非误删。

checkpoint_id 字典序即时间序（官方约定），排序语义与 MemorySaver 一致。
仅实现异步接口（agraph 运行时只用 a* 系列），同步接口显式拒绝。
"""

from __future__ import annotations

import base64
import builtins
import json
from collections.abc import AsyncIterator, Iterator, Sequence
from typing import Any, Final, cast

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    PendingWrite,
    get_checkpoint_id,
    get_checkpoint_metadata,
)
from redis.asyncio import Redis

_SEP: Final = "\x00"
_EMPTY: Final = "empty"

# 引用扣减 + 归零即删必须原子：先扣后删分两步会在并发写入时误删活 blob。
_DECREF_LUA = """
local next = redis.call('HINCRBY', KEYS[1], ARGV[1], ARGV[2])
if next <= 0 then
    redis.call('HDEL', KEYS[1], ARGV[1])
    redis.call('HDEL', KEYS[2], ARGV[1])
end
return next
"""


def _b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def _unb64(text: str) -> bytes:
    return base64.b64decode(text.encode("ascii"))


def _configurable(config: RunnableConfig) -> dict[str, Any]:
    """configurable 在 langchain-core 1.x 中声明为闭合 TypedDict，
    显式展开为通用字典以承载 saver 协议的自定义键（thread_id 等）。"""
    return config.get("configurable", {})


class RedisSaver(BaseCheckpointSaver[str]):
    """Redis 持久化检查点保存器；Redis 客户端由组合根注入并持有。"""

    def __init__(self, client: Redis, prefix: str = "slg:ckpt") -> None:
        super().__init__()
        self._client = client
        self._prefix = prefix

    # ---- 异步接口（agraph / server 全部走这里） ----

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        conf = _configurable(config)
        thread_id: str = conf["thread_id"]
        checkpoint_ns: str = conf.get("checkpoint_ns", "")
        checkpoint_id = get_checkpoint_id(config)
        entries = await self._load_entries(thread_id, checkpoint_ns)
        if checkpoint_id:
            entry = entries.get(checkpoint_id)
            return await self._build_tuple(thread_id, checkpoint_ns, checkpoint_id, entry, config)
        if not entries:
            return None
        latest = max(entries)
        return await self._build_tuple(thread_id, checkpoint_ns, latest, entries[latest], None)

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        if config is not None:
            conf = _configurable(config)
            thread_id: str = conf["thread_id"]
            config_ns: str | None = conf.get("checkpoint_ns")
            threads = [thread_id]
        else:
            config_ns = None
            threads = await self._scan_threads()
        before_id = get_checkpoint_id(before) if before else None
        for tid in threads:
            namespaces = await self._list_namespaces(tid, config_ns)
            for checkpoint_ns in namespaces:
                entries = await self._load_entries(tid, checkpoint_ns)
                for checkpoint_id in sorted(entries, reverse=True):
                    if before_id and checkpoint_id >= before_id:
                        continue
                    entry = entries[checkpoint_id]
                    metadata = self.serde.loads_typed((entry["md_type"], _unb64(entry["md_b64"])))
                    if filter and not all(
                        query == metadata.get(key) for key, query in filter.items()
                    ):
                        continue
                    if limit is not None:
                        if limit <= 0:
                            return
                        limit -= 1
                    tup = await self._build_tuple(tid, checkpoint_ns, checkpoint_id, entry, None)
                    if tup is not None:
                        yield tup

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        conf = _configurable(config)
        thread_id: str = conf["thread_id"]
        checkpoint_ns: str = conf["checkpoint_ns"]
        values = checkpoint["channel_values"]
        inner = {k: v for k, v in checkpoint.items() if k != "channel_values"}
        cp_typed = self.serde.dumps_typed(cast("dict[str, Any]", inner))
        md_typed = self.serde.dumps_typed(get_checkpoint_metadata(config, metadata))
        entry = [
            cp_typed[0],
            _b64(cp_typed[1]),
            md_typed[0],
            _b64(md_typed[1]),
            conf.get("checkpoint_id"),
        ]
        # 单事务落盘：blob 写入、引用计数递增（对所有被引用版本，含从父
        # checkpoint 带过来的未变化 channel）与清单写入同生共死。
        refs_key = self._refs_key(thread_id)
        async with self._client.pipeline(transaction=True) as pipe:
            for channel, version in new_versions.items():
                typed = (
                    self.serde.dumps_typed(values[channel]) if channel in values else (_EMPTY, b"")
                )
                pipe.hset(
                    self._blobs_key(thread_id),
                    self._blob_field(checkpoint_ns, channel, str(version)),
                    json.dumps([typed[0], _b64(typed[1])]),
                )
            for channel, version in checkpoint["channel_versions"].items():
                pipe.hincrby(refs_key, self._blob_field(checkpoint_ns, channel, str(version)), 1)
            pipe.hset(
                self._storage_key(thread_id),
                self._entry_field(checkpoint_ns, checkpoint["id"]),
                json.dumps(entry),
            )
            await pipe.execute()
        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        conf = _configurable(config)
        thread_id: str = conf["thread_id"]
        checkpoint_ns: str = conf.get("checkpoint_ns", "")
        checkpoint_id: str = conf["checkpoint_id"]
        existing = await self._load_writes(thread_id, checkpoint_ns, checkpoint_id)
        seen = {(w["task_id"], w["write_idx"]) for w in existing}
        for idx, (channel, value) in enumerate(writes):
            write_idx = WRITES_IDX_MAP.get(channel, idx)
            if write_idx >= 0 and (task_id, write_idx) in seen:
                continue
            typed = self.serde.dumps_typed(value)
            payload = json.dumps([task_id, channel, typed[0], _b64(typed[1]), task_path])
            field = self._writes_field(checkpoint_ns, checkpoint_id, task_id, write_idx)
            await self._client.hset(self._writes_key(thread_id), field, payload)

    async def adelete_thread(self, thread_id: str) -> None:
        await self._client.delete(
            self._storage_key(thread_id),
            self._blobs_key(thread_id),
            self._writes_key(thread_id),
            self._refs_key(thread_id),
        )

    async def adelete_checkpoints_after(
        self,
        thread_id: str,
        base_checkpoint_id: str | None,
        checkpoint_ns: str = "",
    ) -> int:
        """精确回滚：删除 base 之后（字典序 > base）的全部 checkpoint 与写入。

        语义：线程状态回到 base checkpoint 的样子——这正是 run 级 rollback
        需要的效果。base 为 None 表示空线程起点，即删除该 ns 的全部条目。
        被删 checkpoint 对 blob 的引用经 Lua 原子扣减，减到零的 blob 随即
        删除（已无任何清单引用它）；仍被存活清单引用的共享 blob 保留。
        返回删除的 checkpoint 条数。
        """
        entries = await self._load_entries(thread_id, checkpoint_ns)
        doomed = [
            (checkpoint_id, entry)
            for checkpoint_id, entry in entries.items()
            if base_checkpoint_id is None or checkpoint_id > base_checkpoint_id
        ]
        if not doomed:
            return 0
        removed = len(doomed)
        doomed_ids = {checkpoint_id for checkpoint_id, _entry in doomed}

        writes_raw = await self._client.hgetall(self._writes_key(thread_id))
        write_prefix = f"{checkpoint_ns}{_SEP}"
        write_fields_by_ckpt: dict[str, list[str]] = {}
        for field, _value in writes_raw.items():
            field_text = self._decode_key(field)
            if not field_text.startswith(write_prefix):
                continue
            # field = {ns}\x00{ckpt_id}\x00{task_id}\x00{idx}
            checkpoint_id = field_text[len(write_prefix) :].split(_SEP)[0]
            if checkpoint_id in doomed_ids:
                write_fields_by_ckpt.setdefault(checkpoint_id, []).append(field_text)

        refs_key = self._refs_key(thread_id)
        blobs_key = self._blobs_key(thread_id)
        async with self._client.pipeline(transaction=True) as pipe:
            for checkpoint_id, entry in doomed:
                pipe.hdel(
                    self._storage_key(thread_id),
                    self._entry_field(checkpoint_ns, checkpoint_id),
                )
                for field_text in write_fields_by_ckpt.get(checkpoint_id, []):
                    pipe.hdel(self._writes_key(thread_id), field_text)
                checkpoint = cast(
                    "Checkpoint",
                    self.serde.loads_typed((entry["cp_type"], _unb64(entry["cp_b64"]))),
                )
                for channel, version in checkpoint["channel_versions"].items():
                    field = self._blob_field(checkpoint_ns, channel, str(version))
                    pipe.eval(_DECREF_LUA, 2, refs_key, blobs_key, field, -1)
            await pipe.execute()
        return removed

    async def gc_thread(self, thread_id: str, checkpoint_ns: str | None = None) -> int:
        """mark-sweep 兜底：重建引用计数并删除无任何存活清单引用的孤儿 blob。

        正常路径靠写入/回滚时的引用计数即时回收；本方法用于修正计数漂移
        或清理引入计数机制之前遗留的孤儿。返回删除的 blob 字段数。
        """
        referenced: dict[str, int] = {}
        for ns in await self._list_namespaces(thread_id, checkpoint_ns):
            entries = await self._load_entries(thread_id, ns)
            for entry in entries.values():
                checkpoint = cast(
                    "Checkpoint",
                    self.serde.loads_typed((entry["cp_type"], _unb64(entry["cp_b64"]))),
                )
                for channel, version in checkpoint["channel_versions"].items():
                    field = self._blob_field(ns, channel, str(version))
                    referenced[field] = referenced.get(field, 0) + 1

        blobs_raw = await self._client.hgetall(self._blobs_key(thread_id))
        orphans = [
            self._decode_key(field)
            for field in blobs_raw
            if self._decode_key(field) not in referenced
        ]
        refs_raw = await self._client.hgetall(self._refs_key(thread_id))
        stale_counts = [
            self._decode_key(field)
            for field in refs_raw
            if self._decode_key(field) not in referenced
        ]
        if orphans:
            await self._client.hdel(self._blobs_key(thread_id), *orphans)
        if stale_counts:
            await self._client.hdel(self._refs_key(thread_id), *stale_counts)
        if referenced:
            await self._client.hset(
                self._refs_key(thread_id),
                mapping={field: str(count) for field, count in referenced.items()},
            )
        return len(orphans)

    # ---- 同步接口：异步运行时下显式拒绝（与官方 Async*Saver 行为一致） ----

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        raise NotImplementedError("RedisSaver 仅支持异步接口（aget_tuple 等）")

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        raise NotImplementedError("RedisSaver 仅支持异步接口（alist 等）")

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        raise NotImplementedError("RedisSaver 仅支持异步接口（aput 等）")

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        raise NotImplementedError("RedisSaver 仅支持异步接口（aput_writes 等）")

    # ---- 内部结构 ----

    def _storage_key(self, thread_id: str) -> str:
        return f"{self._prefix}:storage:{thread_id}"

    def _blobs_key(self, thread_id: str) -> str:
        return f"{self._prefix}:blobs:{thread_id}"

    def _writes_key(self, thread_id: str) -> str:
        return f"{self._prefix}:writes:{thread_id}"

    def _refs_key(self, thread_id: str) -> str:
        return f"{self._prefix}:refs:{thread_id}"

    @staticmethod
    def _entry_field(checkpoint_ns: str, checkpoint_id: str) -> str:
        return f"{checkpoint_ns}{_SEP}{checkpoint_id}"

    @staticmethod
    def _blob_field(checkpoint_ns: str, channel: str, version: str) -> str:
        return f"{checkpoint_ns}{_SEP}{channel}{_SEP}{version}"

    @staticmethod
    def _writes_field(checkpoint_ns: str, checkpoint_id: str, task_id: str, write_idx: int) -> str:
        return f"{checkpoint_ns}{_SEP}{checkpoint_id}{_SEP}{task_id}{_SEP}{write_idx}"

    @staticmethod
    def _decode_key(key: str | bytes) -> str:
        return key.decode() if isinstance(key, bytes) else str(key)

    async def _load_entries(
        self, thread_id: str, checkpoint_ns: str
    ) -> builtins.dict[str, builtins.dict[str, Any]]:
        """加载某 thread + ns 的全部 checkpoint 条目，按 field 前缀过滤。"""
        raw = await self._client.hgetall(self._storage_key(thread_id))
        prefix = f"{checkpoint_ns}{_SEP}"
        entries: dict[str, dict[str, Any]] = {}
        for field, value in raw.items():
            field_text = self._decode_key(field)
            if not field_text.startswith(prefix):
                continue
            checkpoint_id = field_text[len(prefix) :]
            parsed = cast("list[Any]", json.loads(value))
            entries[checkpoint_id] = {
                "cp_type": parsed[0],
                "cp_b64": parsed[1],
                "md_type": parsed[2],
                "md_b64": parsed[3],
                "parent_id": parsed[4],
            }
        return entries

    async def _load_writes(
        self, thread_id: str, checkpoint_ns: str, checkpoint_id: str
    ) -> builtins.list[builtins.dict[str, Any]]:
        raw = await self._client.hgetall(self._writes_key(thread_id))
        prefix = f"{checkpoint_ns}{_SEP}{checkpoint_id}{_SEP}"
        writes: list[dict[str, Any]] = []
        for field, value in raw.items():
            field_text = self._decode_key(field)
            if not field_text.startswith(prefix):
                continue
            parsed = cast("list[Any]", json.loads(value))
            tail = field_text[len(prefix) :].split(_SEP)
            writes.append(
                {
                    "task_id": parsed[0],
                    "channel": parsed[1],
                    "type": parsed[2],
                    "b64": parsed[3],
                    "task_path": parsed[4],
                    "write_idx": int(tail[-1]),
                }
            )
        return writes

    async def _build_tuple(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
        entry: builtins.dict[str, Any] | None,
        request_config: RunnableConfig | None,
    ) -> CheckpointTuple | None:
        if entry is None:
            return None
        checkpoint = cast(
            "Checkpoint",
            self.serde.loads_typed((entry["cp_type"], _unb64(entry["cp_b64"]))),
        )
        channel_values = await self._load_blobs(
            thread_id, checkpoint_ns, checkpoint["channel_versions"]
        )
        parent_id = entry["parent_id"]
        config = request_config or {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            }
        }
        writes = await self._load_writes(thread_id, checkpoint_ns, checkpoint_id)
        pending: list[PendingWrite] = [
            (
                write["task_id"],
                write["channel"],
                self.serde.loads_typed((write["type"], _unb64(write["b64"]))),
            )
            for write in writes
        ]
        return CheckpointTuple(
            config=config,
            checkpoint={
                **checkpoint,
                "channel_values": channel_values,
            },
            metadata=self.serde.loads_typed((entry["md_type"], _unb64(entry["md_b64"]))),
            parent_config=(
                {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": parent_id,
                    }
                }
                if parent_id
                else None
            ),
            pending_writes=pending,
        )

    async def _load_blobs(
        self, thread_id: str, checkpoint_ns: str, versions: ChannelVersions
    ) -> builtins.dict[str, Any]:
        channels = list(versions)
        fields = [
            self._blob_field(checkpoint_ns, channel, str(version))
            for channel, version in versions.items()
        ]
        # CoW 存储：checkpoint 只引用 (channel, version)，未变化的 channel 天然
        # 复用既有 blob。读路径一次 HMGET 批量取齐，避免逐 channel 往返。
        raws = await self._client.hmget(self._blobs_key(thread_id), fields)
        result: dict[str, Any] = {}
        for channel, raw in zip(channels, raws, strict=True):
            if raw is None:
                continue
            parsed = cast("list[Any]", json.loads(raw))
            if parsed[0] == _EMPTY:
                continue
            result[channel] = self.serde.loads_typed((parsed[0], _unb64(parsed[1])))
        return result

    async def _list_namespaces(self, thread_id: str, config_ns: str | None) -> builtins.list[str]:
        if config_ns is not None:
            return [config_ns]
        raw = await self._client.hkeys(self._storage_key(thread_id))
        namespaces = {self._decode_key(field).split(_SEP, 1)[0] for field in raw}
        return sorted(namespaces)

    async def _scan_threads(self) -> builtins.list[str]:
        pattern = f"{self._prefix}:storage:*"
        threads: list[str] = []
        # redis-py 的 scan_iter 存根部分未知，此处仅此一处放宽。
        async for raw_key in self._client.scan_iter(match=pattern):  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            key = cast("str | bytes", raw_key)
            threads.append(self._decode_key(key).rsplit(":", 1)[-1])
        return threads
