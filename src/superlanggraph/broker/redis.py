"""Redis Stream broker：core 协议的持久化实现。

- 任务与控制流走 consumer group + PEL，支撑至少一次投递；
- 结果与事件按 run 分流；事件写入带 MAXLEN 上限；
- 租约续期与故障认领（XAUTOCLAIM）在构建顺序第 3/7 步补齐。

redis-py 的响应类型较为宽松，本模块在私有适配方法里用 cast 显式固定其运行时
形状（decode_responses=False 时键值为 bytes），类型噪音不外溢到服务层。
"""

import uuid
from collections.abc import AsyncGenerator
from typing import Any, cast

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from superlanggraph.core.envelope import Envelope
from superlanggraph.core.errors import BrokerError
from superlanggraph.core.settings import BrokerSettings

_TAIL_BLOCK_MS = 1_000
"""读尾流时的阻塞间隔：无新数据时让出事件循环，不空转。"""

type StreamFields = dict[bytes, bytes]
"""单条流条目的字段集。"""

type XReadReply = list[tuple[bytes, list[tuple[bytes, StreamFields]]]]
"""XREAD / XREADGROUP 的运行时响应形状。"""


def _as_str(value: str | bytes) -> str:
    return value if isinstance(value, str) else value.decode()


class RedisBroker:
    """实现 core.protocols 全部接口；Redis 客户端由组合根注入并持有。"""

    def __init__(self, settings: BrokerSettings, client: Redis) -> None:
        self._settings = settings
        self._client = client
        self._consumer = uuid.uuid4().hex
        self._groups_ready: set[str] = set()

    async def publish_control(self, envelope: Envelope) -> None:
        await self._xadd(self._control_key, envelope)

    def consume_control(self) -> AsyncGenerator[Envelope, None]:
        return self._consume_control()

    async def enqueue_task(self, envelope: Envelope) -> None:
        await self._xadd(self._task_key, envelope)

    async def dequeue_task(self, lease_seconds: float) -> tuple[str, Envelope] | None:
        await self._ensure_group(self._task_key)
        # 长BLOCK会与 redis-py 的读超时交互，改为 ≤5 秒的轮询节奏。
        block_ms = max(min(int(lease_seconds * 1000), 5_000), 10)
        entries = await self._read_group_page(self._task_key, block_ms)
        if not entries:
            return None
        entry_id, raw = entries[0]
        return entry_id, Envelope.from_bytes(raw)

    async def ack_task(self, receipt: str) -> None:
        acked = await self._client.xack(self._task_key, self._settings.consumer_group, receipt)
        if acked == 0:
            raise BrokerError(f"未知任务凭据: {receipt}")

    async def reclaim_stale(self, min_idle_ms: int) -> list[tuple[str, Envelope]]:
        """XAUTOCLAIM：认领 worker 死亡遗留的停滞任务（含字段解析）。"""
        reply = await self._client.xautoclaim(
            self._task_key,
            self._settings.consumer_group,
            self._consumer,
            min_idle_time=min_idle_ms,
            start_id="0-0",
            count=10,
        )
        entries = cast("list[Any]", reply[1] if len(reply) >= 2 else [])
        out: list[tuple[str, Envelope]] = []
        for entry in entries:
            entry_id, fields = cast("tuple[Any, Any]", entry)
            fields_map = cast("dict[bytes, bytes]", fields)
            raw = fields_map.get(b"data")
            if raw is None:
                continue
            out.append((_as_str(entry_id), Envelope.from_bytes(raw)))
        return out

    async def publish_result(self, run_id: str, envelope: Envelope) -> None:
        await self._xadd(self._run_key("results", run_id), envelope)

    def read_results(
        self, run_id: str, last_id: str = "0"
    ) -> AsyncGenerator[tuple[str, Envelope], None]:
        return self._tail(self._run_key("results", run_id), last_id)

    async def append_event(self, run_id: str, envelope: Envelope) -> str:
        return await self._xadd(
            self._run_key("events", run_id),
            envelope,
            maxlen=self._settings.event_maxlen,
        )

    def read_events(
        self, run_id: str, last_id: str = "0"
    ) -> AsyncGenerator[tuple[str, Envelope], None]:
        return self._tail(self._run_key("events", run_id), last_id)

    @property
    def _control_key(self) -> str:
        return f"{self._settings.stream_prefix}:ctrl"

    @property
    def _task_key(self) -> str:
        return f"{self._settings.stream_prefix}:tasks"

    def _run_key(self, kind: str, run_id: str) -> str:
        return f"{self._settings.stream_prefix}:{kind}:{run_id}"

    async def _xadd(self, key: str, envelope: Envelope, maxlen: int | None = None) -> str:
        fields = {"data": envelope.to_bytes().decode("utf-8")}
        if maxlen is None:
            # redis-py 的 xadd 用未约束 TypeVar 声明 fields，任何具体 dict 都无法满足
            # 其不变性（存根怪癖）；运行时形状正确，仅豁免此行的该条检查。
            entry_id = await self._client.xadd(key, fields)  # pyright: ignore[reportArgumentType]
        else:
            entry_id = await self._client.xadd(key, fields, maxlen=maxlen)  # pyright: ignore[reportArgumentType]
        return _as_str(entry_id)

    async def _ensure_group(self, key: str) -> None:
        """创建 consumer group；并发下的 BUSYGROUP 属预期结果，不算错误。"""
        if key in self._groups_ready:
            return
        try:
            await self._client.xgroup_create(
                key, self._settings.consumer_group, id="0", mkstream=True
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise BrokerError(f"创建 consumer group 失败: {key}") from exc
        self._groups_ready.add(key)

    async def _read_group_page(self, key: str, block_ms: int) -> list[tuple[str, bytes]]:
        """从 consumer group 领取至多一条未投递消息，返回 (条目 ID, 载荷)。"""
        reply = cast(
            "XReadReply | None",
            await self._client.xreadgroup(
                self._settings.consumer_group,
                self._consumer,
                {key: ">"},
                count=1,
                block=block_ms,
            ),
        )
        entries: list[tuple[str, bytes]] = []
        for _stream, stream_entries in reply or []:
            for entry_id, fields in stream_entries:
                raw = fields.get(b"data")
                if raw is None:
                    raise BrokerError(f"流条目缺少 data 字段: {key}")
                entries.append((_as_str(entry_id), raw))
        return entries

    async def _consume_control(self) -> AsyncGenerator[Envelope, None]:
        group = self._settings.consumer_group
        await self._ensure_group(self._control_key)
        while True:
            page = await self._read_group_page(self._control_key, _TAIL_BLOCK_MS)
            for entry_id, raw in page:
                await self._client.xack(self._control_key, group, entry_id)
                # TODO(构建顺序第 3 步)：控制消息应处理完成后再确认。
                yield Envelope.from_bytes(raw)

    async def _tail(self, key: str, last_id: str) -> AsyncGenerator[tuple[str, Envelope], None]:
        position = last_id
        while True:
            reply = cast(
                "XReadReply | None",
                await self._client.xread({key: position}, count=64, block=_TAIL_BLOCK_MS),
            )
            for _stream, stream_entries in reply or []:
                for entry_id, fields in stream_entries:
                    raw = fields.get(b"data")
                    if raw is None:
                        raise BrokerError(f"流条目缺少 data 字段: {key}")
                    position = _as_str(entry_id)
                    yield position, Envelope.from_bytes(raw)
