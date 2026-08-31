"""跨进程消息封套：所有经由 broker 传输的消息共用一个显式信封。

设计约定见 docs/architecture.md 第 4 节：全序由 Redis Stream 条目 ID 承载，
封套只携带业务标识与载荷；payload 是 JSON 对象，位于类型边界的 Any 区域。
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Literal, cast

from superlanggraph.core.errors import SerializationError

MsgType = Literal["control", "task", "result", "event"]
"""消息类别：控制指令 / 节点任务 / 执行结果 / 流式事件。"""

ENVELOPE_VERSION = 1

_VALID_MSG_TYPES: tuple[str, ...] = ("control", "task", "result", "event")


@dataclass(frozen=True, slots=True)
class Envelope:
    """消息封套。不可变，序列化即真相，通道上不存在第二种消息形状。"""

    type: MsgType
    run_id: str
    payload: dict[str, Any]
    task_id: str | None = None
    ns: tuple[str, ...] = ()
    trace: dict[str, str] | None = None
    version: int = ENVELOPE_VERSION
    ts: float = field(default_factory=time.time)

    def to_bytes(self) -> bytes:
        """序列化为 JSON 字节串。"""
        return json.dumps(asdict(self), ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    @classmethod
    def from_bytes(cls, raw: bytes) -> Envelope:
        """从 JSON 字节串反序列化；任何不符合封套形状的输入都显式报错。"""
        try:
            data = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SerializationError("封套不是合法 JSON") from exc
        if not isinstance(data, dict):
            raise SerializationError("封套必须是 JSON 对象")
        # isinstance 收窄后键类型仍是 Unknown，这里显式固定 JSON 边界的形状。
        fields = cast("dict[str, Any]", data)
        missing = [key for key in ("type", "run_id", "payload") if key not in fields]
        if missing:
            raise SerializationError(f"封套缺少必填字段: {missing}")
        if fields["type"] not in _VALID_MSG_TYPES:
            raise SerializationError(f"未知消息类别: {fields['type']}")
        if not isinstance(fields["payload"], dict):
            raise SerializationError("payload 必须是 JSON 对象")
        payload = cast("dict[str, Any]", fields["payload"])
        ns_value = fields.get("ns", ())
        ns: tuple[str, ...] = ()
        if isinstance(ns_value, list | tuple):
            ns = tuple(cast("list[str]", ns_value))
        return cls(
            type=fields["type"],
            run_id=fields["run_id"],
            payload=payload,
            task_id=fields.get("task_id"),
            ns=ns,
            trace=fields.get("trace"),
            version=fields.get("version", ENVELOPE_VERSION),
            ts=fields.get("ts", 0.0),
        )
