"""SSE 输出：事件流到 HTTP 流的桥。

帧格式为标准 SSE（``id:`` / ``event:`` / ``data:``）；事件名 = stream mode，
subgraph 命名空间以 ``|`` 追加（v1 协议约定，见 docs/api-contract.md）。
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any, cast

from superlanggraph.core.protocols import EventBus


def sse_frame(event: str, data_text: str, event_id: str | None = None) -> str:
    """构造一帧 SSE 文本。"""
    prefix = f"id: {event_id}\n" if event_id else ""
    return f"{prefix}event: {event}\ndata: {data_text}\n\n"


async def tail_events(
    events: EventBus,
    key: str,
    *,
    requested_modes: set[str],
    last_event_id: str | None,
    metadata_event: dict[str, Any] | None = None,
) -> AsyncIterator[str]:
    """尾随读取事件流并投影为 SSE 帧；``end`` 事件终止流。"""
    if metadata_event is not None:
        yield sse_frame("metadata", json.dumps(metadata_event))
    async for entry_id, envelope in events.read_events(key, last_id=last_event_id or "0"):
        payload = envelope.payload
        event = str(payload.get("event", ""))
        if event == "end":
            yield sse_frame("end", str(payload.get("data") or "{}"))
            return
        base = event.split("|", maxsplit=1)[0]
        if base not in requested_modes and event not in requested_modes:
            continue
        ns = cast("list[Any]", payload.get("ns") or [])
        name = "|".join([base, *[str(part) for part in ns]]) if ns else base
        yield sse_frame(name, str(payload.get("data") or "null"), event_id=entry_id)
