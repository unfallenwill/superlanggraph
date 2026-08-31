"""跨进程值序列化：统一使用官方 JsonPlusSerializer。

驱动器与执行器之间传输的节点输入、返回值、中断载荷都经这里编码；
形状为 ``[type, base64]`` 的 typed 值，与检查点序列化保持同一套语义。
SSE 等文本协议位使用 JSON 化（langchain 对象经 ``to_json`` 协议转换）。
"""

from __future__ import annotations

import base64
import dataclasses
import json
from typing import Any, Final

from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

_SERIALIZER: Final = JsonPlusSerializer()

TypedValue = list[str]
"""serde 编码后的形状：``[type, base64]``。"""


def dumps(value: Any) -> TypedValue:
    """把任意可序列化值编码为 ``[type, base64]``。"""
    type_tag, raw = _SERIALIZER.dumps_typed(value)
    return [type_tag, base64.b64encode(raw).decode("ascii")]


def loads(typed: TypedValue) -> Any:
    """从 ``[type, base64]`` 解码。"""
    return _SERIALIZER.loads_typed((typed[0], base64.b64decode(typed[1])))


def dumps_json_text(value: Any) -> str:
    """把值编码为 JSON 文本（用于 SSE data 等文本协议位）。"""
    return json.dumps(value, ensure_ascii=False, default=_json_default)


def _json_default(obj: Any) -> Any:
    """JSON 化钩子：dataclass 转 dict，langchain 对象走 to_json，pydantic 走 model_dump。"""
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    to_json = getattr(obj, "to_json", None)
    if callable(to_json):
        return to_json()
    model_dump = getattr(obj, "model_dump", None)
    if callable(model_dump):
        return model_dump()
    return str(obj)
