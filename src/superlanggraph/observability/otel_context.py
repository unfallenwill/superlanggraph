"""Envelope 的跨进程 trace 上下文注入与提取（W3C TraceContext）。

载体是 ``Envelope.trace``（``dict[str, str]``），内容为 W3C 传播头
（``traceparent`` / ``tracestate``）。driver 派发任务、worker 回传结果、
事件流条目都注入，消费侧提取——一条 run 的 trace 从 HTTP 入口贯穿到远程节点。
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping

from opentelemetry import propagate
from opentelemetry.context import Context

from superlanggraph.core.envelope import Envelope


def inject_into_envelope(envelope: Envelope) -> Envelope:
    """把当前上下文注入 envelope，返回携带 trace 的新实例（frozen 不可变）。"""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    if not carrier:
        return envelope
    return dataclasses.replace(envelope, trace=carrier)


def extract_from_envelope(envelope: Envelope) -> Context:
    """从 envelope 的 trace 字段提取上下文（无则返回当前上下文）。"""
    carrier: dict[str, str] = dict(envelope.trace or {})
    return propagate.extract(carrier)


def context_from_mapping(carrier: Mapping[str, str]) -> Context:
    """从任意字符串映射提取上下文（控制消息等非 envelope 场景）。"""
    return propagate.extract(dict(carrier))


def context_to_trace_mapping() -> dict[str, str]:
    """导出当前上下文为字符串映射（任务派发等需要显式传递的场景）。"""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier
