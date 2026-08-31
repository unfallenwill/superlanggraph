"""OTel 上下文注入/提取的往返测试（不依赖 exporter）。"""

from opentelemetry import trace
from opentelemetry.context import attach, detach
from opentelemetry.trace import SpanContext, TraceFlags

from superlanggraph.core.envelope import Envelope
from superlanggraph.observability import (
    extract_from_envelope,
    inject_into_envelope,
)


def _valid_span_context() -> SpanContext:
    return SpanContext(
        trace_id=0x12345678901234567890123456789012,
        span_id=0x1234567890ABCDEF,
        is_remote=False,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
    )


def test_trace_context_roundtrip_through_envelope() -> None:
    """有效父 span 上下文注入 envelope 后，提取应得到同一 trace_id。"""

    parent_sc = _valid_span_context()
    ctx = trace.set_span_in_context(trace.NonRecordingSpan(parent_sc))
    token = attach(ctx)
    try:
        envelope = Envelope(type="task", run_id="r", payload={}, task_id="t")
        injected = inject_into_envelope(envelope)
    finally:
        detach(token)

    assert injected.trace is not None
    assert "traceparent" in injected.trace
    # 原 envelope 不可变：trace 不应原地修改
    assert envelope.trace is None

    child_ctx = extract_from_envelope(injected)
    child_sc = trace.get_current_span(child_ctx).get_span_context()
    assert child_sc.trace_id == parent_sc.trace_id
    assert child_sc.span_id == parent_sc.span_id
    assert child_sc.is_remote  # 提取侧标记为远程父上下文


def test_inject_without_active_context_leaves_envelope_untouched() -> None:
    envelope = Envelope(type="task", run_id="r", payload={}, task_id="t")
    injected = inject_into_envelope(envelope)
    assert injected is envelope
