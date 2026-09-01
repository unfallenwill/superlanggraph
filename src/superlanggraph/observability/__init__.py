"""可观测性基础设施：OTel 初始化、跨进程 trace 上下文、指标采集。"""

from superlanggraph.observability.metrics import make_metrics_collector
from superlanggraph.observability.otel import (
    meter,
    otel_enabled,
    setup_otel,
    shutdown_otel,
    span_attributes,
    tracer,
)
from superlanggraph.observability.otel_context import (
    context_to_trace_mapping,
    extract_from_envelope,
    inject_into_envelope,
)

__all__ = [
    "context_to_trace_mapping",
    "extract_from_envelope",
    "inject_into_envelope",
    "make_metrics_collector",
    "meter",
    "otel_enabled",
    "setup_otel",
    "shutdown_otel",
    "span_attributes",
    "tracer",
]
