"""OpenTelemetry 初始化：traces + metrics，OTLP/HTTP 导出，未配置时全部 no-op。

配置（环境变量，遵循 OTel 标准命名）：
- ``OTEL_EXPORTER_OTLP_ENDPOINT`` — 未设置则不初始化任何 provider（零开销）
- ``OTEL_SERVICE_NAME``             — 覆盖默认的 ``superlanggraph-<role>``
- ``SLG_ENV``                       — deployment.environment.name（默认 dev）

跨进程传播：W3C TraceContext，载体为 Envelope.trace（见 core.otel_context）。
"""

from __future__ import annotations

import os
from importlib.metadata import version
from typing import Any

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def _endpoint() -> str | None:
    return os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT") or None


def otel_enabled() -> bool:
    """是否配置了 OTLP 端点；未配置时不创建任何 provider（零开销 no-op）。"""
    return _endpoint() is not None


def _resource(role: str) -> Resource:
    service_name = os.environ.get("OTEL_SERVICE_NAME") or f"superlanggraph-{role}"
    return Resource.create(
        {
            "service.name": service_name,
            "service.version": _package_version(),
            "deployment.environment.name": os.environ.get("SLG_ENV", "dev"),
            "superlanggraph.role": role,
        }
    )


def _package_version() -> str:
    try:
        return version("superlanggraph")
    except Exception:
        return "0.0.0"


def setup_otel(role: str) -> None:
    """初始化当前角色进程的 TracerProvider 与 MeterProvider（幂等）。

    未设置 ``OTEL_EXPORTER_OTLP_ENDPOINT`` 时不做任何事，
    全局 trace/meter API 保持 no-op 实现。
    """
    if not otel_enabled():
        return
    endpoint = _endpoint()
    resource = _resource(role)

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces"))
    )
    trace.set_tracer_provider(tracer_provider)

    metric_reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=f"{endpoint}/v1/metrics")
    )
    meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
    metrics.set_meter_provider(meter_provider)


def shutdown_otel() -> None:
    """进程退出前 flush 并关闭 provider（丢尾部遥测的常见原因就是没做这步）。"""
    provider = trace.get_tracer_provider()
    shutdown = getattr(provider, "shutdown", None)
    if callable(shutdown):
        shutdown()
    meter_provider = metrics.get_meter_provider()
    meter_shutdown = getattr(meter_provider, "shutdown", None)
    if callable(meter_shutdown):
        meter_shutdown()


def tracer(name: str) -> trace.Tracer:
    """按角色进程获取 Tracer；未启用时返回 no-op Tracer。"""
    return trace.get_tracer(name)


def meter(name: str) -> metrics.Meter:
    """按角色进程获取 Meter；未启用时返回 no-op Meter。"""
    return metrics.get_meter(name)


def span_attributes(**attrs: Any) -> dict[str, Any]:
    """过滤 None 值的属性字典（span 属性不接受 None）。"""
    return {key: value for key, value in attrs.items() if value is not None}
