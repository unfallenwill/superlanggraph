"""运维指标：队列深度 / PEL / 活跃 run / 心跳存活（OTel ObservableGauge）。

由 runtime 周期采集，经 OTLP 导出到 collector 的 Prometheus 端点。
未启用 OTel 时全部 no-op。
"""

from __future__ import annotations

import contextlib
from typing import Any

from opentelemetry import metrics as otel_metrics


def make_metrics_collector(client: Any, prefix: str, group: str) -> Any:
    """构造周期指标采集回调（由 runtime 在 start_background 时注册）。

    返回 async 回调，每次调用采集一轮 gauge 并通过 OTel 导出。
    """

    otel_meter = otel_metrics.get_meter("superlanggraph.metrics")
    tasks_gauge = otel_meter.create_gauge(
        "slg.tasks.length", description="任务队列长度", unit="{tasks}"
    )
    ctrl_gauge = otel_meter.create_gauge(
        "slg.ctrl.length", description="控制流长度", unit="{messages}"
    )
    pel_gauge = otel_meter.create_gauge(
        "slg.tasks.pel", description="PEL 未确认任务数", unit="{tasks}"
    )
    active_gauge = otel_meter.create_gauge(
        "slg.active_runs", description="活跃 run 数", unit="{runs}"
    )

    async def collect() -> None:
        with contextlib.suppress(Exception):
            tasks_len = await client.xlen(f"{prefix}:tasks")
            tasks_gauge.set(tasks_len)

            ctrl_len = await client.xlen(f"{prefix}:ctrl")
            ctrl_gauge.set(ctrl_len)

            pel = await client.xpending_range(f"{prefix}:tasks", group, min="-", max="+", count=100)
            pel_gauge.set(len(pel) if pel else 0)

            active = 0
            async for _key in client.scan_iter(match=f"{prefix}:thread-active:*"):
                active += 1
            active_gauge.set(active)

    return collect
