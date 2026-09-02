"""高并发压测：langgraph-sdk 直连 superlanggraph，并发爬坡测吞吐与延迟。

用法（先启动 server，见 recovery_demo.py 的启动方式）：
    uv run python examples/demo/benchmark.py

图用 agent（generate + amplify，纯运行时开销，不掺模型延迟）。
每档并发跑固定时长，统计成功数、吞吐（runs/s）与 P50/P90/P99 延迟。
"""

from __future__ import annotations

import asyncio
import time

from langgraph_sdk import get_client

URL = "http://127.0.0.1:8000"
API_KEY = "demo-key"
GRAPH = "agent"
CONCURRENCY_LEVELS = (10, 50, 100)
WINDOW_SECONDS = 8.0


async def hammer(client, stop_at: float, latencies: list[float], errors: list[str]) -> None:
    """单个压测协程：循环建线程发 run，直到时间窗口关闭。"""
    while time.perf_counter() < stop_at:
        started = time.perf_counter()
        try:
            thread = await client.threads.create()
            await client.runs.wait(thread["thread_id"], GRAPH, input={"count": 1})
            latencies.append(time.perf_counter() - started)
        except Exception as exc:  # 压测客户端：失败计数后继续
            errors.append(repr(exc))


def percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    index = min(len(sorted_values) - 1, int(len(sorted_values) * pct))
    return sorted_values[index]


async def run_level(level: int) -> None:
    """一档并发：C 个协程压 WINDOW_SECONDS 秒，输出吞吐与延迟分位。"""
    client = get_client(url=URL, api_key=API_KEY, timeout=60)
    latencies: list[float] = []
    errors: list[str] = []

    started = time.perf_counter()
    stop_at = started + WINDOW_SECONDS
    await asyncio.gather(*(hammer(client, stop_at, latencies, errors) for _ in range(level)))
    elapsed = time.perf_counter() - started

    latencies.sort()
    ok, failed = len(latencies), len(errors)
    total = ok + failed
    print(
        f"并发 {level:>3} | 完成 {total:>4}（失败 {failed}）"
        f" | 吞吐 {total / elapsed:6.1f} runs/s"
        f" | P50 {percentile(latencies, 0.50):.3f}s"
        f" P90 {percentile(latencies, 0.90):.3f}s"
        f" P99 {percentile(latencies, 0.99):.3f}s"
    )
    if errors:
        print(f"  首个错误: {errors[0][:200]}")


async def main() -> None:
    client = get_client(url=URL, api_key=API_KEY)
    assistant = await client.assistants.get(GRAPH)
    print(f"目标 {URL}，assistant={assistant['assistant_id'][:8]}...，每档 {WINDOW_SECONDS:.0f}s\n")

    for level in CONCURRENCY_LEVELS:
        await run_level(level)
        await asyncio.sleep(2)  # 档间冷却，避免上一档余波污染下一档


asyncio.run(main())
