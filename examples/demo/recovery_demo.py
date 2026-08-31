"""守尸接管演示：run 执行中途 kill driver，新 driver 自动认领恢复。"""

import asyncio
import os
import signal
import subprocess
import time
import uuid

from langgraph_sdk import get_client

URL = "http://127.0.0.1:8000"
API_KEY = "demo-key"


def kill_drivers() -> list[int]:
    """杀掉全部 driver 进程，返回被杀的 pid。"""
    out = subprocess.run(
        ["pgrep", "-af", "superlanggraph driver"], capture_output=True, text=True
    ).stdout
    killed = []
    for line in out.splitlines():
        pid_str, _, cmd = line.partition(" ")
        if "bin/superlanggraph" in cmd:
            os.kill(int(pid_str), signal.SIGKILL)
            killed.append(int(pid_str))
    return killed


async def main() -> None:
    client = get_client(url=URL, api_key=API_KEY)
    thread_id = (await client.threads.create())["thread_id"]

    print("1) 启动 slow run（节点 sleep 3 秒）", flush=True)
    run = await client.runs.create(thread_id, "slow", input={"count": 0})
    run_id = run["run_id"]

    for _ in range(30):
        record = await client.runs.get(thread_id, run_id)
        if record["status"] == "running":
            break
        await asyncio.sleep(0.2)
    print(f"2) run 进入 running：{run_id[:8]}...", flush=True)

    print("3) kill 掉全部 driver 进程（模拟宿主机崩溃）", flush=True)
    killed = kill_drivers()
    print(f"   被杀 driver pid: {killed}", flush=True)
    time.sleep(1)

    print("4) 启动新的 driver 进程（守尸开始扫描）", flush=True)
    proc = subprocess.Popen(
        ["uv", "run", "superlanggraph", "driver", "--config",
         "examples/demo/langgraph.json"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    print("5) 等待守尸认领孤儿 run 并恢复...", flush=True)
    for _ in range(60):
        record = await client.runs.get(thread_id, run_id)
        status = record["status"]
        if status in ("success", "error", "interrupted"):
            break
        await asyncio.sleep(0.5)
    print(f"6) 恢复完成，最终状态: {status}", flush=True)
    state = await client.threads.get_state(thread_id)
    print(f"7) 线程最终值: {state['values']}")

    proc.terminate()
    assert status == "success", "孤儿 run 应被守尸接管并恢复至 success"
    print("\n守尸接管演示通过。")


asyncio.run(main())
