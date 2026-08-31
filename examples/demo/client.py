"""演示客户端：SDK 直连 superlanggraph，跑通流式 / HITL / store 全流程。

用法：uv run python examples/demo/client.py
"""

import asyncio
import uuid

from langgraph_sdk import get_client

URL = "http://127.0.0.1:8000"
API_KEY = "demo-key"


async def demo_stream(client) -> None:
    """流式 run：values / updates / custom 三种模式。"""
    print("=" * 30, "1. 流式 run（含 custom 进度事件）", "=" * 30)
    thread = await client.threads.create(metadata={"demo": "stream"})
    run = client.runs.stream(
        thread["thread_id"],
        "agent",
        input={"count": 3},
        stream_mode=["values", "updates", "custom"],
    )
    async for chunk in run:
        if chunk.event == "metadata":
            print(f"  [{chunk.event}] run_id={chunk.data['run_id'][:8]}...")
        elif chunk.event == "custom":
            print(f"  [{chunk.event}] {chunk.data}")
        elif chunk.event == "values":
            print(f"  [{chunk.event}] count={chunk.data.get('count')}")
        elif chunk.event == "updates":
            print(f"  [{chunk.event}] {chunk.data}")
        elif chunk.event == "end":
            print(f"  [{chunk.event}] status={chunk.data.get('status')}")
    await run.aclose()


async def demo_wait(client) -> None:
    """同步 run：阻塞到终态拿结果。"""
    print("=" * 30, "2. 同步 run（runs.wait）", "=" * 30)
    thread = await client.threads.create()
    result = await client.runs.wait(thread["thread_id"], "agent", input={"count": 100})
    print(f"  count=100 -> generate +10 -> amplify x3 => count={result['count']}")


async def demo_hitl(client) -> None:
    """人工介入：interrupt 暂停等待审批，Command(resume) 续跑。"""
    print("=" * 30, "3. 人工介入（interrupt → resume）", "=" * 30)
    thread = await client.threads.create()
    run = client.runs.stream(thread["thread_id"], "hitl", input={"count": 0})
    async for chunk in run:
        if isinstance(chunk.data, dict) and chunk.data.get("__interrupt__"):
            question = chunk.data["__interrupt__"][0]["value"]["question"]
            print(f"  [interrupt] {question}")
        if chunk.event == "end":
            break
    await run.aclose()

    state = await client.threads.get(thread["thread_id"])
    print(f"  暂停中，线程状态: {state['status']}")

    resumed = await client.runs.wait(thread["thread_id"], "hitl", command={"resume": 500})
    print(f"  [resume] 批准 +500 => count={resumed['count']}")


async def demo_store(client) -> None:
    """跨线程记忆：store 的写入、读取与搜索。"""
    print("=" * 30, "4. 跨线程记忆（store）", "=" * 30)
    ns = ["demo", "memory"]
    await client.store.put_item(ns, "preference", {"theme": "dark", "lang": "zh"})
    item = await client.store.get_item(ns, "preference")
    print(f"  get: {item['value']}")
    found = await client.store.search_items(ns, filter={"lang": "zh"})
    print(f"  search 命中: {[i['key'] for i in found['items']]}")


async def main() -> None:
    client = get_client(url=URL, api_key=API_KEY)
    print(f"连接 {URL}\n")
    await demo_stream(client)
    await demo_wait(client)
    await demo_hitl(client)
    await demo_store(client)
    print("\n全部演示通过。")


asyncio.run(main())
