"""演示客户端：官方 SDK 直连 react-agent，跑通 token 流式与跨轮记忆。

前置：
    1. cp .env.example .env 并填入 OPENAI_API_KEY
    2. docker compose up -d redis
    3. uv run superlanggraph server --config examples/react-agent/langgraph.json
用法（URL 可用环境变量覆盖）：
    SLG_EXAMPLE_URL=http://127.0.0.1:8002 uv run python examples/react-agent/client.py
"""

import asyncio
import json
import os

from langgraph_sdk import get_client

URL = os.getenv("SLG_EXAMPLE_URL", "http://127.0.0.1:8000")
API_KEY = "demo-key"  # 未启用鉴权（SLG_AUTH_API_KEYS 为空）时任填


def _content_text(message: dict) -> str:
    """从官方序列化的消息里提取文本（兼容字符串与内容块列表）。"""
    content = message.get("kwargs", {}).get("content", "")
    if isinstance(content, str):
        return content
    return "".join(
        block.get("text", "")
        for block in content
        if isinstance(block, dict) and block.get("type") == "text"
    )


async def main() -> None:
    client = get_client(url=URL, api_key=API_KEY)
    thread = await client.threads.create()

    print("=" * 20, "1. 流式 run：messages 模式逐 token 输出", "=" * 20)
    answer = ""
    token_count = 0
    async for chunk in client.runs.stream(
        thread["thread_id"],
        "agent",
        input={"messages": [{"role": "user", "content": "上海天气怎么样？"}]},
        stream_mode=["messages", "values"],
    ):
        if chunk.event == "messages":
            token_count += 1
            text = _content_text(chunk.data[0])
            if text:
                print(text, end="", flush=True)
        elif chunk.event == "values":
            answer = _content_text(chunk.data["messages"][-1])
    print()
    print(f"[共 {token_count} 个 token 事件] 最终回答: {answer[:60]}...")

    print("=" * 20, "2. 跨轮记忆：同 thread 只问「北京呢？」", "=" * 20)
    async for chunk in client.runs.stream(
        thread["thread_id"],
        "agent",
        input={"messages": [{"role": "user", "content": "北京呢？"}]},
        stream_mode=["values"],
    ):
        if chunk.event == "values":
            answer = _content_text(chunk.data["messages"][-1])
    print(f"追问回答: {answer[:60]}...")
    print("全部演示通过。")


if __name__ == "__main__":
    asyncio.run(main())
