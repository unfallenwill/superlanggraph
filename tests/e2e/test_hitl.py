# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportMissingTypeArgument=false, reportUnnecessaryIsInstance=false, reportIndexIssue=false, reportCallIssue=false, reportOptionalSubscript=false, reportUnknownArgumentType=false
# langgraph-sdk 存根泛型推断不完整；e2e 是类型边界，src/ 不受影响。

"""HITL 端到端：interrupt 暂停 → 线程中断态 → Command(resume) 续跑。"""

from __future__ import annotations

from typing import Any, cast


async def test_interrupt_and_resume(server_url: str, client: Any) -> None:
    thread_id = (await client.threads.create())["thread_id"]

    interrupts: list[Any] = []
    run = client.runs.stream(thread_id, "hitl", input={"count": 0}, stream_mode="values")
    async for chunk in run:
        data = chunk.data
        if isinstance(data, dict) and data.get("__interrupt__"):
            interrupts = list(data["__interrupt__"])
        if chunk.event == "end":
            break
    assert interrupts, "values 流应携带 __interrupt__"
    assert interrupts[0]["value"]["question"] == "给个数字"

    thread = await client.threads.get(thread_id)
    assert thread["status"] == "interrupted"

    resumed = cast(
        "dict[str, Any]",
        await client.runs.wait(thread_id, "hitl", command={"resume": 42}),
    )
    assert resumed["count"] == 52  # 42 + 10，续跑后经 bump 节点
