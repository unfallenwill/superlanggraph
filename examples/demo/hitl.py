"""演示人工介入：interrupt 请求审批，resume 后继续。"""

from typing import TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt


class State(TypedDict, total=False):
    count: int


def approve(state: State) -> dict:
    granted = interrupt({"question": "是否批准本次放大操作？"})
    return {"count": state.get("count", 0) + int(granted)}


builder = StateGraph(State)
builder.add_node("approve", approve)
builder.add_edge(START, "approve")
builder.add_edge("approve", END)
graph = builder.compile()
