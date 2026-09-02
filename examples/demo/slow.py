"""慢节点图：供守尸接管演示。"""

import time
from typing import TypedDict

from langgraph.graph import END, START, StateGraph


class State(TypedDict, total=False):
    count: int


def slow(state: State) -> dict:
    time.sleep(3)
    return {"count": state.get("count", 0) + 1}


builder = StateGraph(State)
builder.add_node("slow", slow)
builder.add_edge(START, "slow")
builder.add_edge("slow", END)
graph = builder.compile()
