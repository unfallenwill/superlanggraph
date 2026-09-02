"""演示 agent：generate 发 custom 流式事件，amplify 放大结果。"""

from typing import TypedDict

from langgraph.config import get_stream_writer
from langgraph.graph import END, START, StateGraph


class State(TypedDict, total=False):
    count: int


def generate(state: State) -> dict:
    writer = get_stream_writer()
    for step in range(3):
        writer({"progress": f"generating {step + 1}/3"})
    return {"count": state.get("count", 0) + 10}


def amplify(state: State) -> dict:
    return {"count": state.get("count", 0) * 3}


builder = StateGraph(State)
builder.add_node("generate", generate)
builder.add_node("amplify", amplify)
builder.add_edge(START, "generate")
builder.add_edge("generate", "amplify")
builder.add_edge("amplify", END)
graph = builder.compile()
