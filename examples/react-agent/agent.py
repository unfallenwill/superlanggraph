"""原生 react-agent 示例：LLM + 天气/时间工具。

与官方 LangGraph 应用写法完全一致：状态用 ``add_messages`` reducer 累积，
历史由运行时的 checkpointer（Redis）按 thread 持久化，token 走标准
``messages`` 流式——节点代码不需要任何运行时专属 API。

模型环境变量来自 langgraph.json 的 ``env`` 字段（本目录 .env），server
启动时先注入进程环境、后加载本模块，这里直接读 ``os.environ`` 即可。
"""

import json
import os
from datetime import datetime
from typing import Annotated, TypedDict

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import (
    AIMessage,
    AnyMessage,
    SystemMessage,
    ToolMessage,
)
from langchain_core.tools import tool
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from pydantic import BaseModel, Field

SYSTEM_PROMPT = (
    "You are a helpful assistant. You can check the weather and the "
    "current time. Answer in the user's language."
)

# --- 工具（演示桩：固定数据，不依赖外部服务） ---


class WeatherReport(BaseModel):
    city: str = Field(description="城市名")
    temperature_c: float
    condition: str = Field(description="sunny/cloudy/rainy/snowy/windy 之一")
    humidity: int = Field(ge=0, le=100)


@tool
def get_weather(city: str) -> WeatherReport:
    """查询某城市当前天气。"""
    return WeatherReport(city=city, temperature_c=22.5, condition="sunny", humidity=65)


@tool
def get_current_time() -> str:
    """查询服务器当前本地时间（ISO-8601）。"""
    return datetime.now().isoformat(timespec="seconds")


TOOLS = [get_weather, get_current_time]


# --- 模型 ---


def build_model() -> ChatAnthropic:
    api_key = os.getenv("OPENAI_API_KEY") or os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("缺少模型 key：请在 .env 设置 OPENAI_API_KEY（或 ANTHROPIC_API_KEY）")
    return ChatAnthropic(
        model=os.getenv("AGENT_MODEL", "deepseek-v4-flash"),
        api_key=api_key,
        base_url=os.getenv("OPENAI_BASE_URL", "https://api.deepseek.com/anthropic"),
        max_tokens=4096,
    )


# --- 图 ---


class State(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]


async def call_model(state: State) -> dict:
    model = build_model().bind_tools(TOOLS)
    prompt = [SystemMessage(content=SYSTEM_PROMPT), *state["messages"]]
    response = await model.ainvoke(prompt)
    return {"messages": [response]}


def _tool_content(result: object) -> str:
    if isinstance(result, str):
        return result
    if hasattr(result, "model_dump_json"):
        return result.model_dump_json()
    return json.dumps(result, ensure_ascii=False, default=str)


async def run_tools(state: State) -> dict:
    registry = {t.name: t for t in TOOLS}
    tool_messages: list[ToolMessage] = []
    for tc in state["messages"][-1].tool_calls:
        try:
            content = _tool_content(await registry[tc["name"]].ainvoke(tc.get("args") or {}))
            status = "success"
        except Exception as exc:  # noqa: BLE001 - 工具失败回传给模型而不是中断
            content = f"tool error: {exc}"
            status = "error"
        tool_messages.append(
            ToolMessage(
                content=content,
                tool_call_id=tc["id"],
                name=tc["name"],
                status=status,
            )
        )
    return {"messages": tool_messages}


def should_continue(state: State) -> str:
    last = state["messages"][-1]
    return "tools" if isinstance(last, AIMessage) and last.tool_calls else END


def build_graph() -> StateGraph:
    g = StateGraph(State)
    g.add_node("model", call_model)
    g.add_node("tools", run_tools)
    g.add_edge(START, "model")
    g.add_conditional_edges("model", should_continue)
    g.add_edge("tools", "model")
    return g


graph = build_graph().compile()
