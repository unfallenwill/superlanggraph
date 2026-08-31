"""远程节点代理：分布式执行的核心契约件。

driver 端把节点的 ``PregelNode.bound`` 替换为本代理；官方 Pregel 循环照常
派发任务，代理把（输入, config）发给远程执行器并等待结果。中断与错误以
原始异常在 driver 进程重抛，由官方流程写检查点。
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, Protocol

from langchain_core.runnables import Runnable, RunnableConfig

from superlanggraph.core.errors import SlgError


class NodeExecutor(Protocol):
    """driver 侧的远程执行器：proxy 借它派发任务并等待结果。"""

    async def execute_node(
        self,
        *,
        graph_id: str,
        node: str,
        thread_id: str,
        run_id: str,
        checkpoint_ns: str,
        input: Any,
        config: RunnableConfig | None,
    ) -> Any:
        """执行一个节点并返回其返回值；中断/错误以异常形式上抛。"""
        ...


class RemoteNodeProxy(Runnable[Any, Any]):
    """节点函数的远程替身；调用语义与原节点一致。"""

    def __init__(
        self,
        graph_id: str,
        node: str,
        original: Runnable[Any, Any],
        executor: NodeExecutor,
    ) -> None:
        self.graph_id = graph_id
        self.node = node
        self._original = original
        self._executor = executor

    def invoke(self, input: Any, config: RunnableConfig | None = None, **kwargs: Any) -> Any:
        raise SlgError("RemoteNodeProxy 仅支持异步调用（分布式运行时为 async）")

    async def ainvoke(
        self,
        input: Any,
        config: RunnableConfig | None = None,
        **kwargs: Any,
    ) -> Any:
        configurable: dict[str, Any] = (config or {}).get("configurable") or {}
        thread_id = configurable.get("thread_id")
        if thread_id is None:
            raise SlgError("分布式节点执行缺少 thread_id")
        return await self._executor.execute_node(
            graph_id=self.graph_id,
            node=self.node,
            thread_id=str(thread_id),
            run_id=str(configurable.get("run_id", "")),
            checkpoint_ns=str(configurable.get("checkpoint_ns", "")),
            input=input,
            config=config,
        )

    async def astream(
        self,
        input: Any,
        config: RunnableConfig | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[Any]:
        result = await self.ainvoke(input, config, **kwargs)
        yield result
