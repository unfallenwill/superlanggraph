"""superlanggraph：自托管、生产就绪的 LangGraph 运行时。

对外提供与 LangGraph Platform 兼容的 HTTP API，内部以 Redis 为中枢
把 LangGraph 的调度与执行拆分为可独立扩缩容的服务。
架构见 docs/architecture.md，工程规范见 docs/engineering.md。
"""
