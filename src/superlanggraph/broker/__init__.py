"""消息基础设施层：core 协议的 Redis 与内存实现。

选择哪个实现是组合根（cli）的职责，基础设施自身不做任何绑定。
"""

from superlanggraph.broker.memory import MemoryBroker
from superlanggraph.broker.redis import RedisBroker

__all__ = ["MemoryBroker", "RedisBroker"]
