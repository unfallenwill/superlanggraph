"""契约层：消息封套、异常、协议与配置。

依赖方向的最底层，禁止导入任何其他内部包（import-linter 分层合同强制）。
"""

from superlanggraph.core.envelope import ENVELOPE_VERSION, Envelope
from superlanggraph.core.errors import BrokerError, SerializationError, SlgError
from superlanggraph.core.protocols import ControlBus, EventBus, ResultBus, TaskQueue
from superlanggraph.core.settings import BrokerSettings

__all__ = [
    "ENVELOPE_VERSION",
    "BrokerError",
    "BrokerSettings",
    "ControlBus",
    "Envelope",
    "EventBus",
    "ResultBus",
    "SerializationError",
    "SlgError",
    "TaskQueue",
]
