"""框架异常体系：跨模块边界处显式抛出、显式捕获，错误绝不静默。"""


class SlgError(Exception):
    """superlanggraph 所有异常的基类。"""


class SerializationError(SlgError):
    """消息序列化或反序列化失败。"""


class BrokerError(SlgError):
    """消息基础设施操作失败。"""
