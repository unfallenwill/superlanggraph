"""运行配置：统一从环境变量 / .env 读取。

敏感信息（密钥等）只放 .env，不入库不入 git（CLAUDE.md 约定）。
配置必须由组合根显式构造后传入服务，库代码禁止自行读取全局状态。
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class BrokerSettings(BaseSettings):
    """消息基础设施配置。环境变量前缀 SLG_，如 SLG_REDIS_URL。"""

    model_config = SettingsConfigDict(env_prefix="SLG_", env_file=".env", extra="ignore")

    redis_url: str = "redis://localhost:6379/0"
    redis_max_connections: int = 2000
    worker_concurrency: int = 32
    stream_prefix: str = "slg"
    consumer_group: str = "drivers"
    event_maxlen: int = 10_000
    lease_seconds: float = 30.0


class AuthSettings(BaseSettings):
    """接入层鉴权配置。``api_keys`` 为逗号分隔的有效 key；为空则不启用鉴权。"""

    model_config = SettingsConfigDict(env_prefix="SLG_AUTH_", env_file=".env", extra="ignore")

    api_keys: str = ""

    @property
    def enabled(self) -> bool:
        return bool(self.api_keys.strip())

    @property
    def valid_keys(self) -> frozenset[str]:
        return frozenset(key.strip() for key in self.api_keys.split(",") if key.strip())
