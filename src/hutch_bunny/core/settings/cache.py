from pydantic_settings import BaseSettings
from pydantic import Field


class CacheSettings(BaseSettings):
    CACHE_ENABLED: bool = Field(
        description="Enable caching of distribution query results", default=False
    )
    CACHE_DIR: str = Field(
        description="Directory to store cached distribution results",
        default="/app/cache",
    )
    CACHE_TTL_HOURS: float = Field(
        description="Cache validity (time-to-live) period in hours (0 = never expires)",
        default=24.0,
    )
    CACHE_REFRESH_ON_STARTUP: bool = Field(
        description="Refresh cache when daemon starts", default=True
    )
