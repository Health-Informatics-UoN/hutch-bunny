from pydantic_settings import BaseSettings
from pydantic import Field


class PollingSettings(BaseSettings):
    """
    Polling configuration settings
    """

    POLLING_INTERVAL: int = Field(description="The polling interval", default=5)
    INITIAL_BACKOFF: int = Field(
        description="The initial backoff in seconds", default=5
    )
    MAX_BACKOFF: int = Field(description="The maximum backoff in seconds", default=60)

