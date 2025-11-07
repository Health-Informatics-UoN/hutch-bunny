from pydantic_settings import BaseSettings
from pydantic import Field


class LoggingSettings(BaseSettings):
    """
    Logging configuration settings
    """

    LOGGER_NAME: str = "hutch"
    LOGGER_LEVEL: str = Field(
        description="The level of the logger. Must be one of: DEBUG, INFO, WARNING, ERROR, CRITICAL",
        default="INFO",
        alias="BUNNY_LOGGER_LEVEL",
        pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$",
    )
    MSG_FORMAT: str = "%(levelname)s - %(asctime)s - %(message)s"
    DATE_FORMAT: str = "%d-%b-%y %H:%M:%S"

