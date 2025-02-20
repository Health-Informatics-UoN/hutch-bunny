from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from typing import Optional, Literal, overload
from functools import cache


class Settings(BaseSettings):
    """
    Settings for the application
    """

    model_config = SettingsConfigDict(validate_default=False)
    DATASOURCE_USE_TRINO: bool = Field(
        description="Whether to use Trino as the datasource", default=False
    )
    DEFAULT_POSTGRES_DRIVER: str = Field(
        description="The default postgres driver", default="postgresql+psycopg"
    )
    DEFAULT_MSSQL_DRIVER: str = Field(
        description="The default mssql driver", default="mssql+pymssql"
    )
    DEFAULT_DB_DRIVER: str = Field(
        description="The default database driver", default="postgresql+psycopg"
    )
    LOW_NUMBER_SUPPRESSION_THRESHOLD: int = Field(
        description="The threshold for low numbers", default=5
    )
    ROUNDING_TARGET: int = Field(description="The target for rounding", default=5)

    LOGGER_NAME: str = "hutch"
    LOGGER_LEVEL: str = Field(
        description="The level of the logger. Must be one of: DEBUG, INFO, WARNING, ERROR, CRITICAL",
        default="INFO",
        alias="BUNNY_LOGGER_LEVEL",
        pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$",
    )
    MSG_FORMAT: str = "%(levelname)s - %(asctime)s - %(message)s"
    DATE_FORMAT: str = "%d-%b-%y %H:%M:%S"

    DATASOURCE_DB_USERNAME: str = Field(
        description="The username for the datasource database", default="trino-user"
    )
    DATASOURCE_DB_PASSWORD: str = Field(
        description="The password for the datasource database"
    )
    DATASOURCE_DB_HOST: str = Field(description="The host for the datasource database")
    DATASOURCE_DB_PORT: int = Field(
        description="The port for the datasource database", default=8080
    )
    DATASOURCE_DB_SCHEMA: str = Field(
        description="The schema for the datasource database"
    )
    DATASOURCE_DB_DATABASE: str = Field(
        description="The database for the datasource database"
    )
    DATASOURCE_DB_CATALOG: str = Field(
        description="The catalog for the datasource database", default="hutch"
    )

    def safe_model_dump(self) -> dict:
        """
        Convert settings to a dictionary, excluding sensitive fields.
        """
        return self.model_dump(exclude={"DATASOURCE_DB_PASSWORD"})


class DaemonSettings(Settings):
    """
    Settings for the daemon
    """

    TASK_API_BASE_URL: str = Field(description="The base URL of the task API")
    TASK_API_USERNAME: str = Field(description="The username for the task API")
    TASK_API_PASSWORD: str = Field(description="The password for the task API")
    TASK_API_TYPE: Optional[Literal["a", "b"]] = Field(
        description="The type of task API to use", default=None
    )
    COLLECTION_ID: str = Field(description="The collection ID")
    POLLING_INTERVAL: int = Field(description="The polling interval", default=5)
    INITIAL_BACKOFF: int = Field(
        description="The initial backoff in seconds", default=5
    )
    MAX_BACKOFF: int = Field(description="The maximum backoff in seconds", default=60)

    def safe_model_dump(self) -> dict:
        """
        Convert settings to a dictionary, excluding sensitive fields.
        """
        return self.model_dump(exclude={"DATASOURCE_DB_PASSWORD", "TASK_API_PASSWORD"})


@overload
def get_settings(daemon: Literal[True]) -> DaemonSettings: ...
@overload
def get_settings(daemon: Literal[False] = False) -> Settings: ...
def get_settings(daemon: bool = False) -> Settings | DaemonSettings:
    """Get application settings, either base or daemon-specific.

    Args:
        daemon: If True, returns DaemonSettings with additional daemon-specific config.
               If False, returns base Settings.

    Returns:
        Settings or DaemonSettings object containing the application configuration
    """
    return _cached_get_settings(daemon)


@cache
def _cached_get_settings(daemon: bool) -> Settings | DaemonSettings:
    """Cached helper function for loading application settings.

    This function uses @cache decorator to memoize settings and avoid reloading
    from environment variables on subsequent calls.

    Args:
        daemon: If True, loads DaemonSettings with additional daemon-specific config.
               If False, loads base Settings.

    Returns:
        Settings or DaemonSettings object containing the cached application configuration
    """
    return DaemonSettings() if daemon else Settings()  # type: ignore
