from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from typing import Optional, Literal

class Settings(BaseSettings):
    """
    Settings for the application
    """
    model_config = SettingsConfigDict(validate_default=False)
    DATASOURCE_USE_TRINO: bool = Field(description='Whether to use Trino as the datasource', default=False)
    DEFAULT_POSTGRES_DRIVER: str = Field(description='The default postgres driver', default="postgresql+psycopg")
    DEFAULT_MSSQL_DRIVER: str = Field(description='The default mssql driver', default="mssql+pymssql")
    DEFAULT_DB_DRIVER: str = Field(description='The default database driver', default="postgresql+psycopg")
    LOW_NUMBER_SUPPRESSION_THRESHOLD: int = Field(description='The threshold for low numbers', default=5)
    ROUNDING_TARGET: int = Field(description='The target for rounding', default=5)
    
    LOGGER_NAME: str = "hutch"
    LOGGER_LEVEL: str = "INFO"
    MSG_FORMAT: str = "%(levelname)s - %(asctime)s - %(message)s"
    DATE_FORMAT: str = "%d-%b-%y %H:%M:%S"

    DATASOURCE_DB_USERNAME: str = Field(description='The username for the datasource database', default="trino-user")
    DATASOURCE_DB_PASSWORD: str = Field(description='The password for the datasource database')
    DATASOURCE_DB_HOST: str = Field(description='The host for the datasource database')
    DATASOURCE_DB_PORT: int = Field(description='The port for the datasource database', default=8080)
    DATASOURCE_DB_SCHEMA: str = Field(description='The schema for the datasource database')
    DATASOURCE_DB_DATABASE: str = Field(description='The database for the datasource database')
    DATASOURCE_DB_CATALOG: str = Field(description='The catalog for the datasource database', default="hutch")

class DaemonSettings(Settings):
    """
    Settings for the daemon
    """
    TASK_API_BASE_URL: Optional[str] = Field(description='The base URL of the task API')
    TASK_API_USERNAME: Optional[str] = Field(description='The username for the task API')
    TASK_API_PASSWORD: Optional[str] = Field(description='The password for the task API')
    TASK_API_TYPE: Optional[Literal['a', 'b']] = Field(description='The type of task API to use')
    COLLECTION_ID: Optional[str] = Field(description='The collection ID')
    POLLING_INTERVAL: int = Field(description='The polling interval', default=5)


# Singleton instance of the settings
_settings_instance: Optional[Settings | DaemonSettings] = None

def get_settings(daemon: bool = False) -> Settings | DaemonSettings:
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = DaemonSettings() if daemon else Settings()
    return _settings_instance
