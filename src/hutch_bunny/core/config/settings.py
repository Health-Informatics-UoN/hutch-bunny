from pydantic_settings import BaseSettings
from pydantic import Field
from hutch_bunny.core.config.database import DatabaseSettings
from hutch_bunny.core.config.logging import LoggingSettings
from hutch_bunny.core.config.obfuscation import ObfuscationSettings
from hutch_bunny.core.config.task_api import TaskApiSettings
from hutch_bunny.core.config.polling import PollingSettings


class Settings(BaseSettings):
    """
    Base settings class for CLI - includes only what CLI needs
    """

    # Compose granular settings that CLI needs
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    obfuscation: ObfuscationSettings = Field(default_factory=ObfuscationSettings)

    def safe_model_dump(self) -> dict[str, object]:
        """
        Convert settings to a dictionary, excluding sensitive fields.
        """
        return {
            "database": self.database.model_dump(exclude={"DATASOURCE_DB_PASSWORD"}),
            "logging": self.logging.model_dump(),
            "obfuscation": self.obfuscation.model_dump(),
        }


class DaemonSettings(BaseSettings):
    """
    Settings for the daemon - includes only what daemon needs
    """

    # Compose granular settings that daemon needs
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    obfuscation: ObfuscationSettings = Field(default_factory=ObfuscationSettings)
    task_api: TaskApiSettings = Field(default_factory=TaskApiSettings)
    polling: PollingSettings = Field(default_factory=PollingSettings)

    def safe_model_dump(self) -> dict[str, object]:
        """
        Convert settings to a dictionary, excluding sensitive fields.
        """
        return {
            "database": self.database.model_dump(exclude={"DATASOURCE_DB_PASSWORD"}),
            "logging": self.logging.model_dump(),
            "obfuscation": self.obfuscation.model_dump(),
            "task_api": self.task_api.model_dump(
                exclude={"TASK_API_PASSWORD", "COLLECTION_ID"}
            ),
            "polling": self.polling.model_dump(),
        }

