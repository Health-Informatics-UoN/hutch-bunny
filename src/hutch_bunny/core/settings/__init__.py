from dotenv import load_dotenv

from hutch_bunny.core.settings.cache import CacheSettings
from hutch_bunny.core.settings.datasource import DatasourceSettings
from hutch_bunny.core.settings.logging import LoggingSettings
from hutch_bunny.core.settings.obfuscation import ObfuscationSettings
from hutch_bunny.core.settings.taskapi import TaskApiSettings
from hutch_bunny.core.settings.telemetry import TelemetrySettings

load_dotenv(dotenv_path=".env", override=False)


class Settings(
    LoggingSettings,
    DatasourceSettings,
    CacheSettings,
    ObfuscationSettings,
    TelemetrySettings,
):
    """
    Settings for the application
    """

    def safe_model_dump(self) -> dict[str, object]:
        """
        Convert settings to a dictionary, excluding sensitive fields.
        """
        return self.model_dump(exclude={"DATASOURCE_DB_PASSWORD"})


class DaemonSettings(Settings, TaskApiSettings):
    """
    Settings for the daemon
    """

    def safe_model_dump(self) -> dict[str, object]:
        """
        Convert settings to a dictionary, excluding sensitive fields.
        """
        return self.model_dump(
            exclude={"DATASOURCE_DB_PASSWORD", "TASK_API_PASSWORD", "COLLECTION_ID"}
        )


__all__ = [
    "Settings",
    "DaemonSettings",
    "CacheSettings",
    "DatasourceSettings",
    "LoggingSettings",
    "ObfuscationSettings",
    "TaskApiSettings",
    "TelemetrySettings",
]
