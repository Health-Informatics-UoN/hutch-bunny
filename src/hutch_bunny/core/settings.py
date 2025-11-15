from pydantic_settings import BaseSettings
from pydantic import Field, field_validator, ValidationInfo
from typing import Optional, Literal, Mapping, Sequence
from hutch_bunny.core.logger import logger
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    """
    Settings for the application
    """
    CACHE_ENABLED: bool = Field(
        description="Enable caching of distribution query results",
        default=False
    )
    CACHE_DIR: str = Field(
        description="Directory to store cached distribution results",
        default="/app/cache"
    )
    CACHE_TTL_HOURS: float = Field(
        description="Cache validity (time-to-live) period in hours (0 = never expires)",
        default=24.0
    )
    CACHE_REFRESH_ON_STARTUP: bool = Field(
        description="Refresh cache when daemon starts",
        default=True
    )
    DATASOURCE_USE_TRINO: bool = Field(
        description="Whether to use Trino as the datasource", default=False
    )
    DATASOURCE_USE_AZURE_MANAGED_IDENTITY: bool = Field(
        description="Whether to use Azure managed identity for authentication",
        default=False,
    )
    DATASOURCE_AZURE_MANAGED_IDENTITY_CLIENT_ID: str | None = Field(
        description="The client ID for Azure managed identity", default=None
    )
    LOW_NUMBER_SUPPRESSION_THRESHOLD: int = Field(
        description="The threshold for low numbers", default=10
    )
    ROUNDING_TARGET: int = Field(description="The target for rounding", default=10)

    LOGGER_NAME: str = "hutch"
    LOGGER_LEVEL: str = Field(
        description="The level of the logger. Must be one of: DEBUG, INFO, WARNING, ERROR, CRITICAL",
        default="INFO",
        alias="BUNNY_LOGGER_LEVEL",
        pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$",
    )
    MSG_FORMAT: str = "%(levelname)s - %(asctime)s - %(message)s"
    DATE_FORMAT: str = "%d-%b-%y %H:%M:%S"

    DATASOURCE_DB_DRIVERNAME: str = Field(
        description="The driver to use for the datasource database, one of: postgresql, mssql, duckdb",
        default="postgresql",
        pattern="^(postgresql|mssql|duckdb)$",
    )
    DATASOURCE_DB_USERNAME: str | None = Field(
        description="The username for the datasource database. Not required when using Azure managed identity.",
        default="trino-user",
    )
    DATASOURCE_DB_PASSWORD: str | None = Field(
        description="The password for the datasource database. Not required when using Azure managed identity.",
        default=None,
    )
    DATASOURCE_DB_HOST: str | None = Field(
        description="The host for the datasource database. Optional if using duckdb.",
        default=None,
    )
    DATASOURCE_DB_PORT: int | None = Field(
        description="The port for the datasource database. Optional if using duckdb.",
        default=None,
    )
    DATASOURCE_DB_SCHEMA: str = Field(
        description="The schema for the datasource database"
    )
    DATASOURCE_DB_DATABASE: str | None = Field(
        description="The database for the datasource database. Optional if using duckdb.",
        default=None,
    )
    DATASOURCE_DB_CATALOG: str = Field(
        description="The catalog for the datasource database", default="hutch"
    )
    DATASOURCE_DUCKDB_PATH_TO_DB: str = Field(
        description="The path to the DuckDB database file", default="/data/file.db"
    )
    DATASOURCE_DUCKDB_MEMORY_LIMIT: str = Field(
        description="The memory limit for DuckDB (e.g. '1000mb', '2gb')", default="1000mb"
    )
    OTEL_ENABLED: bool = Field(
        description="Boolean indicating whether or not telemetry data is exported via opentelemetry to the observability backend(s).", 
        default=False,
    )
    OTEL_SERVICE_NAME: str = Field(
        description="Service identification for opentelemetry.", 
        default= "hutch-bunny-daemon"
    )
    OTEL_EXPORTER_OTLP_ENDPOINT: str = Field(
        description="Opentelemetry collector endpoint required for sending data.", 
        default="http://otel-collector:4317"
    )
    DATASOURCE_DUCKDB_TEMP_DIRECTORY: str = Field(
        description="The temporary directory for DuckDB - used as a swap fir larger-than-memory processing.", default="/tmp"
    )
    DATASOURCE_DB_CONNECTION_QUERY: Mapping[str, str | Sequence[str]] | None = Field(
        description="A mapping representing the query string. Contains strings for keys and either strings or tuples of strings for values.",
        default=None
    )

    def safe_model_dump(self) -> dict[str, object]:
        """
        Convert settings to a dictionary, excluding sensitive fields.
        """
        return self.model_dump(exclude={"DATASOURCE_DB_PASSWORD"})
    
    @staticmethod
    def _validate_duckdb_field(v, info: ValidationInfo, field_name: str) -> str | int | None:
        driver = info.data.get("DATASOURCE_DB_DRIVERNAME", None)
        if driver == "duckdb":
            return v
        if v is None or (isinstance(v, str) and not v):
            raise ValueError(f"{field_name} is required unless using duckdb.")
        return v

    @field_validator("DATASOURCE_DB_HOST")
    def validate_db_host(cls, v: str | None, info: ValidationInfo) -> str | None:
        return cls._validate_duckdb_field(v, info, "DATASOURCE_DB_HOST")

    @field_validator("DATASOURCE_DB_PORT")
    def validate_db_port(cls, v: int | None, info: ValidationInfo) -> int | None:
        return cls._validate_duckdb_field(v, info, "DATASOURCE_DB_PORT")

    @field_validator("DATASOURCE_DB_DATABASE")
    def validate_db_database(cls, v: str | None, info: ValidationInfo) -> str | None:
        return cls._validate_duckdb_field(v, info, "DATASOURCE_DB_DATABASE")




class DaemonSettings(Settings):
    """
    Settings for the daemon
    """

    TASK_API_ENFORCE_HTTPS: bool = Field(
        description="Whether to enforce HTTPS for the task API", default=True  
    )
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

    @field_validator("TASK_API_BASE_URL")
    def validate_https_enforcement(cls, v: str, info: ValidationInfo) -> str:
        """
        Validates that HTTPS is used when TASK_API_ENFORCE_HTTPS is True.
        """
        enforce_https = info.data.get("TASK_API_ENFORCE_HTTPS", True)

        if not v.startswith("https://"):
            if enforce_https:
                raise ValueError(
                    "HTTPS is required for the task API but not used. Set TASK_API_ENFORCE_HTTPS to false if you are using a non-HTTPS connection."
                )
            else:
                logger.warning(
                    "HTTPS is not used for the task API. This is not recommended in production environments."
                )
        return v

    def safe_model_dump(self) -> dict[str, object]:
        """
        Convert settings to a dictionary, excluding sensitive fields.
        """
        return self.model_dump(
            exclude={"DATASOURCE_DB_PASSWORD", "TASK_API_PASSWORD", "COLLECTION_ID"}
        )
