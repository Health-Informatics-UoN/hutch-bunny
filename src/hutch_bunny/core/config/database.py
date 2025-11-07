from pydantic_settings import BaseSettings
from pydantic import Field, field_validator, ValidationInfo
from typing import Any


class DatabaseSettings(BaseSettings):
    """
    Database connection and configuration settings
    """

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

    # DuckDB specific settings
    DATASOURCE_DUCKDB_PATH_TO_DB: str = Field(
        description="The path to the DuckDB database file", default="/data/file.db"
    )
    DATASOURCE_DUCKDB_MEMORY_LIMIT: str = Field(
        description="The memory limit for DuckDB (e.g. '1000mb', '2gb')",
        default="1000mb",
    )
    DATASOURCE_DUCKDB_TEMP_DIRECTORY: str = Field(
        description="The temporary directory for DuckDB - used as a swap for larger-than-memory processing.",
        default="/tmp",
    )

    # Azure settings
    DATASOURCE_USE_AZURE_MANAGED_IDENTITY: bool = Field(
        description="Whether to use Azure managed identity for authentication",
        default=False,
    )
    DATASOURCE_AZURE_MANAGED_IDENTITY_CLIENT_ID: str | None = Field(
        description="The client ID for Azure managed identity", default=None
    )

    # Trino settings
    DATASOURCE_USE_TRINO: bool = Field(
        description="Whether to use Trino as the datasource", default=False
    )

    @staticmethod
    def _validate_duckdb_field(v: Any, info: ValidationInfo, field_name: str) -> Any:
        driver = info.data.get("DATASOURCE_DB_DRIVERNAME", None)
        if driver == "duckdb":
            return v
        if v is None or (isinstance(v, str) and not v):
            raise ValueError(f"{field_name} is required unless using duckdb.")
        return v

    @field_validator("DATASOURCE_DB_HOST")
    def validate_db_host(cls, v: str | None, info: ValidationInfo) -> str | None:
        result = cls._validate_duckdb_field(v, info, "DATASOURCE_DB_HOST")
        return result if isinstance(result, str) or result is None else str(result)

    @field_validator("DATASOURCE_DB_PORT")
    def validate_db_port(cls, v: int | None, info: ValidationInfo) -> int | None:
        result = cls._validate_duckdb_field(v, info, "DATASOURCE_DB_PORT")
        return result if isinstance(result, int) or result is None else int(result)

    @field_validator("DATASOURCE_DB_DATABASE")
    def validate_db_database(cls, v: str | None, info: ValidationInfo) -> str | None:
        result = cls._validate_duckdb_field(v, info, "DATASOURCE_DB_DATABASE")
        return result if isinstance(result, str) or result is None else str(result)

