from pydantic_settings import BaseSettings
from pydantic import Field, field_validator, ValidationInfo
from typing import Mapping, Sequence


class DatasourceSettings(BaseSettings):
    DATASOURCE_USE_TRINO: bool = Field(
        description="Whether to use Trino as the datasource", default=False
    )
    DATASOURCE_USE_SNOWFLAKE: bool = Field(
        description="Whether to use Snowflake as the datasource", default=False
    )
    DATASOURCE_USE_AZURE_MANAGED_IDENTITY: bool = Field(
        description="Whether to use Azure managed identity for authentication",
        default=False,
    )
    DATASOURCE_AZURE_MANAGED_IDENTITY_CLIENT_ID: str | None = Field(
        description="The client ID for Azure managed identity", default=None
    )
    DATASOURCE_DB_DRIVERNAME: str = Field(
        description="The driver to use for the datasource database, one of: postgresql, mssql, duckdb, snowflake",
        default="postgresql",
        pattern="^(postgresql|mssql|duckdb|snowflake-connector-python)$",
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
        description="The memory limit for DuckDB (e.g. '1000mb', '2gb')",
        default="1000mb",
    )
    DATASOURCE_DUCKDB_TEMP_DIRECTORY: str = Field(
        description="The temporary directory for DuckDB - used as a swap fir larger-than-memory processing.",
        default="/tmp",
    )
    DATASOURCE_DB_ACCOUNT: str | None = Field(
        description="The Snowflake account identifier (e.g., 'LGGOZEC-CJ54726')",
        default=None,
    )
    DATASOURCE_DB_SNOWFLAKE_WAREHOUSE: str = Field(
        description="The Snowflake warehouse to use for queries", default="COMPUTE_WH"
    )
    DATASOURCE_DB_SNOWFLAKE_ROLE: str = Field(
        description="The Snowflake role to use for queries", default="SYSADMIN"
    )
    DATASOURCE_PRIVATE_KEY_PATH: str = Field(
        description="Path to the private key file (.p8) for key pair auth",
        default="/app/private_key.p8",
    )
    DATASOURCE_PRIVATE_KEY_PASSPHRASE: str = Field(
        description="Passphrase for the encrypted private key", default="password"
    )
    DATASOURCE_DB_CONNECTION_QUERY: Mapping[str, str | Sequence[str]] | None = Field(
        description="A mapping representing the query string. Contains strings for keys and either strings or tuples of strings for values.",
        default=None,
    )

    @staticmethod
    def _validate_duckdb_field(
        v, info: ValidationInfo, field_name: str
    ) -> str | int | None:
        driver = info.data.get("DATASOURCE_DB_DRIVERNAME", None)
        if driver == "duckdb":
            return v
        if v is None or (isinstance(v, str) and not v):
            raise ValueError(f"{field_name} is required unless using duckdb.")
        return v

    @staticmethod
    def _validate_optional_field(
        v, info: ValidationInfo, field_name: str, optional_drivers: set[str]
    ) -> str | int | None:
        """Validate fields that are optional for certain drivers (like DuckDB and Snowflake)"""
        driver = info.data.get("DATASOURCE_DB_DRIVERNAME", None)
        if driver in optional_drivers:
            return v
        if v is None or (isinstance(v, str) and not v):
            drivers_str = ", ".join(optional_drivers)
            raise ValueError(f"{field_name} is required unless using {drivers_str}.")
        return v

    @field_validator("DATASOURCE_DB_HOST")
    def validate_db_host(cls, v: str | None, info: ValidationInfo) -> str | None:
        driver = info.data.get("DATASOURCE_DB_DRIVERNAME", None)
        if driver in {"duckdb", "snowflake-connector-python"}:
            return v
        return cls._validate_duckdb_field(v, info, "DATASOURCE_DB_HOST")

    @field_validator("DATASOURCE_DB_PORT")
    def validate_db_port(cls, v: int | None, info: ValidationInfo) -> int | None:
        return cls._validate_optional_field(
            v, info, "DATASOURCE_DB_PORT", {"duckdb", "snowflake-connector-python"}
        )

    @field_validator("DATASOURCE_DB_DATABASE")
    def validate_db_database(cls, v: str | None, info: ValidationInfo) -> str | None:
        return cls._validate_duckdb_field(v, info, "DATASOURCE_DB_DATABASE")

    @field_validator("DATASOURCE_DB_ACCOUNT")
    def validate_snowflake_account(
        cls, v: str | None, info: ValidationInfo
    ) -> str | None:
        """Validate that account is provided when using Snowflake"""
        driver = info.data.get("DATASOURCE_DB_DRIVERNAME", None)
        use_snowflake = info.data.get("DATASOURCE_USE_SNOWFLAKE", False)

        if (driver == "snowflake" or use_snowflake) and not v:
            raise ValueError("DATASOURCE_DB_ACCOUNT is required when using Snowflake.")
        return v
