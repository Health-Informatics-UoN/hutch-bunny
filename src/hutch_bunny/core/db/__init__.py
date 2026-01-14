"""Database management module for Hutch Bunny."""

from hutch_bunny.core.logger import logger, INFO
from hutch_bunny.core.settings import Settings
from tenacity import (
    retry,
    stop_after_attempt,
    wait_fixed,
    before_sleep_log,
    after_log,
)

from .base import BaseDBClient
from .snowflake import SnowflakeDBClient
from .sync import SyncDBClient
from .trino import TrinoDBClient
from .azure import AzureManagedIdentityDBClient
from .duckdb import DuckDBClient
from .utils import (
    DEFAULT_TRINO_PORT,
    expand_short_drivers,
)

settings = Settings()

def _is_snowflake_connector(drivername: str | None) -> bool:
    """
    Return True if the configured SQLAlchemy driver/dialect indicates Snowflake.

    We accept both:
    - 'snowflake' (SQLAlchemy dialect name)
    - 'snowflake-connector-python' (legacy/misconfigured value seen in envs)
    - 'snowflake...' variants (e.g. 'snowflake+something')
    """
    if not drivername:
        return False
    dn = drivername.strip().lower()
    return dn == "snowflake" or dn == "snowflake-connector-python" or dn.startswith("snowflake+")

def _create_trino_client() -> TrinoDBClient:
    """Create a Trino database client."""
    datasource_db_port = settings.DATASOURCE_DB_PORT or DEFAULT_TRINO_PORT

    # Validate required fields for Trino
    if not settings.DATASOURCE_DB_USERNAME or not settings.DATASOURCE_DB_PASSWORD:
        raise ValueError(
            "DATASOURCE_DB_USERNAME and DATASOURCE_DB_PASSWORD are required for Trino"
        )

    return TrinoDBClient(
        username=settings.DATASOURCE_DB_USERNAME,
        password=settings.DATASOURCE_DB_PASSWORD,
        host=settings.DATASOURCE_DB_HOST,
        port=datasource_db_port,
        schema=settings.DATASOURCE_DB_SCHEMA,
        catalog=settings.DATASOURCE_DB_CATALOG,
    )


def _create_azure_client() -> AzureManagedIdentityDBClient:
    """Create an Azure managed identity database client."""
    datasource_db_port = settings.DATASOURCE_DB_PORT
    datasource_db_drivername = expand_short_drivers(
        settings.DATASOURCE_DB_DRIVERNAME,
        use_azure_managed_identity=True,
    )

    return AzureManagedIdentityDBClient(
        host=settings.DATASOURCE_DB_HOST,
        port=int(datasource_db_port) if datasource_db_port is not None else None,
        database=settings.DATASOURCE_DB_DATABASE,
        drivername=datasource_db_drivername,
        managed_identity_client_id=settings.DATASOURCE_AZURE_MANAGED_IDENTITY_CLIENT_ID,
        schema=settings.DATASOURCE_DB_SCHEMA,
    )

def _create_duckdb_client() -> DuckDBClient:
    """Create an DuckDB client."""

    return DuckDBClient(
        path_to_db=settings.DATASOURCE_DUCKDB_PATH_TO_DB,
        duckdb_memory_limit=settings.DATASOURCE_DUCKDB_MEMORY_LIMIT,
        schema=settings.DATASOURCE_DB_SCHEMA,
        duckdb_temp_directory=settings.DATASOURCE_DUCKDB_TEMP_DIRECTORY,
    )

def _create_sync_client() -> SyncDBClient:
    """Create a regular synchronous database client."""
    datasource_db_port = settings.DATASOURCE_DB_PORT
    datasource_db_drivername = expand_short_drivers(
        settings.DATASOURCE_DB_DRIVERNAME,
        use_azure_managed_identity=False,
    )

    # Validate that username and password are provided for non-Azure connections
    if not settings.DATASOURCE_DB_USERNAME or not settings.DATASOURCE_DB_PASSWORD:
        raise ValueError(
            "DATASOURCE_DB_USERNAME and DATASOURCE_DB_PASSWORD are required when not using Azure managed identity"
        )

    return SyncDBClient(
        username=settings.DATASOURCE_DB_USERNAME,
        password=settings.DATASOURCE_DB_PASSWORD,
        host=settings.DATASOURCE_DB_HOST,
        port=int(datasource_db_port) if datasource_db_port is not None else None,
        database=settings.DATASOURCE_DB_DATABASE,
        drivername=datasource_db_drivername,
        schema=settings.DATASOURCE_DB_SCHEMA,
        query=settings.DATASOURCE_DB_CONNECTION_QUERY
    )



def _create_snowflake_client() -> SnowflakeDBClient:
    """Create a Snowflake database client."""
    datasource_db_snowflake_warehouse = settings.DATASOURCE_DB_SNOWFLAKE_WAREHOUSE
    datasource_db_snowflake_role = settings.DATASOURCE_DB_SNOWFLAKE_ROLE
    datasource_db_snowflake_key_path = settings.DATASOURCE_PRIVATE_KEY_PATH
    datasource_db_snowflake_passphrase = settings.DATASOURCE_PRIVATE_KEY_PASSPHRASE


    # Validate that username and password are provided for snowflake connections
    if not settings.DATASOURCE_DB_SNOWFLAKE_WAREHOUSE or not settings.DATASOURCE_DB_SNOWFLAKE_ROLE:
        raise ValueError(
            "DATASOURCE_DB_SNOWFLAKE_WAREHOUSE and DATASOURCE_DB_SNOWFLAKE_ROLE are required when using snowflake"
        )
    # Validate that username and password are provided for encrypted snowflake connections
    if not settings.DATASOURCE_PRIVATE_KEY_PATH or not settings.DATASOURCE_PRIVATE_KEY_PASSPHRASE:
        raise ValueError(
            "DATASOURCE_PRIVATE_KEY_PATH and DATASOURCE_PRIVATE_KEY_PASSPHRASE are required when using snowflake"
        )
    return SnowflakeDBClient(
        username=settings.DATASOURCE_DB_USERNAME,
        account=settings.DATASOURCE_DB_ACCOUNT,
        warehouse=settings.DATASOURCE_DB_SNOWFLAKE_WAREHOUSE,
        database=settings.DATASOURCE_DB_DATABASE,
        schema=settings.DATASOURCE_DB_SCHEMA,
        password=settings.DATASOURCE_DB_PASSWORD,
        private_key_path=settings.DATASOURCE_PRIVATE_KEY_PATH,
        private_key_passphrase=settings.DATASOURCE_PRIVATE_KEY_PASSPHRASE,
        role=settings.DATASOURCE_DB_SNOWFLAKE_ROLE

    )



@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(60),
    before_sleep=before_sleep_log(logger, INFO),
    after=after_log(logger, INFO),
)
def get_db_client() -> BaseDBClient:
    """Get the appropriate database client based on configuration."""
    logger.info("Connecting to database...")

    try:
        if settings.DATASOURCE_USE_TRINO:
            return _create_trino_client()
        elif settings.DATASOURCE_USE_AZURE_MANAGED_IDENTITY:
            return _create_azure_client()
        elif settings.DATASOURCE_DB_DRIVERNAME == "duckdb":
            return _create_duckdb_client()
        elif settings.DATASOURCE_USE_SNOWFLAKE:
            return _create_snowflake_client()
        else:
            return _create_sync_client()
    except TypeError as e:
        logger.error(str(e))
        exit()


__all__ = [
    "BaseDBClient",
    "SyncDBClient",
    "TrinoDBClient",
    "AzureManagedIdentityDBClient",
    "SnowflakeDBClient",
    "get_db_client",
    "DEFAULT_TRINO_PORT",
    "expand_short_drivers",
]
