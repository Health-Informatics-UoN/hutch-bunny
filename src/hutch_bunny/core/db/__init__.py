"""Database management module for Hutch Bunny."""

from hutch_bunny.core.logger import logger, INFO
from hutch_bunny.core.config.database import DatabaseSettings
from tenacity import (
    retry,
    stop_after_attempt,
    wait_fixed,
    before_sleep_log,
    after_log,
)

from .base import BaseDBClient
from .sync import SyncDBClient
from .trino import TrinoDBClient
from .azure import AzureManagedIdentityDBClient
from .duckdb import DuckDBClient
from .utils import (
    DEFAULT_TRINO_PORT,
    expand_short_drivers,
)


def _create_trino_client(settings: DatabaseSettings) -> TrinoDBClient:
    """Create a Trino database client."""
    datasource_db_port = settings.DATASOURCE_DB_PORT or DEFAULT_TRINO_PORT

    # Validate required fields for Trino
    if (
        not settings.DATASOURCE_DB_USERNAME
        or not settings.DATASOURCE_DB_PASSWORD
    ):
        raise ValueError(
            "DATASOURCE_DB_USERNAME and DATASOURCE_DB_PASSWORD are required for Trino"
        )

    if not settings.DATASOURCE_DB_HOST:
        raise ValueError("DATASOURCE_DB_HOST is required for Trino")

    return TrinoDBClient(
        username=settings.DATASOURCE_DB_USERNAME,
        password=settings.DATASOURCE_DB_PASSWORD,
        host=settings.DATASOURCE_DB_HOST,
        port=datasource_db_port,
        schema=settings.DATASOURCE_DB_SCHEMA,
        catalog=settings.DATASOURCE_DB_CATALOG,
    )


def _create_azure_client(
    settings: DatabaseSettings,
) -> AzureManagedIdentityDBClient:
    """Create an Azure managed identity database client."""
    datasource_db_port = settings.DATASOURCE_DB_PORT
    datasource_db_drivername = expand_short_drivers(
        settings.DATASOURCE_DB_DRIVERNAME,
        use_azure_managed_identity=True,
    )

    # Validate required fields for Azure
    if not settings.DATASOURCE_DB_HOST:
        raise ValueError("DATASOURCE_DB_HOST is required for Azure managed identity")
    if not settings.DATASOURCE_DB_DATABASE:
        raise ValueError(
            "DATASOURCE_DB_DATABASE is required for Azure managed identity"
        )
    if not datasource_db_port:
        raise ValueError("DATASOURCE_DB_PORT is required for Azure managed identity")

    return AzureManagedIdentityDBClient(
        host=settings.DATASOURCE_DB_HOST,
        port=int(datasource_db_port),
        database=settings.DATASOURCE_DB_DATABASE,
        drivername=datasource_db_drivername,
        managed_identity_client_id=settings.DATASOURCE_AZURE_MANAGED_IDENTITY_CLIENT_ID,
        schema=settings.DATASOURCE_DB_SCHEMA,
    )


def _create_duckdb_client(settings: DatabaseSettings) -> DuckDBClient:
    """Create an DuckDB client."""

    return DuckDBClient(
        path_to_db=settings.DATASOURCE_DUCKDB_PATH_TO_DB,
        duckdb_memory_limit=settings.DATASOURCE_DUCKDB_MEMORY_LIMIT,
        schema=settings.DATASOURCE_DB_SCHEMA,
        duckdb_temp_directory=settings.DATASOURCE_DUCKDB_TEMP_DIRECTORY,
    )


def _create_sync_client(settings: DatabaseSettings) -> SyncDBClient:
    """Create a regular synchronous database client."""
    datasource_db_port = settings.DATASOURCE_DB_PORT
    datasource_db_drivername = expand_short_drivers(
        settings.DATASOURCE_DB_DRIVERNAME,
        use_azure_managed_identity=False,
    )

    # Validate that username and password are provided for non-Azure connections
    if (
        not settings.DATASOURCE_DB_USERNAME
        or not settings.DATASOURCE_DB_PASSWORD
    ):
        raise ValueError(
            "DATASOURCE_DB_USERNAME and DATASOURCE_DB_PASSWORD are required when not using Azure managed identity"
        )

    if not settings.DATASOURCE_DB_HOST:
        raise ValueError("DATASOURCE_DB_HOST is required for sync client")
    if not settings.DATASOURCE_DB_DATABASE:
        raise ValueError("DATASOURCE_DB_DATABASE is required for sync client")
    if not datasource_db_port:
        raise ValueError("DATASOURCE_DB_PORT is required for sync client")

    return SyncDBClient(
        username=settings.DATASOURCE_DB_USERNAME,
        password=settings.DATASOURCE_DB_PASSWORD,
        host=settings.DATASOURCE_DB_HOST,
        port=int(datasource_db_port),
        database=settings.DATASOURCE_DB_DATABASE,
        drivername=datasource_db_drivername,
        schema=settings.DATASOURCE_DB_SCHEMA,
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(60),
    before_sleep=before_sleep_log(logger, INFO),
    after=after_log(logger, INFO),
)
def get_db_client() -> BaseDBClient:
    """
    Get the appropriate database client based on configuration.
    
    Returns:
        BaseDBClient: The appropriate database client.
    """
    settings = DatabaseSettings()
    logger.info("Connecting to database...")

    try:
        if settings.DATASOURCE_USE_TRINO:
            return _create_trino_client(settings)
        elif settings.DATASOURCE_USE_AZURE_MANAGED_IDENTITY:
            return _create_azure_client(settings)
        elif settings.DATASOURCE_DB_DRIVERNAME == "duckdb":
            return _create_duckdb_client(settings)
        else:
            return _create_sync_client(settings)
    except TypeError as e:
        logger.error(str(e))
        exit()


__all__ = [
    "BaseDBClient",
    "SyncDBClient",
    "TrinoDBClient",
    "AzureManagedIdentityDBClient",
    "get_db_client",
    "DEFAULT_TRINO_PORT",
    "expand_short_drivers",
]
