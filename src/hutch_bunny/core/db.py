from hutch_bunny.core.logger import logger
from hutch_bunny.core.db_manager import SyncDBManager, TrinoDBManager

from hutch_bunny.core.settings import get_settings

settings = get_settings()


# These are db specific constants, not intended for users to override,
# here to avoid magic strings and provide clarity / ease of change in future.
DEFAULT_TRINO_PORT = 8080
POSTGRES_SHORT_NAME = "postgresql"
MSSQL_SHORT_NAME = "mssql"
DEFAULT_POSTGRES_DRIVER = f"{POSTGRES_SHORT_NAME}+psycopg"
DEFAULT_MSSQL_DRIVER = f"{MSSQL_SHORT_NAME}+pymssql"

def expand_short_drivers(drivername: str):
    """
    Expand unqualified "short" db driver names when necessary so we can override sqlalchemy
    e.g. when using psycopg3, expand `postgresql` explicitly rather than use sqlalchemy's default of psycopg2
    """

    if drivername == POSTGRES_SHORT_NAME:
        return DEFAULT_POSTGRES_DRIVER

    if drivername == MSSQL_SHORT_NAME:
        return DEFAULT_MSSQL_DRIVER

    # Add other explicit driver qualification as needed ...

    return drivername


def get_db_manager() -> SyncDBManager | TrinoDBManager:
    logger.info("Connecting to database...")

    # Trino has some different settings / defaults compared with SQLAlchemy
    if settings.DATASOURCE_USE_TRINO:
        datasource_db_port = settings.DATASOURCE_DB_PORT or DEFAULT_TRINO_PORT
        try:
            return TrinoDBManager(
                username=settings.DATASOURCE_DB_USERNAME,
                password=settings.DATASOURCE_DB_PASSWORD,
                host=settings.DATASOURCE_DB_HOST,
                port=settings.DATASOURCE_DB_PORT,
                schema=settings.DATASOURCE_DB_SCHEMA,
                catalog=settings.DATASOURCE_DB_CATALOG,
            )
        except TypeError as e:
            logger.error(str(e))
            exit()
    else:
        datasource_db_port = settings.DATASOURCE_DB_PORT
        datasource_db_drivername = expand_short_drivers(settings.DATASOURCE_DB_DRIVERNAME)

        try:
            return SyncDBManager(
                username=settings.DATASOURCE_DB_USERNAME,
                password=settings.DATASOURCE_DB_PASSWORD,
                host=settings.DATASOURCE_DB_HOST,
                port=(
                    int(datasource_db_port) if datasource_db_port is not None else None
                ),
                database=settings.DATASOURCE_DB_DATABASE,
                drivername=datasource_db_drivername,
                schema=settings.DATASOURCE_DB_SCHEMA,
            )
        except TypeError as e:
            logger.error(str(e))
            exit()
