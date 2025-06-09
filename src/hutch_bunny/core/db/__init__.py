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

from .base import BaseDBManager
from .sync import SyncDBManager
from .managed import ManagedIdentityDBManager
from .trino import TrinoDBManager
from .utils import (
    DEFAULT_TRINO_PORT,
    expand_short_drivers,
)

settings = Settings()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(60),
    before_sleep=before_sleep_log(logger, INFO),
    after=after_log(logger, INFO),
)
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
        datasource_db_drivername = expand_short_drivers(
            settings.DATASOURCE_DB_DRIVERNAME
        )

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


__all__ = [
    "BaseDBManager",
    "SyncDBManager",
    "ManagedIdentityDBManager",
    "TrinoDBManager",
    "get_db_manager",
    "DEFAULT_TRINO_PORT",
    "expand_short_drivers",
]
