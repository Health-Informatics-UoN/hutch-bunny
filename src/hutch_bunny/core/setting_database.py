from logging import Logger
from os import environ
from hutch_bunny.core.db_manager import SyncDBManager, TrinoDBManager
import hutch_bunny.core.settings as settings


def setting_database(logger: Logger):
    logger.info("Setting up database connection...")

    # Trino has some different settings / defaults comapred with SQLAlchemy
    if settings.DATASOURCE_USE_TRINO:
        datasource_db_port = environ.get("DATASOURCE_DB_PORT", 8080)
        try:
            db_manager = TrinoDBManager(
                username=environ.get("DATASOURCE_DB_USERNAME", "trino-user"),
                password=environ.get("DATASOURCE_DB_PASSWORD"),
                host=environ.get("DATASOURCE_DB_HOST"),
                port=int(datasource_db_port),
                schema=environ.get("DATASOURCE_DB_SCHEMA"),
                catalog=environ.get("DATASOURCE_DB_CATALOG"),
            )
        except TypeError as e:
            logger.error(str(e))
            exit()
    else:
        datasource_db_port = environ.get("DATASOURCE_DB_PORT")
        datasource_db_drivername = environ.get(
            "DATASOURCE_DB_DRIVERNAME", settings.DEFAULT_DB_DRIVER
        )

        # expand postgres to a full default driver, so we can override sqlalchemy
        if datasource_db_drivername == "postgresql":
            datasource_db_drivername = settings.DEFAULT_POSTGRES_DRIVER
        
        if datasource_db_drivername == "mssql":
            datasource_db_drivername = settings.DEFAULT_MSSQL_DRIVER

        try:
            db_manager = SyncDBManager(
                username=environ.get("DATASOURCE_DB_USERNAME"),
                password=environ.get("DATASOURCE_DB_PASSWORD"),
                host=environ.get("DATASOURCE_DB_HOST"),
                port=(
                    int(datasource_db_port) if datasource_db_port is not None else None
                ),
                database=environ.get("DATASOURCE_DB_DATABASE"),
                drivername=datasource_db_drivername,
                schema=environ.get("DATASOURCE_DB_SCHEMA"),
            )
        except TypeError as e:
            logger.error(str(e))
            exit()

    return db_manager
