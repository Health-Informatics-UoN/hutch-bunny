import logging
from os import environ
from dotenv import load_dotenv

load_dotenv()


##
# DB Connection Settings
#   Additional settings reads are in `setting_database.py`
##

DATASOURCE_USE_TRINO = bool(environ.get("DATASOURCE_USE_TRINO", False))

# what unqualified `postgresql` will turn into. if left blank, will use SQLalchemy's default of `postgresql+psycopg2`
DEFAULT_POSTGRES_DRIVER = "postgresql+psycopg"

# what unqualified `mssql` will turn into. if left blank, will use SQLalchemy's default of `mssql+pymssql`
DEFAULT_MSSQL_DRIVER = "mssql+pymssql"

# what SQLAlchemy will use if DATASOURCE_DB_DRIVERNAME is not specified in the environment
DEFAULT_DB_DRIVER = DEFAULT_POSTGRES_DRIVER

# Logging configuration
LOGGER_NAME = "hutch"
LOGGER_LEVEL = logging.getLevelNamesMapping().get(
    environ.get("BUNNY_LOGGER_LEVEL") or "INFO", "INFO"
)
BACKUP_LOGGER_NAME = "backup"
MSG_FORMAT = "%(levelname)s - %(asctime)s - %(message)s"
DATE_FORMAT = "%d-%b-%y %H:%M:%S"

TASK_API_BASE_URL = environ.get("TASK_API_BASE_URL")
TASK_API_USERNAME = environ.get("TASK_API_USERNAME")
TASK_API_PASSWORD = environ.get("TASK_API_PASSWORD")
TASK_API_TYPE = environ.get("TASK_API_TYPE")
if TASK_API_TYPE and TASK_API_TYPE not in ["a", "b", "c"]:
    raise TypeError("TASK_API_TYPE must be either 'a' or 'b' or 'c'")

LOW_NUMBER_SUPPRESSION_THRESHOLD = environ.get("LOW_NUMBER_SUPPRESSION_THRESHOLD")
ROUNDING_TARGET = environ.get("ROUNDING_TARGET")


POLLING_INTERVAL_DEFAULT = 5
### currently no guards to ensure that POLLING_INTERVAL and POLLING_TIMEOUT are >=0
POLLING_INTERVAL = int(environ.get("POLLING_INTERVAL", POLLING_INTERVAL_DEFAULT))

if POLLING_INTERVAL < 0:
    print("POLLING_INTERVAL must be a positive integer. Setting to default 5s...")
    POLLING_INTERVAL = POLLING_INTERVAL_DEFAULT

COLLECTION_ID = environ.get("COLLECTION_ID")

try:
    BUNNY_VERSION = version("hutch_bunny")
except Exception:
    BUNNY_VERSION = "unknown"


def log_settings():
    from hutch_bunny.core.logger import (
        logger,
    )  # This is here to prevent a circular import

    logger.debug("Running with settings:")
    logger.debug(f"  BUNNY VERSION: {BUNNY_VERSION}")
    logger.debug(f"  DATASOURCE_USE_TRINO: {DATASOURCE_USE_TRINO}")
    logger.debug(f"  DEFAULT_POSTGRES_DRIVER: {DEFAULT_POSTGRES_DRIVER}")
    logger.debug(f"  DEFAULT_DB_DRIVER: {DEFAULT_DB_DRIVER}")
    logger.debug(f"  LOGGER_NAME: {LOGGER_NAME}")
    logger.debug(f"  LOGGER_LEVEL: {LOGGER_LEVEL}")
    logger.debug(f"  BACKUP_LOGGER_NAME: {BACKUP_LOGGER_NAME}")
    logger.debug(f"  MSG_FORMAT: {MSG_FORMAT}")
    logger.debug(f"  DATE_FORMAT: {DATE_FORMAT}")
    logger.debug(f"  TASK_API_BASE_URL: {TASK_API_BASE_URL}")
    logger.debug(f"  TASK_API_USERNAME: {TASK_API_USERNAME}")
    logger.debug(f"  TASK_API_PASSWORD: {TASK_API_PASSWORD}")
    logger.debug(f"  TASK_API_TYPE: {TASK_API_TYPE}")
    logger.debug(
        f"  LOW_NUMBER_SUPPRESSION_THRESHOLD: {LOW_NUMBER_SUPPRESSION_THRESHOLD}"
    )
    logger.debug(f"  ROUNDING_TARGET: {ROUNDING_TARGET}")
    logger.debug(f"  POLLING_INTERVAL_DEFAULT: {POLLING_INTERVAL_DEFAULT}")
    logger.debug(f"  POLLING_INTERVAL: {POLLING_INTERVAL}")
    logger.debug(f"  COLLECTION_ID: {COLLECTION_ID}")
