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
