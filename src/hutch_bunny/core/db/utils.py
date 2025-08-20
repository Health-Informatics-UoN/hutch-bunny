"""Database utilities and constants for Hutch Bunny."""

# These are db specific constants, not intended for users to override,
# here to avoid magic strings and provide clarity / ease of change in future.
DEFAULT_TRINO_PORT = 8080
POSTGRES_SHORT_NAME = "postgresql"
MSSQL_SHORT_NAME = "mssql"
DEFAULT_POSTGRES_DRIVER = f"{POSTGRES_SHORT_NAME}+psycopg"
DEFAULT_MSSQL_DRIVER = "{ODBC Driver 18 for SQL Server}"


def expand_short_drivers(drivername: str) -> str:
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
