import time
from typing import Any, Optional, Sequence
from functools import wraps

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import URL as SQLAURL
from sqlalchemy.exc import OperationalError
from trino.sqlalchemy import URL as TrinoURL  # type: ignore
from hutch_bunny.core.logger import logger
from hutch_bunny.core.settings import get_settings
from sqlalchemy.engine import Row
from sqlalchemy.sql import Executable

settings = get_settings()


def WakeAzureDB(retries: int = 1, delay: int = 30, error_code: str = "40613") -> Any:
    """Decorator to retry a function on specific Azure DB wake-up errors.

    Args:
        retries (int): Number of retries before giving up. 1 retry
         is sufficient to wake an Azure DB.
        delay (int): Delay in seconds between retries. 30 seconds is
         enough time for the Azure DB to wake up.
        error_code (str): The error code to check for in the exception. 40613
         is the error code for an Azure DB that is asleep.

    Returns:
        Callable: The wrapped function with retry logic or the original
         function.
    """

    def decorator(func):
        if (
            settings.DATASOURCE_WAKE_DB is False
            and settings.DATASOURCE_DB_DRIVERNAME == "mssql"
        ):
            return func

        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(retries + 1):
                try:
                    return func(*args, **kwargs)
                except OperationalError as e:
                    error_msg = str(e)
                    if error_code in error_msg:
                        if attempt < retries:
                            logger.info(
                                f"{func.__name__} has called a sleeping DB, retrying in {delay} seconds..."
                            )
                            time.sleep(delay)
                        else:
                            raise e
                    else:
                        raise e

        return wrapper

    return decorator


class BaseDBManager:
    def __init__(
        self,
        username: str,
        password: str,
        host: str,
        port: int,
        database: str,
        drivername: str,
    ) -> None:
        """Constructor method for DBManager classes.
        Creates the connection engine and the inpector for the database.

        Args:
            username (str): The username for the database.
            password (str): The password for the database.
            host (str): The host for the database.
            port (int): The port number for the database.
            database (str): The name of the database.
            drivername (str): The database driver e.g. "psycopg2", "pymysql", etc.

        Raises:
            NotImplementedError: Raised when this method has not been implemented in subclass.
        """
        raise NotImplementedError

    def execute_and_fetch(self, stmnt: Executable) -> Sequence[Row[Any]]:  # type: ignore
        """Execute a statement against the database and fetch the result.

        Args:
            stmnt (Any): The statement object to be executed.

        Raises:
            NotImplementedError: Raised when this method has not been implemented in subclass.

        Returns:
            Sequence[Row[Any]]: The sequence of rows returned.
        """
        raise NotImplementedError

    def execute(self, stmnt: Executable) -> None:
        """Execute a statement against the database and don't fetch any results.

        Args:
            stmnt (Any): The statement object to be executed.

        Raises:
            NotImplementedError: Raised when this method has not been implemented in subclass.
        """
        raise NotImplementedError

    def list_tables(self) -> list[str]:
        """List the tables in the database.

        Raises:
            NotImplementedError: Raised when this method has not been implemented in subclass.

        Returns:
            list[str]: The list of tables in the database.
        """
        raise NotImplementedError


class SyncDBManager(BaseDBManager):
    @WakeAzureDB()
    def __init__(
        self,
        username: str,
        password: str,
        host: str,
        port: int,
        database: str,
        drivername: str,
        schema: str | None = None,
    ) -> None:
        url = SQLAURL.create(
            drivername=drivername,
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
        )

        self.schema = schema if schema is not None and len(schema) > 0 else None
        self.engine = create_engine(url=url)

        if self.schema is not None:
            self.engine.update_execution_options(
                schema_translate_map={None: self.schema}
            )

        self.inspector = inspect(self.engine)

        self._check_tables_exist()
        self._check_indexes_exist()

    def _check_tables_exist(self) -> None:
        """
        Check if all required tables exist in the database.

        Args:
            None

        Returns:
            None

        Raises:
            RuntimeError: Raised when the tables are missing.
        """
        required_tables = {
            "concept",
            "person",
            "measurement",
            "condition_occurrence",
            "observation",
            "drug_exposure",
        }
        existing_tables = set(self.inspector.get_table_names(schema=self.schema))
        missing_tables = required_tables - existing_tables

        if missing_tables:
            raise RuntimeError(
                f"Missing tables in the database: {', '.join(missing_tables)}"
            )

    def _check_indexes_exist(self) -> None:
        """
        Check if all required indexes exist in the database.
        Warning is logged if any indexes are missing.

        Args:
            None

        Returns:
            None
        """
        # Based on query data so far, these are the most common indexes.
        required_indexes = {
            "person": ["idx_person_id"],
            "concept": ["idx_concept_concept_id"],
            "condition_occurrence": ["idx_condition_concept_id_1"],
            "observation": ["idx_observation_concept_id_1"],
            "measurement": ["idx_measurement_concept_id_1"],
        }
        missing_indexes = {}
        for table, expected_indexes in required_indexes.items():
            existing_indexes = {
                idx["name"]
                for idx in self.inspector.get_indexes(table, schema=self.schema)
            }
            missing = set(expected_indexes) - existing_indexes
            if missing:
                missing_indexes[table] = missing

        if missing_indexes:
            logger.warning(
                (
                    "Missing indexes in the database: "
                    f"{', '.join(missing_indexes)}. "
                    "Queries will be slower and we recommend adding these indexes."
                )
            )

    @WakeAzureDB()
    def execute_and_fetch(self, stmnt: Executable) -> Sequence[Row[Any]]:  # type: ignore
        with self.engine.begin() as conn:
            result = conn.execute(statement=stmnt)
            rows = result.all()
        self.engine.dispose()
        return rows

    @WakeAzureDB()
    def execute(self, stmnt: Executable) -> None:
        with self.engine.begin() as conn:
            conn.execute(statement=stmnt)
        self.engine.dispose()

    @WakeAzureDB()
    def list_tables(self) -> list[str]:
        return self.inspector.get_table_names(schema=self.schema)


class TrinoDBManager(BaseDBManager):
    def __init__(
        self,
        username: str,
        host: str,
        port: int,
        catalog: str,
        password: Optional[str] = None,
        drivername: Optional[str] = None,
        schema: Optional[str] = None,
        database: Optional[str] = None,
    ) -> None:
        """Create a DB manager that interacts with Trino.

        Args:
            username (str): The username on the Trino server.
            password (Union[str, None]): (optional) The password for the Trino server.
            host (str): The host of the Trino server.
            port (int): The port of the Trino server.
            database (Union[str, None]): Ignored.
            drivername (str): (Union[str, None]): Ignored.
            schema (Union[str, None]): (optional) The schema in the database.
            catalog (str): The catalog on the Trino server.
        """
        url = TrinoURL(
            user=username,
            password=password,
            host=host,
            port=port,
            schema=schema,
            catalog=catalog,
        )

        self.engine = create_engine(url, connect_args={"http_scheme": "http"})
        self.inspector = inspect(self.engine)

    def execute_and_fetch(self, stmnt: Executable) -> Sequence[Row[Any]]:  # type: ignore
        with self.engine.begin() as conn:
            result = conn.execute(statement=stmnt)
            rows = result.all()
        # Need to call `dispose` - not automatic
        self.engine.dispose()
        return rows

    def execute(self, stmnt: Executable) -> None:
        with self.engine.begin() as conn:
            conn.execute(statement=stmnt)
        # Need to call `dispose` - not automatic
        self.engine.dispose()

    def list_tables(self) -> list[str]:
        return self.inspector.get_table_names()
