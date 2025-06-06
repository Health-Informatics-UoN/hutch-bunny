from typing import Any, Optional, Sequence
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import URL as SQLAURL
from trino.sqlalchemy import URL as TrinoURL  # type: ignore
from hutch_bunny.core.logger import logger
from hutch_bunny.core.settings import Settings
from sqlalchemy.engine import Row
from sqlalchemy.sql import Executable
from typing import ParamSpec, TypeVar

settings = Settings()

P = ParamSpec("P")
R = TypeVar("R")


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
        Check if all required tables or views exist in the database.

        Args:
            None

        Returns:
            None

        Raises:
            RuntimeError: Raised when the required tables/views are missing.
        """
        required_tables = {
            "concept",
            "person",
            "measurement",
            "condition_occurrence",
            "observation",
            "drug_exposure",
        }

        # Get both tables and views and combine
        existing_tables = set(self.inspector.get_table_names(schema=self.schema))
        existing_views = set(self.inspector.get_view_names(schema=self.schema))
        existing_objects = existing_tables.union(existing_views)

        if missing_tables := required_tables - existing_objects:
            raise RuntimeError(
                f"Missing tables or views in the database: {', '.join(missing_tables)}"
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

    def execute_and_fetch(self, stmnt: Executable) -> Sequence[Row[Any]]:  # type: ignore
        with self.engine.begin() as conn:
            result = conn.execute(statement=stmnt)
            rows = result.all()
        self.engine.dispose()
        return rows

    def execute(self, stmnt: Executable) -> None:
        with self.engine.begin() as conn:
            conn.execute(statement=stmnt)
        self.engine.dispose()

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
