from typing import Any, Sequence, Mapping
from sqlalchemy import create_engine
from sqlalchemy.inspection import inspect
from sqlalchemy.engine import URL as SQLAURL, Row, Engine
from sqlalchemy.sql import Executable
from hutch_bunny.core.logger import logger
from hutch_bunny.core.settings import Settings
from .base import BaseDBClient

settings = Settings()


class SyncDBClient(BaseDBClient):
    def __init__(
        self,
        username: str,
        password: str,
        host: str,
        port: int,
        database: str,
        drivername: str,
        schema: str | None = None,
        query: Mapping[str, str | Sequence[str]] | None = None,
    ) -> None:
        url = SQLAURL.create(
            drivername=drivername,
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
            query=query
        )

        self.schema = schema if schema is not None and len(schema) > 0 else None
        self._engine = create_engine(url=url)

        if self.schema is not None:
            self._engine.update_execution_options(
                schema_translate_map={None: self.schema}
            )

        self._inspector = inspect(self._engine)

        self._check_tables_exist()
        self._check_indexes_exist()

    @property
    def engine(self) -> Engine:
        return self._engine

    @property
    def inspector(self) -> Any:
        return self._inspector

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
        table_names = self.inspector.get_table_names(schema=self.schema)
        if not isinstance(table_names, list):
            raise TypeError("Expected a list of table names")
        return table_names
