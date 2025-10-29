from sqlalchemy import create_engine, inspect
from .sync import SyncDBClient


class DuckDBClient(SyncDBClient):
    def __init__(
        self,
        path_to_db: str,
        duckdb_memory_limit: str,
        duckdb_temp_directory: str,
        schema: str | None = None,
    ) -> None:
        """Constructor method for DuckDBClient.
        Creates the connection engine and the inspector for the database using DuckDB in memory database.

        Args:
            path_to_db (str): The path to the duckdb database file.
            duckdb_memory_limit (str): The memory limit for duckdb (e.g. '1000mb', '2gb').
            schema (str | None): Optional schema name.
        """

        self.schema = schema if schema is not None and len(schema) > 0 else None

        self._engine = create_engine("duckdb:///" + path_to_db, connect_args={
            'read_only': True,
            'config': {
                'memory_limit': duckdb_memory_limit,
                'temp_directory': duckdb_temp_directory
            }
        })

        if self.schema is not None:
            self._engine.update_execution_options(
                schema_translate_map={None: self.schema}
            )

        self._inspector = inspect(self._engine)

        self._check_tables_exist()