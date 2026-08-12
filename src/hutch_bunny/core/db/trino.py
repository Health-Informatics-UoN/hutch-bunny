from collections.abc import Sequence
from typing import Any

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine, Row
from sqlalchemy.sql import Executable
from trino.sqlalchemy import URL as TrinoURL  # type: ignore

from .base import BaseDBClient


class TrinoDBClient(BaseDBClient):
    def __init__(
        self,
        username: str,
        host: str,
        port: int,
        catalog: str,
        password: str | None = None,
        drivername: str | None = None,
        schema: str | None = None,
        database: str | None = None,
    ) -> None:
        """Create a DB client that interacts with Trino.

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

        self._engine = create_engine(url, connect_args={"http_scheme": "http"})
        self._inspector = inspect(self._engine)

    @property
    def engine(self) -> Engine:
        return self._engine

    @property
    def inspector(self) -> Any:
        return self._inspector

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
        table_names = self.inspector.get_table_names()
        if not isinstance(table_names, list):
            raise TypeError("Expected a list of table names")
        return table_names
