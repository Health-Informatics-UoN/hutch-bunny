from typing import Any, Optional, Sequence
from sqlalchemy import create_engine, inspect
from trino.sqlalchemy import URL as TrinoURL  # type: ignore
from sqlalchemy.engine import Row
from sqlalchemy.sql import Executable
from .base import BaseDBManager


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
