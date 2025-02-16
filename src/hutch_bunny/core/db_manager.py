from typing import Any, Optional

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import URL as SQLAURL
from trino.sqlalchemy import URL as TrinoURL  # TODO: how to do as optional?
from dotenv import load_dotenv

load_dotenv()


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

    def execute_and_fetch(self, stmnt: Any) -> list:
        """Execute a statement against the database and fetch the result.

        Args:
            stmnt (Any): The statement object to be executed.

        Raises:
            NotImplementedError: Raised when this method has not been implemented in subclass.

        Returns:
            list: The list of rows returned.
        """
        raise NotImplementedError

    def execute(self, stmnt: Any) -> None:
        """Execute a statement against the database and don't fetch any results.

        Args:
            stmnt (Any): The statement object to be executed.

        Raises:
            NotImplementedError: Raised when this method has not been implemented in subclass.
        """
        raise NotImplementedError

    def list_tables(self) -> list:
        """List the tables in the database.

        Raises:
            NotImplementedError: Raised when this method has not been implemented in subclass.

        Returns:
            list: The list of tables in the database.
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
        schema: Optional[str] = None,
        connect_args: Optional[dict] = None,
    ) -> None:
        if not isinstance(username, str):
            raise TypeError("`username` must be a string")
        if not isinstance(password, str):
            raise TypeError("`password` must be a string")
        if not isinstance(host, str):
            raise TypeError("`host` must be a string")
        if not isinstance(port, int):
            raise TypeError("`port` must be an integer")
        if not isinstance(database, str):
            raise TypeError("`database` must be a string")

        url = SQLAURL.create(
            drivername=drivername,
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
        )

        self.schema = schema if schema is not None and len(schema) > 0 else None

        if connect_args is not None:
            self.engine = create_engine(url=url, connect_args=connect_args)
        else:
            self.engine = create_engine(url=url)

        if self.schema is not None:
            self.engine.update_execution_options(
                schema_translate_map={None: self.schema}
            )

        self.inspector = inspect(self.engine)

    def execute_and_fetch(self, stmnt: Any) -> list:
        with self.engine.begin() as conn:
            result = conn.execute(statement=stmnt)
            rows = result.all()
        # Need to call `dispose` - not automatic
        self.engine.dispose()
        return rows

    def execute(self, stmnt: Any) -> None:
        with self.engine.begin() as conn:
            conn.execute(statement=stmnt)
        # Need to call `dispose` - not automatic
        self.engine.dispose()

    def list_tables(self) -> list:
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
        # check required args
        if not isinstance(username, str):
            raise TypeError("`username` must be a string")
        if not isinstance(host, str):
            raise TypeError("`host` must be a string")
        if not isinstance(port, int):
            raise TypeError("`port` must be an integer")
        if not isinstance(catalog, str):
            raise TypeError("`catalog` must be a string")

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

    def execute_and_fetch(self, stmnt: Any) -> list:
        """Execute a SQL statement and return a list of rows containing the
        results of the query.

        Args:
            stmnt (Any): The SQL statement to be executed.

        Returns:
            list: The results of the SQL statement.
        """
        with self.engine.begin() as conn:
            result = conn.execute(statement=stmnt)
            rows = result.all()
        # Need to call `dispose` - not automatic
        self.engine.dispose()
        return rows

    def execute(self, stmnt: Any) -> None:
        """Execute a SQL statement. Useful for when results aren't expected back, such as
        updating or deleting.

        Args:
            stmnt (Any): The SQL statement to be executed.
        """
        with self.engine.begin() as conn:
            conn.execute(statement=stmnt)
        # Need to call `dispose` - not automatic
        self.engine.dispose()

    def list_tables(self) -> list:
        """Get a list of tables in the database.

        Returns:
            list: The tables in the database.
        """
        return self.inspector.get_table_names()
