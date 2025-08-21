from typing import Any, Sequence
from sqlalchemy.sql import Executable
from sqlalchemy.engine import Row, Engine
from typing import ParamSpec, TypeVar
from abc import abstractmethod

P = ParamSpec("P")
R = TypeVar("R")


class BaseDBClient:
    def __init__(
        self,
        username: str,
        password: str,
        host: str,
        port: int,
        database: str,
        drivername: str,
    ) -> None:
        """Constructor method for DB client classes.
        Creates the connection engine and the inspector for the database.

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

    @property
    @abstractmethod
    def engine(self) -> Engine:
        """The SQLAlchemy engine instance.

        Returns:
            Engine: The SQLAlchemy engine instance.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def inspector(self) -> Any:
        """The SQLAlchemy inspector instance.

        Returns:
            Any: The SQLAlchemy inspector instance.
        """
        raise NotImplementedError

    @abstractmethod
    def execute_and_fetch(self, stmnt: Executable) -> Sequence[Row[Any]]:
        """Execute a statement against the database and fetch the result.

        Args:
            stmnt (Any): The statement object to be executed.

        Raises:
            NotImplementedError: Raised when this method has not been implemented in subclass.

        Returns:
            Sequence[Row[Any]]: The sequence of rows returned.
        """
        raise NotImplementedError

    @abstractmethod
    def execute(self, stmnt: Executable) -> None:
        """Execute a statement against the database and don't fetch any results.

        Args:
            stmnt (Any): The statement object to be executed.

        Raises:
            NotImplementedError: Raised when this method has not been implemented in subclass.
        """
        raise NotImplementedError

    @abstractmethod
    def list_tables(self) -> list[str]:
        """List the tables in the database.

        Raises:
            NotImplementedError: Raised when this method has not been implemented in subclass.

        Returns:
            list[str]: The list of tables in the database.
        """
        raise NotImplementedError
