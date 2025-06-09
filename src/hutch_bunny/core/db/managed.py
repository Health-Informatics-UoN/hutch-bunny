from typing import Any
from sqlalchemy import create_engine, inspect, event
from sqlalchemy.engine import URL as SQLAURL
from azure.identity import DefaultAzureCredential
from .sync import SyncDBManager


class ManagedIdentityDBManager(SyncDBManager):
    def __init__(
        self,
        username: str,
        host: str,
        port: int,
        database: str,
        drivername: str,
        managed_identity_client_id: str,
        schema: str | None = None,
    ) -> None:
        """Constructor method for ManagedIdentityDBManager.
        Creates the connection engine and the inspector for the database using managed identity authentication.

        Args:
            username (str): The username for the database.
            host (str): The host for the database.
            port (int): The port number for the database.
            database (str): The name of the database.
            drivername (str): The database driver e.g. "psycopg2", "pymysql", etc.
            managed_identity_client_id (str): The client ID for managed identity.
            schema (str | None): Optional schema name.
        """
        # Create URL without password
        url = SQLAURL.create(
            drivername=drivername,
            username=username,
            host=host,
            port=port,
            database=database,
        )

        self.schema = schema if schema is not None and len(schema) > 0 else None
        self.engine = create_engine(url=url)

        # Set up managed identity authentication
        self.managed_identity_client_id = managed_identity_client_id
        self._setup_managed_identity_auth()

        if self.schema is not None:
            self.engine.update_execution_options(
                schema_translate_map={None: self.schema}
            )

        self.inspector = inspect(self.engine)

        self._check_tables_exist()
        self._check_indexes_exist()

    def _setup_managed_identity_auth(self) -> None:
        """Set up the managed identity authentication event listener."""

        @event.listens_for(self.engine, "do_connect")
        def do_connect(
            dialect: Any, conn_rec: Any, cargs: Any, cparams: dict[str, Any]
        ) -> None:
            credential = DefaultAzureCredential()
            token = credential.get_token("https://database.windows.net/.default")
            cparams["password"] = token.token
