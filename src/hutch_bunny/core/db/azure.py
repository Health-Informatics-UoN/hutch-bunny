import struct
from typing import Any
from sqlalchemy import create_engine, inspect, event
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.core.credentials import TokenCredential
from hutch_bunny.core.logger import logger
from .sync import SyncDBClient


class AzureManagedIdentityDBClient(SyncDBClient):
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        drivername: str,
        managed_identity_client_id: str,
        schema: str | None = None,
    ) -> None:
        """Constructor method for AzureManagedIdentityDBClient.
        Creates the connection engine and the inspector for the database using Azure managed identity authentication.

        Supports both user-assigned and system-assigned managed identities. When a client ID is provided,
        uses ManagedIdentityCredential; otherwise uses DefaultAzureCredential which will automatically
        detect and use the appropriate authentication method (system-assigned managed identity in Azure,
        Azure CLI tokens for local development, etc.).

        Args:
            username (str): The username for the database.
            host (str): The host for the database.
            port (int): The port number for the database.
            database (str): The name of the database.
            drivername (str): The ODBC driver name e.g. "{ODBC Driver 18 for SQL Server}".
            managed_identity_client_id (str | None): The client ID for user-assigned managed identity.
                                                   If None, uses system-assigned managed identity or Azure CLI.
            schema (str | None): Optional schema name.
        """
        # Create connection URL using ODBC format for Azure AD authentication
        from urllib.parse import quote

        url = f"Driver={drivername};Server=tcp:{host},{port};Database={database};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30"
        url = "mssql+pyodbc:///?odbc_connect={0}".format(quote(url))

        self.schema = schema if schema is not None and len(schema) > 0 else None
        self._engine = create_engine(url=url)

        # Set up Azure managed identity authentication
        self.managed_identity_client_id = managed_identity_client_id
        self._setup_azure_managed_identity_auth()

        if self.schema is not None:
            self._engine.update_execution_options(
                schema_translate_map={None: self.schema}
            )

        self._inspector = inspect(self._engine)

        self._check_tables_exist()
        self._check_indexes_exist()

    def _setup_azure_managed_identity_auth(self) -> None:
        """Set up the Azure managed identity authentication event listener."""

        @event.listens_for(self._engine, "do_connect")
        def do_connect(
            dialect: Any, conn_rec: Any, cargs: Any, cparams: dict[str, Any]
        ) -> None:
            # Get the appropriate credential
            credential: TokenCredential = (
                ManagedIdentityCredential(client_id=self.managed_identity_client_id)
                if self.managed_identity_client_id
                else DefaultAzureCredential()
            )

            # Get token and encode it for ODBC
            token_bytes = credential.get_token(
                "https://database.windows.net/.default"
            ).token.encode("UTF-16-LE")
            token = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

            # Use SQL_COPT_SS_ACCESS_TOKEN connection attribute for ODBC
            SQL_COPT_SS_ACCESS_TOKEN = 1256
            cparams["attrs_before"] = {SQL_COPT_SS_ACCESS_TOKEN: token}
            logger.debug("token added")
