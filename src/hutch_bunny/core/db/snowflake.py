import snowflake.connector
from sqlalchemy import create_engine
from sqlalchemy.inspection import inspect
from sqlalchemy.engine import Row, Engine
from sqlalchemy.sql import Executable
from typing import Any, Sequence
from snowflake.sqlalchemy import URL as SnowflakeURL

try:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False
    default_backend = None  # type: ignore
    serialization = None  # type: ignore

from hutch_bunny.core.logger import logger
from .base import BaseDBClient


class SnowflakeDBClient(BaseDBClient):
    def __init__(
        self,
        username: str,
        account: str,
        warehouse: str,
        database: str,
        schema: str | None = None,
        password: str | None = None,
        private_key_path: str | None = None,
        private_key_passphrase: str | None = None,
        role: str | None = None,
    ) -> None:
        """Create a DB client that interacts with Snowflake.
        Supports both password and key pair authentication.
        Args:
            username (str): The username for the Snowflake account.
            account (str): The Snowflake account identifier (e.g., 'LGGOZEC-CJ54726').
            warehouse (str): The Snowflake warehouse to use.
            database (str): The name of the Snowflake database.
            schema (str | None): (optional) The schema in the database.
            password (str | None): The password for password authentication.
            private_key_path (str | None): Path to the private key file (.p8) for key pair auth.
            private_key_passphrase (str | None): Passphrase for the encrypted private key.
            role (str | None): (optional) The Snowflake role to use.
        """
        self.username = username
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema if schema is not None and len(schema) > 0 else None
        self.role = role

        # Determine authentication method
        if private_key_path:
            logger.info("Using key pair authentication for Snowflake")
            self._private_key_bytes = self._load_private_key(
                private_key_path,
                private_key_passphrase
            )
            self._password = None
        elif password:
            logger.info("Using password authentication for Snowflake")
            self._private_key_bytes = None
            self._password = password
        else:
            raise ValueError(
                "Either password or private_key_path must be provided for Snowflake authentication"
            )

        # Create SQLAlchemy engine
        self._engine = self._create_engine()
        self._inspector = inspect(self._engine)

        self._check_tables_exist()
        self._check_indexes_exist()

    def _load_private_key(self, key_path: str, passphrase: str | None) -> bytes:
        """Load and decrypt the private key file.

        Args:
            key_path (str): Path to the private key file (.p8).
            passphrase (str | None): Passphrase for the encrypted private key.

        Returns:
            bytes: The decrypted private key in DER format.

        Raises:
            FileNotFoundError: If the key file is not found.
            ValueError: If the passphrase is incorrect or key format is invalid.
        """
        if not CRYPTOGRAPHY_AVAILABLE:
            raise ImportError(
                "cryptography library is required for key pair authentication. "
                "Install it with: pip install cryptography"
            )

        try:
            logger.debug(f"Loading private key from: {key_path}")

            with open(key_path, "rb") as key_file:
                p_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=passphrase.encode() if passphrase else None,
                    backend=default_backend()
                )

            # Serialize to DER format for Snowflake connector
            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )

            logger.info(" Private key loaded and decrypted successfully")
            return pkb

        except FileNotFoundError:
            raise FileNotFoundError(
                f"Private key file not found at: {key_path}. "
                "Please ensure the path is correct and the file exists."
            )
        except Exception as e:
            if "Bad decrypt" in str(e) or "incorrect password" in str(e).lower():
                raise ValueError(
                    "Failed to decrypt private key. The passphrase may be incorrect."
                ) from e
            raise ValueError(f"Error loading private key: {e}") from e

    def _create_engine(self) -> Engine:
        """Create SQLAlchemy engine with appropriate authentication using SnowflakeURL."""

        # Build connection parameters dictionary for connect_args
        # These get passed directly to snowflake.connector.connect()
        connect_args = {
            'user': self.username,
            'account': self.account,
            'warehouse': self.warehouse,
            'database': self.database,
        }

        if self.schema:
            connect_args['schema'] = self.schema

        if self.role:
            connect_args['role'] = self.role

        # Add authentication
        if self._private_key_bytes:
            connect_args['private_key'] = self._private_key_bytes
            logger.debug("  Auth: Private key")
        else:
            connect_args['password'] = self._password
            logger.debug("  Auth: Password")

        # Create a minimal SnowflakeURL - the real connection params are in connect_args
        # Using account as the "host" parameter for the URL
        sf_url = SnowflakeURL(
            account=self.account,
            user=self.username,
            database=self.database,
            schema=self.schema,
            warehouse=self.warehouse,
        )

        logger.debug(f"  SnowflakeURL: {sf_url}")
        logger.debug(f"  connect_args keys: {list(connect_args.keys())}")

        # Create engine
        # The connect_args will be passed to snowflake.connector.connect()
        engine = create_engine(
            sf_url,
            connect_args=connect_args,
            pool_pre_ping=True,
        )

        # Set default schema for queries
        if self.schema is not None:
            engine.update_execution_options(
                schema_translate_map={None: self.schema}
            )

        logger.info("✅ SQLAlchemy Engine for Snowflake created successfully!")

        # Test the connection immediately
        try:
            logger.info("Testing connection...")
            with engine.connect() as conn:
                result = conn.exec_driver_sql(
                    "SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE()")
                row = result.fetchone()
                logger.info(f"  Connected as user: {row[0]}")
                logger.info(f"  Current database: {row[1]}")
                logger.info(f"  Current schema: {row[2]}")
                logger.info(f"  Current role: {row[3]}")
                logger.info("✅ Connection test successful!")
        except Exception as e:
            logger.error(f"❌ Connection test failed: {e}")
            raise RuntimeError(
                f"Failed to connect to Snowflake. Please verify:\n"
                f"1. Account identifier is correct: {self.account}\n"
                f"2. User '{self.username}' exists and has access\n"
                f"3. Private key/password is correct\n"
                f"4. Role '{self.role}' is valid and assigned to user\n"
                f"5. Warehouse '{self.warehouse}' exists and is accessible\n"
                f"Original error: {e}"
            ) from e

        return engine

    @property
    def engine(self) -> Engine:
        return self._engine

    @property
    def inspector(self) -> Any:
        return self._inspector

    def _check_tables_exist(self) -> None:
        """
        Check if all required tables or views exist in the Snowflake database.

        Raises:
            RuntimeError: Raised when the required tables/views are missing.
        """
        required_tables = {
            "CONCEPT",
            "PERSON",
            "MEASUREMENT",
            "CONDITION_OCCURRENCE",
            "OBSERVATION",
            "DRUG_EXPOSURE",
        }

        logger.info(f"Checking tables in Snowflake...")
        logger.info(f"  Database: {self.database}")
        logger.info(f"  Schema: {self.schema}")
        logger.info(f"  Username: {self.username}")
        logger.info(f"  Role: {self.role}")

        try:
            with self.engine.connect() as conn:
                # First, let's see what schemas actually exist in this database

                logger.info(f"SHOW SCHEMAS IN DATABASE {self.database}")
                schemas_query = f"SHOW SCHEMAS IN DATABASE {self.database}"
                result = conn.exec_driver_sql(schemas_query)
                schemas = [row[1] for row in result.fetchall()]  # Schema name is usually in column 1
                logger.info(f"  Available schemas: {schemas}")

                # Check if our target schema exists
                schema_upper = self.schema.upper() if self.schema else None
                if schema_upper not in schemas:
                    raise RuntimeError(
                        f"Schema '{self.schema}' does not exist in database '{self.database}'.\n"
                        f"Available schemas: {', '.join(schemas)}\n"
                        f"Note: Schema names are case-sensitive. Make sure DATASOURCE_DB_SCHEMA matches exactly."
                    )

                logger.info(f"Step 2: Schema '{self.schema}' found! Querying tables...")

                # Query INFORMATION_SCHEMA for tables and views
                query = f"""
                    SELECT TABLE_NAME, TABLE_TYPE
                    FROM {self.database}.INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = '{schema_upper}'
                    AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
                """
                logger.debug(f"Executing query: {query}")
                result = conn.exec_driver_sql(query)
                rows = result.fetchall()

                existing_objects = {row[0].upper() for row in rows}

                logger.info(f"  Found {len(existing_objects)} tables/views: {sorted(existing_objects)}")

        except Exception as e:
            logger.error(f"Failed to retrieve tables and views from Snowflake: {e}")
            logger.error(f"Database: {self.database}, Schema: {self.schema}")
            raise RuntimeError(
                f"Unable to retrieve tables from Snowflake.\n"
                f"Database: {self.database}\n"
                f"Schema: {self.schema}\n"
                f"Please verify:\n"
                f"1. The schema name is correct (check DATASOURCE_DB_SCHEMA setting)\n"
                f"2. User '{self.username}' has USAGE privilege on the schema\n"
                f"3. Role '{self.role}' has access to the schema\n"
                f"Original error: {e}"
            ) from e

        if missing_tables := required_tables - existing_objects:
            # Check case-insensitive
            missing_tables_lower = {table.lower() for table in missing_tables}
            existing_objects_lower = {obj.lower() for obj in existing_objects}
            still_missing = missing_tables_lower - existing_objects_lower

            if still_missing:
                raise RuntimeError(
                    f"Missing tables or views in Snowflake '{self.database}.{self.schema}': "
                    f"{', '.join(still_missing)}\n"
                    f"Found: {', '.join(sorted(existing_objects))}"
                )

        logger.info(f"✅ All required tables found in Snowflake database")

    def _check_indexes_exist(self) -> None:
        """
        Snowflake uses clustering keys instead of traditional indexes.
        Index checking is not applicable for Snowflake.
        """
        logger.debug("Index checking skipped for Snowflake (uses clustering keys)")
        pass

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
        # Don't pass schema - use connection default
        table_names = self.inspector.get_table_names()
        if not isinstance(table_names, list):
            raise TypeError("Expected a list of table names")
        return table_names
