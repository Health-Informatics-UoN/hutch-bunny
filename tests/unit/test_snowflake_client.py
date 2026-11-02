"""
Tests for the Snowflake client's connection, authentication and checks, run against an
in-memory fakesnow database so no Snowflake account or OMOP data is needed.
"""

from pathlib import Path
from typing import Any, Iterator
from unittest.mock import patch

import fakesnow
import pytest
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
from sqlalchemy import text

from hutch_bunny.core.db import SnowflakeDBClient, _create_snowflake_client, get_db_client

DATABASE = "OMOP"
SCHEMA = "CDM"
REQUIRED_TABLES = [
    "CONCEPT",
    "PERSON",
    "MEASUREMENT",
    "CONDITION_OCCURRENCE",
    "OBSERVATION",
    "DRUG_EXPOSURE",
]


@pytest.fixture
def fake_snowflake() -> Iterator[None]:
    """An in-memory fakesnow database holding empty OMOP tables."""
    # The dialect parses SELECT CURRENT_VERSION(), which fakesnow answers with DuckDB's version
    with (
        fakesnow.patch(),
        patch.object(SnowflakeDialect, "_get_server_version_info", return_value=(9, 0, 0)),
    ):
        cursor = snowflake.connector.connect(database=DATABASE, schema=SCHEMA).cursor()
        for table in REQUIRED_TABLES:
            cursor.execute(f"CREATE TABLE {table} (id INT)")
        yield


@pytest.fixture
def private_key_path(tmp_path: Path) -> Path:
    """An encrypted RSA private key, as used for Snowflake key pair authentication."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    path = tmp_path / "private_key.p8"
    path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.BestAvailableEncryption(b"secret"),
        )
    )
    return path


def make_client(**kwargs: Any) -> SnowflakeDBClient:
    options: dict[str, Any] = {
        "username": "fake_user",
        "account": "fake_account",
        "warehouse": "fake_warehouse",
        "database": DATABASE,
        "schema": SCHEMA,
        "role": "fake_role",
        **kwargs,
    }
    return SnowflakeDBClient(**options)


@pytest.mark.unit
def test_password_auth_connects(fake_snowflake: None) -> None:
    client = make_client(password="fake_password")

    assert client._password == "fake_password"
    assert client._private_key_bytes is None
    assert "person" in client.list_tables()


@pytest.mark.unit
def test_key_pair_auth_loads_decrypted_key(fake_snowflake: None, private_key_path: Path) -> None:
    client = make_client(private_key_path=str(private_key_path), private_key_passphrase="secret")

    # The connector needs the key decrypted and in DER format
    key = serialization.load_der_private_key(client._private_key_bytes or b"", password=None)
    assert isinstance(key, rsa.RSAPrivateKey)
    assert client._password is None
    assert "person" in client.list_tables()


@pytest.mark.unit
def test_key_pair_auth_preferred_over_password(fake_snowflake: None, private_key_path: Path) -> None:
    client = make_client(
        password="fake_password",
        private_key_path=str(private_key_path),
        private_key_passphrase="secret",
    )

    assert client._private_key_bytes is not None
    assert client._password is None


@pytest.mark.unit
def test_wrong_key_passphrase_raises(private_key_path: Path) -> None:
    with pytest.raises(ValueError, match="passphrase may be incorrect"):
        make_client(private_key_path=str(private_key_path), private_key_passphrase="wrong")


@pytest.mark.unit
def test_missing_key_file_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="Private key file not found"):
        make_client(private_key_path=str(tmp_path / "missing.p8"), private_key_passphrase="secret")


@pytest.mark.unit
def test_invalid_key_file_raises(tmp_path: Path) -> None:
    key_path = tmp_path / "not_a_key.p8"
    key_path.write_text("not a private key")

    with pytest.raises(ValueError, match="Error loading private key"):
        make_client(private_key_path=str(key_path), private_key_passphrase="secret")

@pytest.mark.unit
def test_no_credentials_raises() -> None:
    with pytest.raises(ValueError, match="Either password or private_key_path must be provided"):
        make_client()


@pytest.mark.unit
def test_failed_connection_raises_with_guidance() -> None:
    with patch("snowflake.connector.connect", side_effect=Exception("account not reachable")):
        with pytest.raises(RuntimeError, match="Failed to connect to Snowflake") as exc_info:
            make_client(password="fake_password")

    assert "account not reachable" in str(exc_info.value)


@pytest.mark.unit
def test_missing_omop_tables_raises(fake_snowflake: None) -> None:
    # A database that has none of the OMOP tables
    with pytest.raises(RuntimeError, match="Missing tables or views in Snowflake") as exc_info:
        make_client(password="fake_password", database="EMPTY")

    assert "PERSON" in str(exc_info.value)


@pytest.mark.unit
def test_failure_listing_tables_raises(fake_snowflake: None) -> None:
    with patch("hutch_bunny.core.db.snowflake.inspect") as mock_inspect:
        mock_inspect.return_value.get_table_names.side_effect = Exception("insufficient privileges")
        with pytest.raises(RuntimeError, match="Unable to retrieve tables from Snowflake") as exc_info:
            make_client(password="fake_password")

    assert "insufficient privileges" in str(exc_info.value)

@pytest.mark.unit
def test_execute_and_fetch(fake_snowflake: None) -> None:
    client = make_client(password="fake_password")

    client.execute(text("CREATE TABLE extra (id INT)"))
    client.execute(text("INSERT INTO extra VALUES (1), (2)"))
    rows = client.execute_and_fetch(text("SELECT count(*) FROM extra"))

    assert [tuple(row) for row in rows] == [(2,)]


@pytest.mark.unit
def test_get_db_client_uses_snowflake_when_enabled() -> None:
    with (
        patch("hutch_bunny.core.db.settings.DATASOURCE_USE_TRINO", False),
        patch("hutch_bunny.core.db.settings.DATASOURCE_USE_AZURE_MANAGED_IDENTITY", False),
        patch("hutch_bunny.core.db.settings.DATASOURCE_DB_DRIVERNAME", "snowflake-connector-python"),
        patch("hutch_bunny.core.db.settings.DATASOURCE_USE_SNOWFLAKE", True),
        patch("hutch_bunny.core.db._create_snowflake_client") as mock_create,
    ):
        client = get_db_client()

    assert client is mock_create.return_value


@pytest.mark.unit
def test_create_snowflake_client_passes_settings() -> None:
    with (
        patch("hutch_bunny.core.db.settings.DATASOURCE_DB_USERNAME", "sf_user"),
        patch("hutch_bunny.core.db.settings.DATASOURCE_DB_ACCOUNT", "sf_account"),
        patch("hutch_bunny.core.db.settings.DATASOURCE_DB_DATABASE", "OMOP"),
        patch("hutch_bunny.core.db.settings.DATASOURCE_DB_SCHEMA", "CDM"),
        patch("hutch_bunny.core.db.settings.DATASOURCE_DB_SNOWFLAKE_WAREHOUSE", "sf_warehouse"),
        patch("hutch_bunny.core.db.settings.DATASOURCE_DB_SNOWFLAKE_ROLE", "sf_role"),
        patch("hutch_bunny.core.db.settings.DATASOURCE_PRIVATE_KEY_PATH", "/keys/key.p8"),
        patch("hutch_bunny.core.db.settings.DATASOURCE_PRIVATE_KEY_PASSPHRASE", "secret"),
        patch("hutch_bunny.core.db.SnowflakeDBClient") as mock_client,
    ):
        _create_snowflake_client()

    kwargs = mock_client.call_args.kwargs
    assert kwargs["username"] == "sf_user"
    assert kwargs["account"] == "sf_account"
    assert kwargs["database"] == "OMOP"
    assert kwargs["schema"] == "CDM"
    assert kwargs["warehouse"] == "sf_warehouse"
    assert kwargs["role"] == "sf_role"
    assert kwargs["private_key_path"] == "/keys/key.p8"
    assert kwargs["private_key_passphrase"] == "secret"


@pytest.mark.unit
@pytest.mark.parametrize(
    "setting, message",
    [
        ("DATASOURCE_DB_SNOWFLAKE_WAREHOUSE", "WAREHOUSE and DATASOURCE_DB_SNOWFLAKE_ROLE are required"),
        ("DATASOURCE_DB_SNOWFLAKE_ROLE", "WAREHOUSE and DATASOURCE_DB_SNOWFLAKE_ROLE are required"),
        ("DATASOURCE_PRIVATE_KEY_PATH", "PRIVATE_KEY_PATH and DATASOURCE_PRIVATE_KEY_PASSPHRASE are required"),
        ("DATASOURCE_PRIVATE_KEY_PASSPHRASE", "PRIVATE_KEY_PATH and DATASOURCE_PRIVATE_KEY_PASSPHRASE are required"),
    ],
)
def test_create_snowflake_client_requires_setting(setting: str, message: str) -> None:
    with (
        patch(f"hutch_bunny.core.db.settings.{setting}", ""),
        patch("hutch_bunny.core.db.SnowflakeDBClient") as mock_client,
    ):
        with pytest.raises(ValueError, match=message):
            _create_snowflake_client()

    mock_client.assert_not_called()
