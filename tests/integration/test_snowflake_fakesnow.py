"""
Runs the Snowflake client against fakesnow, which emulates Snowflake on top of DuckDB,
so the Snowflake SQL Bunny builds is tested without a Snowflake account. The OMOP data
is copied from the DuckDB test database, so these tests only run when the tests are
pointed at DuckDB. Each sample query must give the same answer as the DuckDB client.
"""

import json
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import patch

import duckdb
import fakesnow
import pytest
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect

from hutch_bunny.core.db import BaseDBClient, SnowflakeDBClient, get_db_client
from hutch_bunny.core.execute_query import execute_query
from hutch_bunny.core.results_modifiers import results_modifiers
from hutch_bunny.core.settings import Settings

settings = Settings()

QUERIES_DIR = Path(__file__).parents[1] / "queries"
SNOWFLAKE_DATABASE = "OMOP"
SNOWFLAKE_SCHEMA = "CDM"

# Raw counts, so a difference between the dialects can't be hidden by rounding
RAW_COUNTS = results_modifiers(low_number_suppression_threshold=0, rounding_target=0)

QUERY_FILES = sorted((QUERIES_DIR / "availability").glob("*.json")) + [
    QUERIES_DIR / "distribution" / "demographics.json",
    QUERIES_DIR / "distribution" / "distribution.json",
]


@pytest.fixture(scope="module")
def snowflake_db_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build a fakesnow database file from the DuckDB test database."""
    if settings.DATASOURCE_DB_DRIVERNAME != "duckdb":
        pytest.skip("fakesnow tests copy their data from the DuckDB test database")

    db_path = tmp_path_factory.mktemp("fakesnow")
    con = duckdb.connect(str(db_path / f"{SNOWFLAKE_DATABASE}.db"))
    con.execute(f"ATTACH '{settings.DATASOURCE_DUCKDB_PATH_TO_DB}' AS src (READ_ONLY)")
    con.execute(f'CREATE SCHEMA "{SNOWFLAKE_SCHEMA}"')

    # Snowflake stores unquoted identifiers in upper case, and fakesnow relies on it
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog = 'src' AND table_schema = ?",
        [settings.DATASOURCE_DB_SCHEMA],
    ).fetchall()
    for (table,) in tables:
        columns = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog = 'src' AND table_schema = ? AND table_name = ? "
            "ORDER BY ordinal_position",
            [settings.DATASOURCE_DB_SCHEMA, table],
        ).fetchall()
        select_list = ", ".join(f'"{c}" AS "{c.upper()}"' for (c,) in columns)
        con.execute(
            f'CREATE TABLE "{SNOWFLAKE_SCHEMA}"."{table.upper()}" AS '
            f'SELECT {select_list} FROM src."{settings.DATASOURCE_DB_SCHEMA}"."{table}"'
        )

    # fakesnow doesn't implement Snowflake's HAVERSINE, which returns kilometres
    con.execute(
        f'CREATE MACRO "{SNOWFLAKE_SCHEMA}".haversine(lat1, lon1, lat2, lon2) AS '
        "2 * 6371 * asin(sqrt("
        "pow(sin(radians(lat2 - lat1) / 2), 2) + "
        "cos(radians(lat1)) * cos(radians(lat2)) * pow(sin(radians(lon2 - lon1) / 2), 2)"
        "))"
    )
    con.close()
    return db_path


@pytest.fixture(scope="module")
def snowflake_client(snowflake_db_path: Path) -> Iterator[SnowflakeDBClient]:
    # The dialect parses SELECT CURRENT_VERSION(), which fakesnow answers with DuckDB's version
    with (
        fakesnow.patch(db_path=snowflake_db_path),
        patch.object(SnowflakeDialect, "_get_server_version_info", return_value=(9, 0, 0)),
    ):
        yield SnowflakeDBClient(
            username="fake_user",
            account="fake_account",
            warehouse="fake_warehouse",
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            password="fake_password",
            role="fake_role",
        )


@pytest.fixture(scope="module")
def duckdb_client(snowflake_db_path: Path) -> BaseDBClient:
    return get_db_client()


@pytest.fixture
def omop_tables_enabled() -> Iterator[None]:
    """Enable the optional OMOP tables so every sample query is exercised."""
    with (
        patch("hutch_bunny.core.solvers.availability_solver.settings.OMOP_SPECIMEN_ENABLED", True),
        patch("hutch_bunny.core.solvers.availability_solver.settings.OMOP_LOCATION_ENABLED", True),
        patch("hutch_bunny.core.solvers.availability_solver.settings.OMOP_DEATH_ENABLED", True),
        patch("hutch_bunny.core.solvers.distribution_solver.settings.OMOP_SPECIMEN_ENABLED", True),
        patch("hutch_bunny.core.solvers.distribution_solver.settings.OMOP_DEATH_ENABLED", True),
    ):
        yield


@pytest.mark.integration
def test_snowflake_client_connects_to_fakesnow(snowflake_client: SnowflakeDBClient) -> None:
    """Test the Snowflake client connects, builds Snowflake SQL and finds the OMOP tables."""
    assert snowflake_client.engine.dialect.name == "snowflake"
    assert "person" in snowflake_client.list_tables()


@pytest.mark.integration
@pytest.mark.parametrize("query_file", QUERY_FILES, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_snowflake_query_matches_duckdb(
    snowflake_client: SnowflakeDBClient,
    duckdb_client: BaseDBClient,
    omop_tables_enabled: None,
    query_file: Path,
) -> None:
    """Test a sample query gives the same answer on Snowflake as on DuckDB."""
    # Arrange
    with open(query_file) as f:
        query: dict[str, Any] = json.load(f)

    # Act
    snowflake_result = execute_query(
        query, RAW_COUNTS, db_client=snowflake_client, encode_result=False
    ).to_dict()
    duckdb_result = execute_query(
        query, RAW_COUNTS, db_client=duckdb_client, encode_result=False
    ).to_dict()

    # Assert
    assert snowflake_result["status"] == "ok"
    assert snowflake_result["queryResult"] == duckdb_result["queryResult"]
