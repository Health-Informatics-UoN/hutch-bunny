"""
Runs each sample query through the Snowflake client on fakesnow (see `fakesnow_client`
in conftest.py) and checks it gives the same answer as the DuckDB client on the same
data. Only runs when the tests are pointed at DuckDB, as that file is the source of the data.
"""

import json
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import patch

import pytest

from hutch_bunny.core.db import BaseDBClient, SnowflakeDBClient, get_db_client
from hutch_bunny.core.execute_query import execute_query
from hutch_bunny.core.results_modifiers import ResultModifier, results_modifiers

QUERIES_DIR = Path(__file__).parents[1] / "queries"

QUERY_FILES = sorted((QUERIES_DIR / "availability").glob("*.json")) + [
    QUERIES_DIR / "distribution" / "demographics.json",
    QUERIES_DIR / "distribution" / "distribution.json",
]

# Rounding and low number suppression are applied in SQL, so are dialect-specific too
MODIFIERS: dict[str, list[ResultModifier]] = {
    "raw": results_modifiers(low_number_suppression_threshold=0, rounding_target=0),
    "default": results_modifiers(low_number_suppression_threshold=10, rounding_target=10),
    "suppress_only": results_modifiers(low_number_suppression_threshold=100, rounding_target=0),
    "round_only": results_modifiers(low_number_suppression_threshold=0, rounding_target=100),
}


@pytest.fixture(scope="module")
def duckdb_client(fakesnow_db_path: Path) -> BaseDBClient:
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
def test_snowflake_client_connects_to_fakesnow(fakesnow_client: SnowflakeDBClient) -> None:
    """Test the Snowflake client connects, builds Snowflake SQL and finds the OMOP tables."""
    assert fakesnow_client.engine.dialect.name == "snowflake"
    assert "person" in fakesnow_client.list_tables()


@pytest.mark.integration
@pytest.mark.parametrize("modifiers_name", MODIFIERS)
@pytest.mark.parametrize("query_file", QUERY_FILES, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_snowflake_query_matches_duckdb(
    fakesnow_client: SnowflakeDBClient,
    duckdb_client: BaseDBClient,
    omop_tables_enabled: None,
    query_file: Path,
    modifiers_name: str,
) -> None:
    """Test a sample query gives the same answer on Snowflake as on DuckDB."""
    # Arrange
    with open(query_file) as f:
        query: dict[str, Any] = json.load(f)
    modifiers = MODIFIERS[modifiers_name]

    # Act
    snowflake_result = execute_query(
        query, modifiers, db_client=fakesnow_client, encode_result=False
    ).to_dict()
    duckdb_result = execute_query(
        query, modifiers, db_client=duckdb_client, encode_result=False
    ).to_dict()

    # Assert
    assert snowflake_result["status"] == "ok"
    assert snowflake_result["queryResult"] == duckdb_result["queryResult"]
