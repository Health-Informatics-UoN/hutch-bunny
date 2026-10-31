import os
from pathlib import Path
from typing import Iterator
from unittest.mock import patch

import duckdb
import fakesnow
import pytest
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
from hutch_bunny.core.rquest_models.cohort import Cohort
from hutch_bunny.core.rquest_models.group import Group
from hutch_bunny.core.rquest_models.rule import Rule
from hutch_bunny.core.db import BaseDBClient, SnowflakeDBClient
from hutch_bunny.core.rquest_models.distribution import (
    DistributionQuery,
    DistributionQueryType,
)
from hutch_bunny.core.rquest_models.availability import AvailabilityQuery
from hutch_bunny.core.settings import Settings
from hutch_bunny.core.db import get_db_client


settings = Settings()

# Set by the Snowflake CI job: db_client becomes a Snowflake client running on fakesnow
USE_FAKESNOW = os.environ.get("TEST_USE_FAKESNOW", "").lower() == "true"

FAKESNOW_DATABASE = "OMOP"
FAKESNOW_SCHEMA = "CDM"

# fakesnow wraps a query in DESCRIBE to get its result columns, and can't parse DESCRIBE
# of a query that starts with a bracketed set operation. Real Snowflake is unaffected,
# and Bunny's full queries wrap these in a count so aren't affected either.
FAKESNOW_DESCRIBE_SET_OP = "fakesnow can't DESCRIBE a query starting with a bracketed INTERSECT/UNION"
FAKESNOW_XFAIL = {
    "tests/integration/test_availability_solver_integration.py::TestBuildGroupQuery::test_multiple_omop_rules_and": FAKESNOW_DESCRIBE_SET_OP,
    "tests/integration/test_availability_solver_integration.py::TestBuildGroupQuery::test_complex_mixed_rules": FAKESNOW_DESCRIBE_SET_OP,
    "tests/integration/test_availability_solver_integration.py::TestBuildGroupQuery::test_group_with_time_constraints": FAKESNOW_DESCRIBE_SET_OP,
}


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    if not USE_FAKESNOW:
        return
    for item in items:
        if reason := FAKESNOW_XFAIL.get(item.nodeid):
            # strict, so the test is flagged once a fakesnow upgrade fixes it
            item.add_marker(pytest.mark.xfail(reason=reason, strict=True))


@pytest.fixture
def db_client(request: pytest.FixtureRequest) -> BaseDBClient:
    if USE_FAKESNOW:
        client: BaseDBClient = request.getfixturevalue("fakesnow_client")
        return client
    return get_db_client()


@pytest.fixture(scope="session")
def fakesnow_db_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """
    Build a fakesnow database file from the DuckDB test database. fakesnow emulates
    Snowflake on top of DuckDB, so the Snowflake client can be tested without an account.
    """
    if settings.DATASOURCE_DB_DRIVERNAME != "duckdb":
        pytest.skip("fakesnow tests copy their data from the DuckDB test database")

    db_path = tmp_path_factory.mktemp("fakesnow")
    con = duckdb.connect(str(db_path / f"{FAKESNOW_DATABASE}.db"))
    con.execute(f"ATTACH '{settings.DATASOURCE_DUCKDB_PATH_TO_DB}' AS src (READ_ONLY)")
    con.execute(f'CREATE SCHEMA "{FAKESNOW_SCHEMA}"')

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
            f'CREATE TABLE "{FAKESNOW_SCHEMA}"."{table.upper()}" AS '
            f'SELECT {select_list} FROM src."{settings.DATASOURCE_DB_SCHEMA}"."{table}"'
        )

    # fakesnow doesn't implement Snowflake's HAVERSINE, which returns kilometres
    con.execute(
        f'CREATE MACRO "{FAKESNOW_SCHEMA}".haversine(lat1, lon1, lat2, lon2) AS '
        "2 * 6371 * asin(sqrt("
        "pow(sin(radians(lat2 - lat1) / 2), 2) + "
        "cos(radians(lat1)) * cos(radians(lat2)) * pow(sin(radians(lon2 - lon1) / 2), 2)"
        "))"
    )
    con.close()
    return db_path


@pytest.fixture(scope="session")
def fakesnow_client(fakesnow_db_path: Path) -> Iterator[SnowflakeDBClient]:
    # The dialect parses SELECT CURRENT_VERSION(), which fakesnow answers with DuckDB's version
    with (
        fakesnow.patch(db_path=fakesnow_db_path),
        patch.object(SnowflakeDialect, "_get_server_version_info", return_value=(9, 0, 0)),
    ):
        yield SnowflakeDBClient(
            username="fake_user",
            account="fake_account",
            warehouse="fake_warehouse",
            database=FAKESNOW_DATABASE,
            schema=FAKESNOW_SCHEMA,
            password="fake_password",
            role="fake_role",
        )


@pytest.fixture
def availability_query_onerule_equals() -> AvailabilityQuery:
    return AvailabilityQuery(
        cohort=Cohort(
            groups=[
                Group(
                    rules=[
                        Rule(
                            varname="OMOP",
                            varcat="Person",
                            type_="TEXT",
                            operator="=",
                            value="8507",
                        )
                    ],
                    rules_operator="AND",
                ),
            ],
            groups_operator="OR",
        ),
        uuid="unique_id",
        protocol_version="v2",
        char_salt="salt",
        collection="collection_id",
        owner="user1",
    )


@pytest.fixture
def availability_query_onerule_notequals() -> AvailabilityQuery:
    return AvailabilityQuery(
        cohort=Cohort(
            groups=[
                Group(
                    rules=[
                        Rule(
                            varname="OMOP",
                            varcat="Person",
                            type_="TEXT",
                            operator="!=",
                            value="8507",
                        )
                    ],
                    rules_operator="AND",
                ),
            ],
            groups_operator="OR",
        ),
        uuid="unique_id",
        protocol_version="v2",
        char_salt="salt",
        collection="collection_id",
        owner="user1",
    )


@pytest.fixture
def availability_query_tworules_equals() -> AvailabilityQuery:
    return AvailabilityQuery(
        cohort=Cohort(
            groups=[
                Group(
                    rules=[
                        Rule(
                            varname="OMOP",
                            varcat="Person",
                            type_="TEXT",
                            operator="=",
                            value="8507",
                        ),
                        Rule(
                            varname="OMOP",
                            varcat="Condition",
                            type_="TEXT",
                            operator="=",
                            value="28060",
                        ),
                    ],
                    rules_operator="AND",
                ),
            ],
            groups_operator="OR",
        ),
        uuid="unique_id",
        protocol_version="v2",
        char_salt="salt",
        collection="collection_id",
        owner="user1",
    )


@pytest.fixture
def availability_query_tworules_notequals() -> AvailabilityQuery:
    return AvailabilityQuery(
        cohort=Cohort(
            groups=[
                Group(
                    rules=[
                        Rule(
                            varname="OMOP",
                            varcat="Person",
                            type_="TEXT",
                            operator="!=",
                            value="8507",
                        ),
                        Rule(
                            varname="OMOP",
                            varcat="Condition",
                            type_="TEXT",
                            operator="!=",
                            value="28060",
                        ),
                    ],
                    rules_operator="AND",
                ),
            ],
            groups_operator="OR",
        ),
        uuid="unique_id",
        protocol_version="v2",
        char_salt="salt",
        collection="collection_id",
        owner="user1",
    )


@pytest.fixture
def distribution_query() -> DistributionQuery:
    return DistributionQuery(
        owner="user1",
        code=DistributionQueryType.DEMOGRAPHICS,
        analysis="DISTRIBUTION",
        uuid="unique_id",
        collection="collection_id",
    )
