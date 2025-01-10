import pytest
from hutch_bunny.core.query_solvers import (
    AvailabilityQuery,
    solve_availability,
)
from hutch_bunny.core.db_manager import SyncDBManager
from hutch_bunny.core.rquest_dto.result import RquestResult
from hutch_bunny.core.rquest_dto.cohort import Cohort
from hutch_bunny.core.rquest_dto.group import Group
from hutch_bunny.core.rquest_dto.rule import Rule
from dotenv import load_dotenv
import os
import hutch_bunny.core.settings as settings

load_dotenv()

pytestmark = pytest.mark.skipif(
    os.environ.get("CI") is not None, reason="Skip integration tests in CI"
)


@pytest.fixture
def db_manager():
    datasource_db_port = os.getenv("DATASOURCE_DB_PORT")
    return SyncDBManager(
        username=os.getenv("DATASOURCE_DB_USERNAME"),
        password=os.getenv("DATASOURCE_DB_PASSWORD"),
        host=os.getenv("DATASOURCE_DB_HOST"),
        port=(int(datasource_db_port) if datasource_db_port is not None else None),
        database=os.getenv("DATASOURCE_DB_DATABASE"),
        drivername=os.getenv("DATASOURCE_DB_DRIVERNAME", settings.DEFAULT_DB_DRIVER),
        schema=os.getenv("DATASOURCE_DB_SCHEMA"),
    )


@pytest.fixture
def availability_query():
    return AvailabilityQuery(
        cohort=Cohort(
            [
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
def availability_example():
    return RquestResult(
        uuid="unique_id",
        status="ok",
        collection_id="collection_id",
        count=6272,
        datasets_count=0,
        files=[],
        message="",
        protocol_version="v2",
    )


@pytest.fixture
def availability_result(db_manager, availability_query):
    return solve_availability(db_manager=db_manager, query=availability_query)


def test_solve_availability_returns_result(availability_result):
    assert isinstance(availability_result, RquestResult)


def test_solve_availability_fields_match_query(
    availability_result, availability_example
):
    assert availability_result.uuid == availability_example.uuid
    assert availability_result.collection_id == availability_example.collection_id
    assert availability_result.protocol_version == availability_example.protocol_version


def test_solve_availability_is_ok(availability_result):
    assert availability_result.status == "ok"


def test_solve_availability_count_matches(availability_result, availability_example):
    assert availability_result.count == availability_example.count
