import pytest
from hutch_bunny.core.db import SyncDBClient
from hutch_bunny.core.rquest_models.availability import AvailabilityQuery
from hutch_bunny.core.solvers.query_solvers import (
    solve_availability,
)
from hutch_bunny.core.rquest_models.result import RquestResult


@pytest.fixture
def availability_example() -> RquestResult:
    return RquestResult(
        uuid="unique_id",
        status="ok",
        collection_id="collection_id",
        count=570,
        datasets_count=0,
        files=[],
        message="",
        protocol_version="v2",
    )


@pytest.fixture
def availability_result(
    db_client: SyncDBClient,
    availability_query_onerule_equals: AvailabilityQuery,
) -> RquestResult:
    return solve_availability(
        results_modifier=[],
        db_client=db_client,
        query=availability_query_onerule_equals,
    )


@pytest.mark.integration
def test_solve_availability_returns_result(availability_result: RquestResult) -> None:
    assert isinstance(availability_result, RquestResult)


@pytest.mark.integration
def test_solve_availability_fields_match_query(
    availability_result: RquestResult, availability_example: RquestResult
) -> None:
    assert availability_result.uuid == availability_example.uuid
    assert availability_result.collection_id == availability_example.collection_id
    assert availability_result.protocol_version == availability_example.protocol_version


@pytest.mark.integration
def test_solve_availability_is_ok(availability_result: RquestResult) -> None:
    assert availability_result.status == "ok"


@pytest.mark.integration
def test_solve_availability_count_matches(availability_result: RquestResult, availability_example: RquestResult) -> None:
    assert availability_result.count == availability_example.count
