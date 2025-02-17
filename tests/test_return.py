import pytest
from hutch_bunny.core.solvers.query_solvers import (
    solve_availability,
)
from hutch_bunny.core.rquest_dto.result import RquestResult


@pytest.fixture
def availability_example():
    return RquestResult(
        uuid="unique_id",
        status="ok",
        collection_id="collection_id",
        count=40,
        datasets_count=0,
        files=[],
        message="",
        protocol_version="v2",
    )


@pytest.fixture
def availability_result(db_manager, availability_query_onerule_equals):
    return solve_availability(
        results_modifier=[],
        db_manager=db_manager,
        query=availability_query_onerule_equals,
    )


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
