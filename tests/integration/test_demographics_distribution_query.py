import pytest
from hutch_bunny.core.solvers.query_solvers import solve_distribution
from hutch_bunny.core.rquest_dto.result import RquestResult
from hutch_bunny.core.rquest_dto.file import File


@pytest.fixture
def distribution_example():
    return RquestResult(
        uuid="unique_id",
        status="ok",
        collection_id="collection_id",
        count=1,
        datasets_count=1,
        files=[
            File(
                name="demographics.distribution",
                data="",
                description="Result of code.distribution analysis",
                size=0.268,
                type_="BCOS",
                sensitive=True,
                reference="",
            )
        ],
        message="",
        protocol_version="v2",
    )


@pytest.fixture
def distribution_result(db_manager, distribution_query):
    db_manager.list_tables()
    return solve_distribution(
        results_modifier=[], db_manager=db_manager, query=distribution_query
    )


@pytest.mark.integration
def test_solve_distribution_returns_result(distribution_result):
    assert isinstance(distribution_result, RquestResult)


@pytest.mark.integration
def test_solve_distribution_is_ok(distribution_result):
    assert distribution_result.status == "ok"


@pytest.mark.integration
def test_solve_distribution_files_count(distribution_result):
    assert len(distribution_result.files) == 1


@pytest.mark.integration
def test_solve_distribution_files_type(distribution_result):
    assert isinstance(distribution_result.files[0], File)


@pytest.mark.integration
def test_solve_distribution_match_query(distribution_result, distribution_example):
    assert distribution_result.files[0].name == distribution_example.files[0].name
    assert distribution_result.files[0].type_ == distribution_example.files[0].type_
    assert (
        distribution_result.files[0].description
        == distribution_example.files[0].description
    )
    assert (
        distribution_result.files[0].sensitive
        == distribution_example.files[0].sensitive
    )
    assert (
        distribution_result.files[0].reference
        == distribution_example.files[0].reference
    )
