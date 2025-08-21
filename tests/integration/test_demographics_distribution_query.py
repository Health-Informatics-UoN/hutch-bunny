import pytest
from hutch_bunny.core.solvers.query_solvers import solve_distribution
from hutch_bunny.core.rquest_models.result import RquestResult
from hutch_bunny.core.rquest_models.file import File
from hutch_bunny.core.rquest_models.distribution import DistributionQuery
from hutch_bunny.core.db import SyncDBClient


@pytest.fixture
def distribution_example() -> RquestResult:
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
def distribution_result(
    db_client: SyncDBClient, distribution_query: DistributionQuery
) -> RquestResult:
    db_client.list_tables()
    return solve_distribution(
        results_modifier=[], db_client=db_client, query=distribution_query
    )


@pytest.mark.integration
def test_solve_distribution_returns_result(distribution_result: RquestResult) -> None:
    assert isinstance(distribution_result, RquestResult)


@pytest.mark.integration
def test_solve_distribution_is_ok(distribution_result: RquestResult) -> None:
    assert distribution_result.status == "ok"


@pytest.mark.integration
def test_solve_distribution_files_count(distribution_result: RquestResult) -> None:
    assert len(distribution_result.files) == 1  # Result file + metadata file


@pytest.mark.integration
def test_solve_distribution_files_type(distribution_result: RquestResult) -> None:
    assert isinstance(distribution_result.files[0], File)


@pytest.mark.integration
def test_solve_distribution_match_query(
    distribution_result: RquestResult, distribution_example: RquestResult
) -> None:
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
