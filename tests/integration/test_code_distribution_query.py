import pytest
from hutch_bunny.core.solvers.query_solvers import solve_distribution
from hutch_bunny.core.rquest_dto.query import DistributionQuery
from hutch_bunny.core.db import SyncDBClient
from hutch_bunny.core.rquest_dto.result import RquestResult
from hutch_bunny.core.rquest_dto.file import File
from hutch_bunny.core.settings import Settings
import hutch_bunny.core.db as db

settings = Settings()


@pytest.fixture
def db_client() -> SyncDBClient:
    datasource_db_port = settings.DATASOURCE_DB_PORT
    return SyncDBClient(
        username=settings.DATASOURCE_DB_USERNAME,
        password=settings.DATASOURCE_DB_PASSWORD,
        host=settings.DATASOURCE_DB_HOST,
        port=(int(datasource_db_port) if datasource_db_port is not None else None),
        database=settings.DATASOURCE_DB_DATABASE,
        drivername=db.expand_short_drivers(settings.DATASOURCE_DB_DRIVERNAME),
        schema=settings.DATASOURCE_DB_SCHEMA,
    )


@pytest.fixture
def distribution_query():
    return DistributionQuery(
        owner="user1",
        code="GENERIC",
        analysis="DISTRIBUTION",
        uuid="unique_id",
        collection="collection_id",
    )


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
                name="code.distribution",
                data="",
                description="Result of code.distribution analysis",
                size=0.308,
                type_="BCOS",
                sensitive=True,
                reference="",
            )
        ],
        message="",
        protocol_version="v2",
    )


@pytest.fixture
def distribution_result(db_client, distribution_query):
    db_client.list_tables()
    return solve_distribution(
        results_modifier=[], db_client=db_client, query=distribution_query
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
