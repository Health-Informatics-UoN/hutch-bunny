import pytest
from unittest.mock import MagicMock
from hutch_bunny.core.rquest_models.distribution import DistributionQuery, DistributionQueryType
from hutch_bunny.core.solvers.distribution_solver import TableCountsDistributionQuerySolver


@pytest.fixture
def query() -> DistributionQuery:
    return DistributionQuery(
        owner="user1",
        code=DistributionQueryType.TABLE_COUNTS,
        analysis="DISTRIBUTION",
        uuid="test-uuid",
        collection="test-collection",
    )


def _make_db_client(table_names: list[str], row_count: int = 5) -> MagicMock:
    """Build a mock db_client that advertises the given tables and returns row_count for COUNT(*)."""
    mock = MagicMock()
    mock.list_tables.return_value = table_names

    scalar_result = MagicMock()
    scalar_result.scalar.return_value = row_count

    mock.engine.connect.return_value.__enter__.return_value.execute.return_value = scalar_result
    return mock


def test_only_existing_tables_appear_in_output(query: DistributionQuery) -> None:
    """Tables not in list_tables() are omitted from the result."""
    db_client = _make_db_client(["person", "concept"])
    solver = TableCountsDistributionQuerySolver(db_client, query)

    tsv, row_count = solver.solve_query([])

    assert row_count == 2
    assert "person" in tsv
    assert "concept" in tsv
    assert "measurement" not in tsv
    assert "condition_occurrence" not in tsv


def test_absent_table_not_in_output(query: DistributionQuery) -> None:
    """A table absent from the DB produces no row, not a zero row."""
    db_client = _make_db_client(["person"])
    solver = TableCountsDistributionQuerySolver(db_client, query)

    tsv, _ = solver.solve_query([])

    assert "death" not in tsv
    assert "specimen" not in tsv


def test_zero_count_for_empty_table(query: DistributionQuery) -> None:
    """An existing table with 0 rows appears in the output with COUNT=0."""
    db_client = _make_db_client(["person", "measurement"], row_count=0)
    solver = TableCountsDistributionQuerySolver(db_client, query)

    tsv, row_count = solver.solve_query([])

    assert row_count == 2
    lines = tsv.strip().splitlines()
    data_lines = [line for line in lines if "person" in line or "measurement" in line]
    for line in data_lines:
        assert line.endswith("\t0")


def test_tsv_has_correct_headers(query: DistributionQuery) -> None:
    db_client = _make_db_client(["person"])
    solver = TableCountsDistributionQuerySolver(db_client, query)

    tsv, _ = solver.solve_query([])

    header = tsv.splitlines()[0]
    assert header == "BIOBANK\tTABLE\tCOUNT"


def test_biobank_is_collection_id(query: DistributionQuery) -> None:
    db_client = _make_db_client(["person"], row_count=10)
    solver = TableCountsDistributionQuerySolver(db_client, query)

    tsv, _ = solver.solve_query([])

    assert "test-collection" in tsv


def test_case_insensitive_table_matching(query: DistributionQuery) -> None:
    """Table names from list_tables() may be mixed-case (e.g. Snowflake uppercase)."""
    db_client = _make_db_client(["PERSON", "MEASUREMENT"], row_count=3)
    solver = TableCountsDistributionQuerySolver(db_client, query)

    tsv, row_count = solver.solve_query([])

    assert row_count == 2
    assert "person" in tsv
    assert "measurement" in tsv


def test_empty_database_returns_empty_result(query: DistributionQuery) -> None:
    db_client = _make_db_client([])
    solver = TableCountsDistributionQuerySolver(db_client, query)

    tsv, row_count = solver.solve_query([])

    assert row_count == 0
    lines = [line for line in tsv.splitlines() if line.strip()]
    assert lines == ["BIOBANK\tTABLE\tCOUNT"]
