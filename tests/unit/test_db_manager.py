import pytest
from unittest.mock import patch, MagicMock
from hutch_bunny.core.db import DuckDBClient, SyncDBClient


@pytest.fixture
def mock_inspector() -> MagicMock:
    """Create a mock inspector for testing."""
    inspector = MagicMock()
    return inspector


@pytest.fixture
def mock_engine() -> MagicMock:
    """Create a mock engine for testing."""
    engine = MagicMock()
    return engine


@pytest.mark.unit
def test_check_tables_exist_all_tables_present(
    mock_inspector: MagicMock, mock_engine: MagicMock
) -> None:
    """Test that no error is raised when all required tables exist."""
    # Setup
    mock_inspector.get_table_names.return_value = [
        "concept",
        "person",
        "measurement",
        "condition_occurrence",
        "observation",
        "drug_exposure",
    ]

    # Create a SyncDBClient instance with mocked dependencies
    with patch("hutch_bunny.core.db.sync.inspect", return_value=mock_inspector):
        db_client = SyncDBClient(
            username="test_user",
            password="test_password",
            host="test_host",
            port=5432,
            database="test_db",
            drivername="postgresql+psycopg",
        )
        db_client._engine = mock_engine
        db_client._inspector = mock_inspector

        # Assert the inspector was called with the correct schema
        mock_inspector.get_table_names.assert_called_once_with(schema=None)


@pytest.mark.unit
def test_check_tables_exist_missing_tables(
    mock_inspector: MagicMock, mock_engine: MagicMock
) -> None:
    """Test that RuntimeError is raised when required tables are missing."""
    # Setup
    mock_inspector.get_table_names.return_value = [
        "concept",
        "person",
        "measurement",
        # Missing: condition_occurrence, observation, drug_exposure
    ]

    # Create a SyncDBClient instance with mocked dependencies
    with patch("hutch_bunny.core.db.sync.inspect", return_value=mock_inspector):
        with pytest.raises(RuntimeError) as exc_info:
            SyncDBClient(
                username="test_user",
                password="test_password",
                host="test_host",
                port=5432,
                database="test_db",
                drivername="postgresql+psycopg",
            )

        # Assert error message contains the missing tables
        assert "Missing tables or views in the database" in str(exc_info.value)
        assert "condition_occurrence" in str(exc_info.value)
        assert "observation" in str(exc_info.value)
        assert "drug_exposure" in str(exc_info.value)


@pytest.mark.unit
def test_check_tables_exist_with_schema(
    mock_inspector: MagicMock, mock_engine: MagicMock
) -> None:
    """Test that the schema is correctly passed to get_table_names."""
    # Setup
    mock_inspector.get_table_names.return_value = [
        "concept",
        "person",
        "measurement",
        "condition_occurrence",
        "observation",
        "drug_exposure",
    ]

    # Create a SyncDBClient instance with mocked dependencies and a schema
    with patch("hutch_bunny.core.db.sync.inspect", return_value=mock_inspector):
        db_client = SyncDBClient(
            username="test_user",
            password="test_password",
            host="test_host",
            port=5432,
            database="test_db",
            drivername="postgresql+psycopg",
            schema="test_schema",
        )
        db_client._engine = mock_engine
        db_client._inspector = mock_inspector

        # Assert the inspector was called with the correct schema
        mock_inspector.get_table_names.assert_called_once_with(schema="test_schema")


@pytest.mark.unit
def test_check_tables_exist_with_views(
    mock_inspector: MagicMock, mock_engine: MagicMock
) -> None:
    """Test that views are accepted as valid alternatives to tables."""
    # Arrange
    mock_inspector.get_table_names.return_value = [
        "concept",
        "person",
        "measurement",
    ]
    mock_inspector.get_view_names.return_value = [
        "condition_occurrence",
        "observation",
        "drug_exposure",
    ]

    # Act
    with patch("hutch_bunny.core.db.sync.inspect", return_value=mock_inspector):
        db_client = SyncDBClient(
            username="test_user",
            password="test_password",
            host="test_host",
            port=5432,
            database="test_db",
            drivername="postgresql+psycopg",
        )
        db_client._engine = mock_engine
        db_client._inspector = mock_inspector

        # Assert
        mock_inspector.get_table_names.assert_called_once_with(schema=None)
        mock_inspector.get_view_names.assert_called_once_with(schema=None)


@pytest.mark.unit
def test_check_tables_exist_cohort_tables_required_when_log_results_enabled(
    mock_inspector: MagicMock,
) -> None:
    """Test that cohort tables are required when LOG_RESULTS_ENABLED is set."""
    # Arrange
    mock_inspector.get_table_names.return_value = [
        "concept",
        "person",
        "measurement",
        "condition_occurrence",
        "observation",
        "drug_exposure",
    ]
    mock_inspector.get_view_names.return_value = []

    # Act
    with (
        patch("hutch_bunny.core.db.sync.inspect", return_value=mock_inspector),
        patch("hutch_bunny.core.db.sync.settings.LOG_RESULTS_ENABLED", True),
        pytest.raises(RuntimeError) as exc_info,
    ):
        SyncDBClient(
            username="test_user",
            password="test_password",
            host="test_host",
            port=5432,
            database="test_db",
            drivername="postgresql+psycopg",
        )

    # Assert
    assert "cohort" in str(exc_info.value)
    assert "cohort_definition" in str(exc_info.value)


@pytest.mark.unit
def test_check_tables_exist_cohort_tables_present_when_log_results_enabled(
    mock_inspector: MagicMock,
) -> None:
    """Test that no error is raised when LOG_RESULTS_ENABLED is set and cohort tables exist."""
    # Arrange
    mock_inspector.get_table_names.return_value = [
        "concept",
        "person",
        "measurement",
        "condition_occurrence",
        "observation",
        "drug_exposure",
        "cohort",
        "cohort_definition",
    ]
    mock_inspector.get_view_names.return_value = []

    # Act / Assert - constructing the client must not raise
    with (
        patch("hutch_bunny.core.db.sync.inspect", return_value=mock_inspector),
        patch("hutch_bunny.core.db.sync.settings.LOG_RESULTS_ENABLED", True),
    ):
        SyncDBClient(
            username="test_user",
            password="test_password",
            host="test_host",
            port=5432,
            database="test_db",
            drivername="postgresql+psycopg",
        )


@pytest.mark.unit
def test_check_indexes_exist_missing_indexes(
    mock_inspector: MagicMock, mock_engine: MagicMock
) -> None:
    """Test that a warning is logged when required indexes are missing."""
    # Setup - return empty list for all tables to simulate missing indexes
    mock_inspector.get_indexes.return_value = []

    # Create a SyncDBClient instance with mocked dependencies
    with patch("hutch_bunny.core.db.sync.inspect", return_value=mock_inspector):
        with patch("hutch_bunny.core.db.sync.logger") as mock_logger:
            # Mock _check_tables_exist to prevent it from running
            with patch.object(SyncDBClient, "_check_tables_exist"):
                db_client = SyncDBClient(
                    username="test_user",
                    password="test_password",
                    host="test_host",
                    port=5432,
                    database="test_db",
                    drivername="postgresql+psycopg",
                )
                db_client._engine = mock_engine
                db_client._inspector = mock_inspector

                # Assert a warning was logged
                mock_logger.warning.assert_called_once()

                # Assert the message contains information about missing indexes
                warning_msg = mock_logger.warning.call_args[0][0]
                assert "Missing indexes in the database" in warning_msg


@pytest.mark.unit
@pytest.mark.parametrize("read_only", [True, False])
def test_duckdb_client_read_only_controls_cohort_logging(
    mock_inspector: MagicMock, read_only: bool
) -> None:
    """Test DuckDB opens writable, and supports cohort logging, only when not read-only."""
    # Arrange
    mock_inspector.get_table_names.return_value = [
        "concept",
        "person",
        "measurement",
        "condition_occurrence",
        "observation",
        "drug_exposure",
    ]
    mock_inspector.get_view_names.return_value = []

    # Act
    with (
        patch("hutch_bunny.core.db.duckdb.create_engine") as mock_create_engine,
        patch("hutch_bunny.core.db.duckdb.inspect", return_value=mock_inspector),
    ):
        db_client = DuckDBClient(
            path_to_db="/data/omop.duckdb",
            duckdb_memory_limit="1000mb",
            duckdb_temp_directory="/tmp",
            read_only=read_only,
        )

    # Assert
    connect_args = mock_create_engine.call_args.kwargs["connect_args"]
    assert connect_args["read_only"] is read_only
    assert db_client.supports_cohort_logging is not read_only


@pytest.mark.unit
def test_duckdb_client_read_only_by_default(mock_inspector: MagicMock) -> None:
    """Test DuckDB is opened read-only unless asked otherwise."""
    # Arrange
    mock_inspector.get_table_names.return_value = [
        "concept",
        "person",
        "measurement",
        "condition_occurrence",
        "observation",
        "drug_exposure",
    ]
    mock_inspector.get_view_names.return_value = []

    # Act
    with (
        patch("hutch_bunny.core.db.duckdb.create_engine") as mock_create_engine,
        patch("hutch_bunny.core.db.duckdb.inspect", return_value=mock_inspector),
    ):
        db_client = DuckDBClient(
            path_to_db="/data/omop.duckdb",
            duckdb_memory_limit="1000mb",
            duckdb_temp_directory="/tmp",
        )

    # Assert
    assert mock_create_engine.call_args.kwargs["connect_args"]["read_only"] is True
    assert db_client.supports_cohort_logging is False
