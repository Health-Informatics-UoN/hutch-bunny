import pytest
from unittest.mock import patch, MagicMock
from hutch_bunny.core.db_manager import SyncDBManager


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

    # Create a SyncDBManager instance with mocked dependencies
    with patch("hutch_bunny.core.db_manager.inspect", return_value=mock_inspector):
        db_manager = SyncDBManager(
            username="test_user",
            password="test_password",
            host="test_host",
            port=5432,
            database="test_db",
            drivername="postgresql+psycopg",
        )
        db_manager.engine = mock_engine
        db_manager.inspector = mock_inspector

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

    # Create a SyncDBManager instance with mocked dependencies
    with patch("hutch_bunny.core.db_manager.inspect", return_value=mock_inspector):
        with pytest.raises(RuntimeError) as exc_info:
            SyncDBManager(
                username="test_user",
                password="test_password",
                host="test_host",
                port=5432,
                database="test_db",
                drivername="postgresql+psycopg",
            )

        # Assert error message contains the missing tables
        assert "Missing tables in the database" in str(exc_info.value)
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

    # Create a SyncDBManager instance with mocked dependencies and a schema
    with patch("hutch_bunny.core.db_manager.inspect", return_value=mock_inspector):
        db_manager = SyncDBManager(
            username="test_user",
            password="test_password",
            host="test_host",
            port=5432,
            database="test_db",
            drivername="postgresql+psycopg",
            schema="test_schema",
        )
        db_manager.engine = mock_engine
        db_manager.inspector = mock_inspector

        # Assert the inspector was called with the correct schema
        mock_inspector.get_table_names.assert_called_once_with(schema="test_schema")


@pytest.mark.unit
def test_check_indexes_exist_missing_indexes(
    mock_inspector: MagicMock, mock_engine: MagicMock
) -> None:
    """Test that a warning is logged when required indexes are missing."""
    # Setup - return empty list for all tables to simulate missing indexes
    mock_inspector.get_indexes.return_value = []

    # Create a SyncDBManager instance with mocked dependencies
    with patch("hutch_bunny.core.db_manager.inspect", return_value=mock_inspector):
        with patch("hutch_bunny.core.db_manager.logger") as mock_logger:
            # Mock _check_tables_exist to prevent it from running
            with patch.object(SyncDBManager, "_check_tables_exist"):
                db_manager = SyncDBManager(
                    username="test_user",
                    password="test_password",
                    host="test_host",
                    port=5432,
                    database="test_db",
                    drivername="postgresql+psycopg",
                )
                db_manager.engine = mock_engine
                db_manager.inspector = mock_inspector

                # Assert a warning was logged
                mock_logger.warning.assert_called_once()

                # Assert the message contains information about missing indexes
                warning_msg = mock_logger.warning.call_args[0][0]
                assert "Missing indexes in the database" in warning_msg
