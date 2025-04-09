import pytest
from unittest.mock import patch, MagicMock

from hutch_bunny.core.db_manager import SyncDBManager


@pytest.mark.integration
def test_check_tables_exist_integration(db_manager: SyncDBManager) -> None:
    """
    This test verifies that the function correctly identifies when all required tables
    exist in a real database. It uses the db_manager fixture which connects to a real
    database.
    """
    # The db_manager fixture already calls _check_tables_exist during initialization
    # If we get here, it means no exception was raised, which is what we want

    # But we can also call the method to assert it doesn't raise an exception
    db_manager._check_tables_exist()

    # If we get here, the test passes
    assert True


@pytest.mark.integration
def test_check_tables_exist_with_missing_tables(db_manager: SyncDBManager) -> None:
    """
    This test verifies that the function correctly raises a RuntimeError when
    required tables are missing.

    Patch the inspector's get_table_names method to return a subset of the required tables.
    """
    mock_inspector = MagicMock()
    mock_inspector.get_table_names.return_value = [
        "concept",
        "person",
        "measurement",
        # Missing: condition_occurrence, observation, drug_exposure
    ]

    with patch.object(
        db_manager.inspector,
        "get_table_names",
        side_effect=mock_inspector.get_table_names,
    ):
        with pytest.raises(RuntimeError) as exc_info:
            db_manager._check_tables_exist()

        # Assert error message contains the missing tables
        assert "Missing tables in the database" in str(exc_info.value)
        assert "condition_occurrence" in str(exc_info.value)
        assert "observation" in str(exc_info.value)
        assert "drug_exposure" in str(exc_info.value)


@pytest.mark.integration
def test_check_tables_exist_with_schema_integration(db_manager: SyncDBManager) -> None:
    """
    Verifies that the function correctly handles schemas when checking
    for required tables.
    """
    if db_manager.schema:
        db_manager._check_tables_exist()

        # If we get here, the test passes
        assert True
    else:
        # If the db_manager doesn't have a schema, we can skip this test
        pytest.skip("No schema configured for this test")
