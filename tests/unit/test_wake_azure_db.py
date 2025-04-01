import pytest
from unittest.mock import patch, MagicMock
from sqlalchemy.exc import OperationalError
from hutch_bunny.core.db_manager import WakeAzureDB


def test_decorator_passes_through_for_non_mssql():
    """Test that the decorator passes through when not using MSSQL."""
    # Use patch with the actual path to settings, not the fixture
    with patch("hutch_bunny.core.db_manager.settings") as mock_settings:
        mock_settings.DATASOURCE_DB_DRIVERNAME = "postgresql"

        # Create a simple function to decorate
        @WakeAzureDB()
        def test_func():
            return "success"

        # The function should be returned as-is without wrapping
        assert test_func() == "success"


def test_successful_execution_no_retry():
    """Test successful execution without any need for retry."""
    with patch("hutch_bunny.core.db_manager.settings") as mock_settings:
        mock_settings.DATASOURCE_DB_DRIVERNAME = "mssql"

        mock_func = MagicMock(return_value="success")
        decorated_func = WakeAzureDB()(mock_func)

        result = decorated_func("arg1", kwarg1="kwarg1")

        assert result == "success"
        mock_func.assert_called_once_with("arg1", kwarg1="kwarg1")


def test_retry_on_specific_error():
    """Test that the function retries on the specific Azure DB error."""
    with patch("hutch_bunny.core.db_manager.settings") as mock_settings, \
         patch('hutch_bunny.core.db_manager.time.sleep') as mock_sleep, \
         patch('hutch_bunny.core.db_manager.logger') as mock_logger:

        mock_settings.DATASOURCE_DB_DRIVERNAME = "mssql"

        # Mock function that fails once with the specific error, then succeeds
        mock_func = MagicMock(side_effect=[
            OperationalError("Error code 40613: The database is currently busy."), 
            "success"
        ])

        decorated_func = WakeAzureDB(retries=2, delay=5, error_code="40613")(mock_func)

        result = decorated_func()

        assert result == "success"
        assert mock_func.call_count == 2
        mock_sleep.assert_called_once_with(5)
        mock_logger.info.assert_called_once()


def test_raises_after_max_retries():
    """Test that the function raises after maximum retries are exhausted."""
    with patch("hutch_bunny.core.db_manager.settings") as mock_settings, \
         patch('hutch_bunny.core.db_manager.time.sleep') as mock_sleep, \
         patch('hutch_bunny.core.db_manager.logger') as mock_logger:

        mock_settings.DATASOURCE_DB_DRIVERNAME = "mssql"

        # Create an error with the specific error code
        error = OperationalError("Error code 40613: The database is currently busy.")

        # Mock function that always fails with the specific error
        mock_func = MagicMock(side_effect=error)

        decorated_func = WakeAzureDB(retries=2, delay=5, error_code="40613")(mock_func)

        # The function should raise after retries are exhausted
        with pytest.raises(OperationalError) as excinfo:
            decorated_func()

        assert "40613" in str(excinfo.value)
        assert mock_func.call_count == 3  # Initial call + 2 retries
        assert mock_sleep.call_count == 2
        assert mock_logger.info.call_count == 2
        mock_logger.error.assert_called_once()


def test_different_error_passes_through():
    """Test that different errors pass through without retry."""
    with patch("hutch_bunny.core.db_manager.settings") as mock_settings:
        mock_settings.DATASOURCE_DB_DRIVERNAME = "mssql"

        # Create an error with a different error code
        error = OperationalError("Error code 12345: Some other error.")

        # Mock function that fails with a different error
        mock_func = MagicMock(side_effect=error)

        decorated_func = WakeAzureDB()(mock_func)

        # The function should immediately raise with the different error
        with pytest.raises(OperationalError) as excinfo:
            decorated_func()

        assert "12345" in str(excinfo.value)
        mock_func.assert_called_once()


def test_preserves_function_metadata():
    """Test that the decorator preserves the original function's metadata."""
    with patch("hutch_bunny.core.db_manager.settings") as mock_settings:
        mock_settings.DATASOURCE_DB_DRIVERNAME = "mssql"

        @WakeAzureDB()
        def test_func():
            """Test function docstring."""
            pass

        assert test_func.__name__ == "test_func"
        assert test_func.__doc__ == "Test function docstring."
