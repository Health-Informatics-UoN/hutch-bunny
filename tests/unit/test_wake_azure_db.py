import pytest
from unittest.mock import patch, MagicMock, call
from sqlalchemy.exc import OperationalError

from hutch_bunny.core.db_manager import WakeAzureDB


# Setup test fixtures
@pytest.fixture
def mock_settings() -> MagicMock:
    """Create a mock settings object for testing."""
    settings = MagicMock()
    settings.DATASOURCE_DB_DRIVERNAME = "mssql"
    settings.DATASOURCE_WAKE_DB = True
    return settings


def simulated_function(should_raise: bool = False, error_code: str = "40613") -> str:
    """Test function that optionally raises an OperationalError.

    Args:
        should_raise: Whether to raise an exception
        error_code: Error code to include in the exception message

    Returns:
        A success message if no error is raised

    Raises:
        OperationalError: If should_raise is True
    """
    if should_raise:
        raise OperationalError(f"Error {error_code}", "Dummy", error_code)
    return "Success"


@pytest.mark.unit
def test_no_error(mock_settings: MagicMock) -> None:
    """Test normal execution without errors."""
    with patch('hutch_bunny.core.db_manager.settings', mock_settings):
        decorated_func = WakeAzureDB()(simulated_function)
        result = decorated_func()
        assert result == "Success"


@pytest.mark.unit
def test_with_error_retry_succeeds(mock_settings: MagicMock) -> None:
    """Test that retry works when the second attempt succeeds."""
    with patch('hutch_bunny.core.db_manager.settings', mock_settings):
        with patch('time.sleep', return_value=None) as mock_sleep:
            # Create a function that fails on first call but succeeds on second
            call_count = 0

            def test_func() -> str:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise OperationalError("Error 40613", "Dummy", "40613")
                return "Success after retry"

            decorated_func = WakeAzureDB(retries=2, delay=10)(test_func)
            result = decorated_func()

            assert result == "Success after retry"
            assert call_count == 2
            mock_sleep.assert_called_once_with(10)


@pytest.mark.unit
def test_with_error_all_retries_fail(mock_settings: MagicMock) -> None:
    """Test that after all retries fail, the exception is raised."""
    with patch('hutch_bunny.core.db_manager.settings', mock_settings):
        # Mock sleep to avoid actual delay
        with patch('time.sleep', return_value=None) as mock_sleep:
            # Simulate function always failing with relevant error code
            decorated_func = WakeAzureDB(retries=3, delay=5)(
                lambda: simulated_function(True, "40613")
            )

            with pytest.raises(OperationalError) as exc_info:
                decorated_func()

            assert "40613" in str(exc_info.value)
            assert mock_sleep.call_count == 3
            assert mock_sleep.call_args_list == [call(5), call(5), call(5)]


@pytest.mark.unit
def test_with_different_error(mock_settings: MagicMock) -> None:
    """Test that non-matching error codes don't trigger retries."""
    with patch('hutch_bunny.core.db_manager.settings', mock_settings):
        with patch('time.sleep', return_value=None) as mock_sleep:
            # Simulate different error code, expecting no retry
            decorated_func = WakeAzureDB()(
                lambda: simulated_function(True, "12345")
            )

            with pytest.raises(OperationalError) as exc_info:
                decorated_func()

            assert "12345" in str(exc_info.value)
            mock_sleep.assert_not_called()


@pytest.mark.unit
def test_wake_db_disabled(mock_settings: MagicMock) -> None:
    """Test that when DATASOURCE_WAKE_DB is False, no retry logic is applied."""
    mock_settings.DATASOURCE_WAKE_DB = False

    with patch('hutch_bunny.core.db_manager.settings', mock_settings):
        test_func = MagicMock(side_effect=OperationalError("Error 40613", "Dummy", "40613"))
        decorated_func = WakeAzureDB()(test_func)

        with pytest.raises(OperationalError):
            decorated_func()

        # Function should be called exactly once with no retries
        test_func.assert_called_once()


@pytest.mark.unit
def test_non_mssql_driver(mock_settings: MagicMock) -> None:
    """Test that for non-MSSQL drivers, retry logic is not applied."""
    mock_settings.DATASOURCE_DB_DRIVERNAME = "postgresql"
    mock_settings.DATASOURCE_WAKE_DB = True

    with patch('hutch_bunny.core.db_manager.settings', mock_settings):
        test_func = MagicMock(return_value="Success")
        decorated_func = WakeAzureDB()(test_func)

        result = decorated_func()

        assert result == "Success"
        test_func.assert_called_once()


if __name__ == "__main__":
    pytest.main()
