import pytest
from unittest.mock import patch
from sqlalchemy.exc import OperationalError
from hutch_bunny.core.db_manager import WakeAzureDB


# Mock functions at the top of the file
@WakeAzureDB(retries=2, delay=1, error_code="40613")
def mock_function_with_matching_error():
    """A mock function that raises an error with the matching error code."""
    raise OperationalError("(40613) Database is currently unavailable", None, None)


@WakeAzureDB(retries=2, delay=1, error_code="40613")
def mock_function_with_non_matching_error():
    """A mock function that raises an error with a non-matching error code."""
    raise OperationalError("(40615) Some other error", None, None)


@WakeAzureDB(retries=2, delay=1, error_code="40613")
def mock_function_success():
    """A mock function that succeeds without errors."""
    return "Success"


@pytest.mark.unit
class TestWakeAzureDB:
    """Tests for the WakeAzureDB decorator."""

    def setup_method(self):
        """Set up common test components."""
        self.sleep_patcher = patch("time.sleep", return_value=None)
        self.mock_sleep = self.sleep_patcher.start()

    def teardown_method(self):
        """Clean up after each test."""
        self.sleep_patcher.stop()

    def test_matching_error_retries(self):
        """Test that the decorator retries when error code matches."""
        # Reset call count (in case tests run in a different order)
        self.mock_sleep.reset_mock()

        # Should retry twice and then raise
        with pytest.raises(OperationalError) as excinfo:
            mock_function_with_matching_error()

        # Verify error message contains the expected error code
        assert "40613" in str(excinfo.value)

        # Verify sleep was called twice (for retries)
        assert self.mock_sleep.call_count == 2
        assert self.mock_sleep.call_args_list == [call(1), call(1)]

    def test_non_matching_error_no_retry(self):
        """Test that the decorator doesn't retry for non-matching error codes."""
        # Reset call count
        self.mock_sleep.reset_mock()

        # Should raise immediately, no retries
        with pytest.raises(OperationalError) as excinfo:
            mock_function_with_non_matching_error()

        # Verify error message doesn't contain matching code
        assert "40613" not in str(excinfo.value)

        # Verify sleep was not called (no retries)
        assert self.mock_sleep.call_count == 0

    def test_successful_execution(self):
        """Test that the decorator allows successful execution without retries."""
        # Reset call count
        self.mock_sleep.reset_mock()

        # Should succeed without any retries
        result = mock_function_success()

        # Verify the return value
        assert result == "Success"

        # Verify sleep was not called
        assert self.mock_sleep.call_count == 0

    @patch("some_module.settings.DATASOURCE_DB_DRIVERNAME", "postgres")
    def test_non_mssql_driver(self):
        """Test that the decorator doesn't add retry logic for non-MSSQL drivers."""
        # Define a test function with a non-MSSQL driver
        @WakeAzureDB(retries=2, delay=1, error_code="40613")
        def postgres_function():
            raise OperationalError("(40613) Error", None, None)

        # Reset call count
        self.mock_sleep.reset_mock()

        # Should raise immediately, no retries
        with pytest.raises(OperationalError):
            postgres_function()

        # Verify sleep was not called (no retries)
        assert self.mock_sleep.call_count == 0
