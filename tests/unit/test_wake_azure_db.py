import pytest
from unittest.mock import patch
from sqlalchemy.exc import OperationalError
from hutch_bunny.core.db_manager import WakeAzureDB


@WakeAzureDB(retries=2, delay=1, error_code="40613")
def mock_function():
    """A mock function to test the WakeAzureDB decorator."""
    raise OperationalError("OperationalError with code 40613", None, None)


@WakeAzureDB(retries=2, delay=1, error_code="40613")
def mock_function_no_error():
    """A mock function that succeeds without errors."""
    return "Success"


@pytest.mark.unit
def test_wake_azure_db_retries():
    """Test that the decorator retries the specified number of times."""
    with patch("time.sleep", return_value=None) as mock_sleep:
        with pytest.raises(OperationalError):
            mock_function()
        assert mock_sleep.call_count == 2  # Retries twice


@pytest.mark.unit
def test_wake_azure_db_success():
    """Test that the decorator allows successful execution."""
    result = mock_function_no_error()
    assert result == "Success"


@pytest.mark.unit
def test_wake_azure_db_non_matching_error():
    """Test that the decorator does not retry for non-matching error codes."""
    @WakeAzureDB(retries=2, delay=1, error_code="40613")
    def mock_function_different_error():
        raise OperationalError("OperationalError with different code", None, None)

    with pytest.raises(OperationalError):
        mock_function_different_error()
