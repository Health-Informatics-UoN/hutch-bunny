import time
from unittest.mock import patch, Mock
from sqlalchemy.exc import OperationalError
from decorators import WakeAzureDB  # Adjust import based on actual file structure

# Setting up mock components
class MockSettings:
    DATASOURCE_DB_DRIVERNAME = "mssql"

@pytest.fixture
def mock_settings():
    return MockSettings()

@pytest.fixture
def mock_logger():
    with patch("decorators.logger") as mock_logger:
        yield mock_logger

# A sample function to apply the decorator
@WakeAzureDB(retries=2, delay=1, error_code="40613")
def sample_function():
    raise OperationalError("An error occurred", params=None, orig=None)

def test_retry_decorator_success(mock_settings, mock_logger):
    # Patch the settings globally
    with patch("decorators.settings", mock_settings):
        # Patch time.sleep to avoid actual sleeping during tests
        with patch("time.sleep", return_value=None):
            with patch("decorators.sample_function", return_value="Success") as mock_func:
                # Modify sample_function to first raise error, then succeed
                mock_func.side_effect = [
                    OperationalError("An error 40613 occurred", None, None),
                    OperationalError("An error 40613 occurred", None, None),
                    "Success"
                ]
                # Call the decorated function
                result = sample_function()

                # Assert the result
                assert result == "Success"
                # Assert logger calls
                assert mock_logger.info.call_count == 2
                assert mock_logger.error.call_count == 0

def test_retry_decorator_fail(mock_settings, mock_logger):
    # Patch the settings globally
    with patch("decorators.settings", mock_settings):
        # Patch time.sleep to avoid actual sleeping during tests
        with patch("time.sleep", return_value=None):
            with patch("decorators.sample_function", side_effect=OperationalError("An error 40613 occurred", None, None)):
                # Check if the retry mechanism works and eventually raises an error
                with pytest.raises(OperationalError):
                    sample_function()
                
                # Assert logger calls
                assert mock_logger.info.call_count == 2
                assert mock_logger.error.call_count == 1
