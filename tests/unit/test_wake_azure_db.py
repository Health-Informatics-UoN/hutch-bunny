import pytest
from unittest.mock import patch, MagicMock
from sqlalchemy.exc import OperationalError
import time

# Correct import path for the decorator
from hutch_bunny.core.db_manager import WakeAzureDB

# Mock settings to simulate the environment
mock_settings = MagicMock()
mock_settings.DATASOURCE_DB_DRIVERNAME = "mssql"

# Helper function to simulate function calls that may raise an exception
def simulated_function(should_raise=False, error_code="40613"):
    if should_raise:
        raise OperationalError("Dummy", "Dummy", error_code)

# Tests

def test_no_error():
    # Test normal execution
    with patch('hutch_bunny.core.db_manager.settings', mock_settings):
        decorated_func = WakeAzureDB()(simulated_function)
        decorated_func()

def test_with_error():
    with patch('hutch_bunny.core.db_manager.settings', mock_settings):
        # Mock sleep to avoid actual delay
        with patch('time.sleep', return_value=None):
            # Simulate function failure with relevant error code
            decorated_func = WakeAzureDB()(lambda: simulated_function(True, "40613"))
            with pytest.raises(OperationalError):
                decorated_func()

def test_with_different_error():
    with patch('hutch_bunny.core.db_manager.settings', mock_settings):
        # Simulate different error code, expecting no retry
        decorated_func = WakeAzureDB()(lambda: simulated_function(True, "DifferentError"))
        with pytest.raises(OperationalError):
            decorated_func()

if __name__ == "__main__":
    pytest.main()
