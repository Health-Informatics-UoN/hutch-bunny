import pytest
from pydantic import ValidationError
from src.hutch_bunny.core.settings import DaemonSettings


@pytest.mark.unit
def test_https_validation_enforced() -> None:
    """
    Verifies that an error is raised when HTTPS is enforced but not used.
    """
    # Arrange & Act & Assert
    with pytest.raises(ValidationError) as excinfo:
        DaemonSettings(
            TASK_API_BASE_URL="http://example.com",
            TASK_API_ENFORCE_HTTPS=True,
            TASK_API_USERNAME="user",
            TASK_API_PASSWORD="password",
            COLLECTION_ID="test",
            DATASOURCE_DB_PASSWORD="db_password",
            DATASOURCE_DB_HOST="localhost",
            DATASOURCE_DB_PORT=5432,
            DATASOURCE_DB_SCHEMA="public",
            DATASOURCE_DB_DATABASE="test_db",
        )

    # Check that the error message contains the expected text
    error_msg = str(excinfo.value)
    assert "HTTPS is required for the task API but not used" in error_msg
    assert "Set TASK_API_ENFORCE_HTTPS to false" in error_msg


@pytest.mark.unit
def test_https_validation_not_enforced() -> None:
    """
    Verifies that no error is raised when HTTPS is not enforced.
    """
    # Arrange & Act
    settings = DaemonSettings(
        TASK_API_BASE_URL="http://example.com",
        TASK_API_ENFORCE_HTTPS=False,
        TASK_API_USERNAME="user",
        TASK_API_PASSWORD="password",
        COLLECTION_ID="test",
        DATASOURCE_DB_PASSWORD="db_password",
        DATASOURCE_DB_HOST="localhost",
        DATASOURCE_DB_PORT=5432,
        DATASOURCE_DB_SCHEMA="public",
        DATASOURCE_DB_DATABASE="test_db",
    )

    # Assert
    assert settings.TASK_API_BASE_URL == "http://example.com"
    assert settings.TASK_API_ENFORCE_HTTPS is False


@pytest.mark.unit
def test_https_validation_https_used() -> None:
    """
    Verifies that no error is raised when HTTPS is used.
    """
    # Arrange & Act
    settings = DaemonSettings(
        TASK_API_BASE_URL="https://example.com",
        TASK_API_ENFORCE_HTTPS=True,
        TASK_API_USERNAME="user",
        TASK_API_PASSWORD="password",
        COLLECTION_ID="test",
        DATASOURCE_DB_PASSWORD="db_password",
        DATASOURCE_DB_HOST="localhost",
        DATASOURCE_DB_PORT=5432,
        DATASOURCE_DB_SCHEMA="public",
        DATASOURCE_DB_DATABASE="test_db",
    )

    # Assert
    assert settings.TASK_API_BASE_URL == "https://example.com"
    assert settings.TASK_API_ENFORCE_HTTPS is True
