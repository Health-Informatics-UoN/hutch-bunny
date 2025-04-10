import pytest
from pydantic import ValidationError
from src.hutch_bunny.core.settings import DaemonSettings, Settings
from unittest.mock import patch


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
@patch("src.hutch_bunny.core.settings.logger")
def test_https_validation_not_enforced(mock_logger) -> None:
    """
    Verifies that a warning is logged when HTTPS is not enforced but not used.
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
    mock_logger.warning.assert_called_once_with(
        "HTTPS is not used for the task API. This is not recommended in production environments."
    )


@pytest.mark.unit
@patch("src.hutch_bunny.core.settings.logger")
def test_https_validation_https_used(mock_logger) -> None:
    """
    Verifies that no error or warning is raised when HTTPS is used.
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
    mock_logger.warning.assert_not_called()


@pytest.mark.unit
def test_base_settings_safe_model_dump() -> None:
    """
    Verifies that safe_model_dump in the base Settings class excludes sensitive fields.
    """
    # Arrange
    settings = Settings(
        DATASOURCE_DB_PASSWORD="db_secret",
        DATASOURCE_DB_HOST="localhost",
        DATASOURCE_DB_PORT=5432,
        DATASOURCE_DB_SCHEMA="public",
        DATASOURCE_DB_DATABASE="test_db",
    )

    # Act
    safe_dump = settings.safe_model_dump()

    # Assert
    assert "DATASOURCE_DB_PASSWORD" not in safe_dump
    assert "DATASOURCE_DB_HOST" in safe_dump
    assert "DATASOURCE_DB_PORT" in safe_dump


@pytest.mark.unit
def test_daemon_settings_safe_model_dump() -> None:
    """
    Verifies that safe_model_dump in the DaemonSettings class excludes sensitive fields.
    """
    # Arrange
    settings = DaemonSettings(
        TASK_API_BASE_URL="https://example.com",
        TASK_API_ENFORCE_HTTPS=True,
        TASK_API_USERNAME="user",
        TASK_API_PASSWORD="secret_password",
        COLLECTION_ID="test",
        DATASOURCE_DB_PASSWORD="db_secret",
        DATASOURCE_DB_HOST="localhost",
        DATASOURCE_DB_PORT=5432,
        DATASOURCE_DB_SCHEMA="public",
        DATASOURCE_DB_DATABASE="test_db",
    )

    # Act
    safe_dump = settings.safe_model_dump()

    # Assert
    assert "TASK_API_PASSWORD" not in safe_dump
    assert "DATASOURCE_DB_PASSWORD" not in safe_dump
    assert "TASK_API_BASE_URL" in safe_dump
    assert "TASK_API_USERNAME" in safe_dump
    assert "COLLECTION_ID" in safe_dump
