import pytest
from pydantic import ValidationError
from src.hutch_bunny.core.config import DaemonSettings, Settings
from unittest.mock import patch


@pytest.mark.unit
def test_https_validation_enforced() -> None:
    """
    Verifies that an error is raised when HTTPS is enforced but not used.
    """
    # Arrange & Act & Assert
    with pytest.raises(ValidationError) as excinfo:
        DaemonSettings(
            task_api__TASK_API_BASE_URL="http://example.com",
            task_api__TASK_API_ENFORCE_HTTPS=True,
            task_api__TASK_API_USERNAME="user",
            task_api__TASK_API_PASSWORD="password",
            task_api__COLLECTION_ID="test",
            database__DATASOURCE_DB_PASSWORD="db_password",
            database__DATASOURCE_DB_HOST="localhost",
            database__DATASOURCE_DB_PORT=5432,
            database__DATASOURCE_DB_SCHEMA="public",
            database__DATASOURCE_DB_DATABASE="test_db",
        )

    # Check that the error message contains the expected text
    error_msg = str(excinfo.value)
    assert "HTTPS is required for the task API but not used" in error_msg
    assert "Set TASK_API_ENFORCE_HTTPS to false" in error_msg


@pytest.mark.unit
@patch("src.hutch_bunny.core.config.task_api.logger")
def test_https_validation_not_enforced(mock_logger) -> None:
    """
    Verifies that a warning is logged when HTTPS is not enforced but not used.
    """
    # Arrange & Act
    settings = DaemonSettings(
        task_api__TASK_API_BASE_URL="http://example.com",
        task_api__TASK_API_ENFORCE_HTTPS=False,
        task_api__TASK_API_USERNAME="user",
        task_api__TASK_API_PASSWORD="password",
        COLLECTION_ID="test",
        database__DATASOURCE_DB_PASSWORD="db_password",
        database__DATASOURCE_DB_HOST="localhost",
        database__DATASOURCE_DB_PORT=5432,
        database__DATASOURCE_DB_SCHEMA="public",
        database__DATASOURCE_DB_DATABASE="test_db",
    )

    # Assert
    assert settings.task_api.TASK_API_BASE_URL == "http://example.com"
    assert settings.task_api.TASK_API_ENFORCE_HTTPS is False
    mock_logger.warning.assert_called_once_with(
        "HTTPS is not used for the task API. This is not recommended in production environments."
    )


@pytest.mark.unit
@patch("src.hutch_bunny.core.config.task_api.logger")
def test_https_validation_https_used(mock_logger) -> None:
    """
    Verifies that no error or warning is raised when HTTPS is used.
    """
    # Arrange & Act
    settings = DaemonSettings(
        task_api__TASK_API_BASE_URL="https://example.com",
        task_api__TASK_API_ENFORCE_HTTPS=True,
        task_api__TASK_API_USERNAME="user",
        task_api__TASK_API_PASSWORD="password",
        COLLECTION_ID="test",
        database__DATASOURCE_DB_PASSWORD="db_password",
        database__DATASOURCE_DB_HOST="localhost",
        database__DATASOURCE_DB_PORT=5432,
        database__DATASOURCE_DB_SCHEMA="public",
        database__DATASOURCE_DB_DATABASE="test_db",
    )

    # Assert
    assert settings.task_api.TASK_API_BASE_URL == "https://example.com"
    assert settings.task_api.TASK_API_ENFORCE_HTTPS is True
    mock_logger.warning.assert_not_called()


@pytest.mark.unit
def test_base_settings_safe_model_dump() -> None:
    """
    Verifies that safe_model_dump in the base Settings class excludes sensitive fields.
    """
    # Arrange
    settings = Settings(
        database__DATASOURCE_DB_PASSWORD="db_secret",
        database__DATASOURCE_DB_HOST="localhost",
        database__DATASOURCE_DB_PORT=5432,
        database__DATASOURCE_DB_SCHEMA="public",
        database__DATASOURCE_DB_DATABASE="test_db",
    )

    # Act
    safe_dump = settings.safe_model_dump()

    # Assert
    assert "DATASOURCE_DB_PASSWORD" not in str(safe_dump)
    assert "DATASOURCE_DB_HOST" in str(safe_dump)
    assert "DATASOURCE_DB_PORT" in str(safe_dump)
    assert "COLLECTION_ID" not in str(safe_dump)


@pytest.mark.unit
def test_daemon_settings_safe_model_dump() -> None:
    """
    Verifies that safe_model_dump in the DaemonSettings class excludes sensitive fields.
    """
    # Arrange
    settings = DaemonSettings(
        task_api__TASK_API_BASE_URL="https://example.com",
        task_api__TASK_API_ENFORCE_HTTPS=True,
        task_api__TASK_API_USERNAME="user",
        task_api__TASK_API_PASSWORD="secret_password",
        COLLECTION_ID="test",
        database__DATASOURCE_DB_PASSWORD="db_secret",
        database__DATASOURCE_DB_HOST="localhost",
        database__DATASOURCE_DB_PORT=5432,
        database__DATASOURCE_DB_SCHEMA="public",
        database__DATASOURCE_DB_DATABASE="test_db",
    )

    # Act
    safe_dump = settings.safe_model_dump()

    # Assert
    assert "TASK_API_PASSWORD" not in str(safe_dump)
    assert "DATASOURCE_DB_PASSWORD" not in str(safe_dump)
    assert "TASK_API_BASE_URL" in str(safe_dump)
    assert "TASK_API_USERNAME" in str(safe_dump)
