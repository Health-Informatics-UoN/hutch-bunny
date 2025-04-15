import logging
import pytest
from unittest.mock import MagicMock, patch
from typing import Generator
import os
from importlib import reload

# Import the entire modules instead of specific functions
# To allow reload of settings for testing
import hutch_bunny.core.logger
import hutch_bunny.core.settings
from hutch_bunny.core.settings import Settings, DaemonSettings


@pytest.fixture
def log_record() -> MagicMock:
    """Fixture to create a mock log record"""
    record = MagicMock(spec=logging.LogRecord)
    record.getMessage.return_value = "Test log message"
    record.args = ()
    return record


@pytest.fixture
def mock_logger() -> Generator[MagicMock, None, None]:
    """Fixture to mock the logger"""
    with patch("hutch_bunny.core.logger.logger") as mock_logger:
        yield mock_logger


def test_configure_logger() -> None:
    """Test the configure_logger function"""
    # Test invalid level raises validation error
    os.environ["BUNNY_LOGGER_LEVEL"] = "FLOPPSY"
    with pytest.raises(ValueError, match="pattern"):
        reload(hutch_bunny.core.settings)
        settings = Settings()
        hutch_bunny.core.logger.configure_logger(settings)

    # Test INFO level
    os.environ["BUNNY_LOGGER_LEVEL"] = "INFO"
    reload(hutch_bunny.core.settings)
    settings = Settings()
    hutch_bunny.core.logger.configure_logger(settings)
    logger = logging.getLogger("hutch_bunny")
    assert logger.level == logging.INFO

    # Test DEBUG level
    os.environ["BUNNY_LOGGER_LEVEL"] = "DEBUG"
    reload(hutch_bunny.core.settings)
    settings = Settings()
    hutch_bunny.core.logger.configure_logger(settings)
    assert logger.level == logging.DEBUG


def test_redact_filter_init() -> None:
    """Test initialization of RedactValueFilter"""
    filter = hutch_bunny.core.logger.RedactValueFilter(["test_value"])
    assert filter.values_to_redact == ["test_value"]
    assert filter.redaction_text == "[REDACTED]"


def test_redact_filter_custom_text() -> None:
    """Test initialization with custom redaction text"""
    filter = hutch_bunny.core.logger.RedactValueFilter(
        ["test_value"], redaction_text="hello"
    )
    assert filter.values_to_redact == ["test_value"]
    assert filter.redaction_text == "hello"


def test_redact_filter_single_value(log_record: MagicMock) -> None:
    """Test filtering a single value from a log message"""
    filter = hutch_bunny.core.logger.RedactValueFilter(["sensitive_data"])
    log_record.getMessage.return_value = "This contains sensitive_data in the message"

    result = filter.filter(log_record)

    assert result is True
    assert log_record.msg == "This contains [REDACTED] in the message"
    assert log_record.args == ()


def test_redact_filter_multiple_values(log_record: MagicMock) -> None:
    """Test filtering multiple values from a log message"""
    filter = hutch_bunny.core.logger.RedactValueFilter(["value1", "value2"])
    log_record.getMessage.return_value = "Message with value1 and value2"

    result = filter.filter(log_record)

    assert result is True
    assert log_record.msg == "Message with [REDACTED] and [REDACTED]"
    assert log_record.args == ()


def test_redact_filter_no_matches(log_record: MagicMock) -> None:
    """Test filtering when no values match"""
    filter = hutch_bunny.core.logger.RedactValueFilter(["nonexistent"])
    log_record.getMessage.return_value = "This message has no matches"

    result = filter.filter(log_record)

    assert result is True
    assert log_record.msg == "This message has no matches"
    assert log_record.args == ()


def test_configure_logger_basic(mock_logger: MagicMock) -> None:
    """Test basic logger configuration"""
    settings = DaemonSettings()
    settings.COLLECTION_ID = "test_collection"
    hutch_bunny.core.logger.configure_logger(settings)

    # Assert that the logger level was set
    assert mock_logger.setLevel.call_count == 1

    # Assert that a handler was added
    assert mock_logger.addHandler.call_count == 1

    # Get the handler that was added
    handler = mock_logger.addHandler.call_args[0][0]

    # Assert that the handler has a formatter
    assert handler.formatter is not None

    # Assert that the handler has a filter and it works
    assert len(handler.filters) == 1
    assert isinstance(handler.filters[0], hutch_bunny.core.logger.RedactValueFilter)
    assert handler.filters[0].values_to_redact == ["test_collection"]
