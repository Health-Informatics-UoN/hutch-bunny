import logging
from typing import List, Protocol, runtime_checkable

logger = logging.getLogger("hutch_bunny")


@runtime_checkable
class LoggerSettings(Protocol):
    """Protocol for logger settings"""

    MSG_FORMAT: str
    DATE_FORMAT: str
    LOGGER_LEVEL: str
    COLLECTION_ID: str


class RedactValueFilter(logging.Filter):
    """
    Filter to redact sensitive values from log messages.
    """

    def __init__(
        self, values_to_redact: List[str], redaction_text: str = "[REDACTED]"
    ) -> None:
        """
        Initialize the filter with the values to redact and the redaction text.
        """
        self.values_to_redact = values_to_redact
        self.redaction_text = redaction_text

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Filter the log record.

        Args:
            record: The log record to filter.

        Returns:
            True if the record should be filtered, False otherwise.
        """
        msg = str(record.getMessage())
        for val in self.values_to_redact:
            if val:
                msg = msg.replace(val, self.redaction_text)
        record.msg = msg
        record.args = ()
        return True


def configure_logger(settings: LoggerSettings) -> None:
    """
    Configure the logger with the given settings.

    Args:
        settings: The settings to configure the logger with.
    """
    LOG_FORMAT = logging.Formatter(settings.MSG_FORMAT, datefmt=settings.DATE_FORMAT)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(LOG_FORMAT)

    # Create a filter to redact sensitive information
    sensitive_values = []

    # Add collection_id to sensitive values if it exists
    if hasattr(settings, "COLLECTION_ID") and settings.COLLECTION_ID:
        sensitive_values.append(settings.COLLECTION_ID)

    sensitive_filter = RedactValueFilter(sensitive_values)

    # Add the filter to the handler
    console_handler.addFilter(sensitive_filter)

    logger.setLevel(settings.LOGGER_LEVEL)
    logger.addHandler(console_handler)
