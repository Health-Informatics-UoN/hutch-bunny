import logging
from hutch_bunny.core.config.logging import LoggingSettings
from hutch_bunny.core.config.task_api import TaskApiSettings


# Define logging level constants for retry
INFO = logging.INFO

logger = logging.getLogger("hutch_bunny")


class RedactValueFilter(logging.Filter):
    """
    Filter to redact sensitive values from log messages.
    """

    def __init__(
        self, values_to_redact: list[str], redaction_text: str = "[REDACTED]"
    ) -> None:
        """
        Initialise the filter with the values to redact and the redaction text.
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


def configure_logger(settings=None) -> None:  # type: ignore
    """
    Configure the logger with the given settings.

    Args:
        settings: Optional settings object. If not provided, creates default settings.
        # type: ignore is to prevent a circular import just for type checking.
    """
    logging_settings = LoggingSettings()
    task_api_settings = TaskApiSettings() if settings is None else None
    
    # Try to get task_api settings from passed settings if available
    if settings is not None and hasattr(settings, "task_api"):
        task_api_settings = settings.task_api
    
    LOG_FORMAT = logging.Formatter(
        logging_settings.MSG_FORMAT, datefmt=logging_settings.DATE_FORMAT
    )
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(LOG_FORMAT)

    # Create a filter to redact collection_id
    sensitive_values: list[str] = []

    if task_api_settings and task_api_settings.COLLECTION_ID:
        sensitive_values.append(task_api_settings.COLLECTION_ID)

    sensitive_filter = RedactValueFilter(sensitive_values)
    console_handler.addFilter(sensitive_filter)

    logger.setLevel(logging_settings.LOGGER_LEVEL)
    logger.addHandler(console_handler)
