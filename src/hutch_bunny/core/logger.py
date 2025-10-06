import logging


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


def configure_logger(settings) -> None:  # type: ignore
    """
    Configure the logger with the given settings.

    Args:
        settings: The settings to configure the logger with.
        # type: ignore is to prevent a circular import just for type checking.
    """
    LOG_FORMAT = logging.Formatter(
        settings.logging.MSG_FORMAT, datefmt=settings.logging.DATE_FORMAT
    )
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(LOG_FORMAT)

    # Create a filter to redact collection_id
    sensitive_values: list[str] = []

    if hasattr(settings, "COLLECTION_ID") and settings.COLLECTION_ID:
        sensitive_values.append(settings.COLLECTION_ID)

    sensitive_filter = RedactValueFilter(sensitive_values)
    console_handler.addFilter(sensitive_filter)

    logger.setLevel(settings.logging.LOGGER_LEVEL)
    logger.addHandler(console_handler)
