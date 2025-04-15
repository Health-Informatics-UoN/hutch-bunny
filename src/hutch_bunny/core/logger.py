import logging
import re
import json
from typing import List, Optional, Protocol, runtime_checkable, Any, Union, Dict

logger = logging.getLogger("hutch_bunny")


@runtime_checkable
class LoggerSettings(Protocol):
    """Protocol for logger settings"""

    MSG_FORMAT: str
    DATE_FORMAT: str
    LOGGER_LEVEL: str
    COLLECTION_ID: str


class SensitiveDataFilter(logging.Filter):
    """
    A filter that redacts sensitive information from log messages.
    """

    def __init__(self, sensitive_values: Optional[List[str]] = None) -> None:
        """
        Initialize the filter with values to redact.

        Args:
            sensitive_values: List of sensitive values to redact from logs.
        """
        super().__init__()
        self.sensitive_values = sensitive_values or []

    def _redact_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recursively redact sensitive values from a dictionary.

        Args:
            data: Dictionary to redact values from.

        Returns:
            Dictionary with sensitive values redacted.
        """
        result: Dict[str, Any] = {}
        for key, value in data.items():
            if isinstance(value, dict):
                result[key] = self._redact_dict(value)
            elif isinstance(value, list):
                result[key] = [
                    self._redact_dict(item)
                    if isinstance(item, dict)
                    else self._redact_string(str(item))
                    for item in value
                ]
            elif isinstance(value, str):
                result[key] = self._redact_string(value)
            else:
                result[key] = value
        return result

    def _redact_string(self, text: str) -> str:
        """
        Redact sensitive values from a string.

        Args:
            text: String to redact values from.

        Returns:
            String with sensitive values redacted.
        """
        if not text:
            return text

        for value in self.sensitive_values:
            if value and value in text:
                # Use regex with word boundaries to ensure we only match the exact value
                pattern = r"\b" + re.escape(value) + r"\b"
                text = re.sub(pattern, "[REDACTED]", text)
        return text

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Filter the log record by redacting sensitive information.

        Args:
            record: The log record to filter.

        Returns:
            True to allow the record to be logged, False to filter it out.
        """
        if not record.args:
            # If there are no arguments, treat msg as a simple string
            if isinstance(record.msg, str):
                record.msg = self._redact_string(record.msg)
            return True

        # Handle string formatting with arguments
        if isinstance(record.msg, str) and record.args:
            # First redact any sensitive information in the arguments
            if isinstance(record.args, (tuple, list)):
                new_args: List[Union[str, int, float, bool, None]] = []
                for arg in record.args:
                    if isinstance(arg, dict):
                        new_args.append(json.dumps(self._redact_dict(arg), indent=2))
                    elif isinstance(arg, str):
                        # Check if the argument is a JSON string
                        try:
                            if arg.strip().startswith("{"):
                                data = json.loads(arg)
                                new_args.append(
                                    json.dumps(self._redact_dict(data), indent=2)
                                )
                            else:
                                new_args.append(self._redact_string(arg))
                        except (json.JSONDecodeError, ValueError):
                            new_args.append(self._redact_string(arg))
                    else:
                        new_args.append(str(arg))
                record.args = tuple(new_args)
            elif isinstance(record.args, dict):
                record.args = self._redact_dict(record.args)

        return True


def configure_logger(settings: LoggerSettings) -> None:
    LOG_FORMAT = logging.Formatter(settings.MSG_FORMAT, datefmt=settings.DATE_FORMAT)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(LOG_FORMAT)

    # Create a filter to redact sensitive information
    sensitive_values = []

    # Add collection_id to sensitive values if it exists
    if hasattr(settings, "COLLECTION_ID") and settings.COLLECTION_ID:
        sensitive_values.append(settings.COLLECTION_ID)

    sensitive_filter = SensitiveDataFilter(sensitive_values)

    # Add the filter to the handler
    console_handler.addFilter(sensitive_filter)

    logger.setLevel(settings.LOGGER_LEVEL)
    logger.addHandler(console_handler)
