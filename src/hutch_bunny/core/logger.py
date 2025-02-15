import logging
from hutch_bunny.core.settings import get_settings
import sys

settings = get_settings()

logger = logging.getLogger(settings.LOGGER_NAME)
LOG_FORMAT = logging.Formatter(
    settings.MSG_FORMAT,
    datefmt=settings.DATE_FORMAT,
)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(LOG_FORMAT)
logger = logging.getLogger(settings.LOGGER_NAME)
logger.setLevel(settings.LOGGER_LEVEL)
logger.addHandler(console_handler)
