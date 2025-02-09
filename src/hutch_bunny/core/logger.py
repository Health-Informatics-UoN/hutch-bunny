import logging
import sys
from hutch_bunny.core.settings import get_settings

settings = get_settings()

logger = logging.getLogger(settings.LOGGER_NAME)
if not logger.hasHandlers():
    LOG_FORMAT = logging.Formatter(
        settings.MSG_FORMAT,
        datefmt=settings.DATE_FORMAT,
    )
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(LOG_FORMAT)
    logger.setLevel(getattr(logging, settings.LOGGER_LEVEL.upper(), logging.INFO))
    logger.addHandler(console_handler)
