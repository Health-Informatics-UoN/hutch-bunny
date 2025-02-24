import logging
import sys
from logging import Logger
from hutch_bunny.core.settings import Settings

def setup_logger(settings: Settings) -> Logger:
    """
    Sets up a logger with the given settings.

    Args:
        settings (Settings): The settings to use for the logger.

    Returns:
        Logger: The logger.
    """
    logger = logging.getLogger(settings.LOGGER_NAME)
    LOG_FORMAT = logging.Formatter(
        settings.MSG_FORMAT,
        datefmt=settings.DATE_FORMAT,
    )
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(LOG_FORMAT)
    logger.setLevel(settings.LOGGER_LEVEL)
    logger.addHandler(console_handler)
    return logger
