import logging

logger = logging.getLogger("hutch_bunny")


def configure_logger(settings) -> None:
    LOG_FORMAT = logging.Formatter(settings.MSG_FORMAT, datefmt=settings.DATE_FORMAT)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(LOG_FORMAT)

    logger.setLevel(settings.LOGGER_LEVEL)
    logger.addHandler(console_handler)
