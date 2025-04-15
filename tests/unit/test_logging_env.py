import pytest
import os
from importlib import reload
import logging

from hutch_bunny.core.settings import Settings
import hutch_bunny.core.logger


def test_configure_logger():
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

    # Test invalid level raises validation error
    os.environ["BUNNY_LOGGER_LEVEL"] = "FLOPPSY"
    with pytest.raises(ValueError, match="pattern"):
        reload(hutch_bunny.core.settings)
        settings = Settings()
        hutch_bunny.core.logger.configure_logger(settings)
