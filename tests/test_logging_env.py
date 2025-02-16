import pytest
import os
import logging
from importlib import reload

import hutch_bunny.core.logger
import hutch_bunny.core.settings


def test_set_level():
    os.environ["BUNNY_LOGGER_LEVEL"] = "INFO"
    reload(hutch_bunny.core.settings)
    reload(hutch_bunny.core.logger)
    assert hutch_bunny.core.logger.logger.level == logging.INFO
    os.environ["BUNNY_LOGGER_LEVEL"] = "DEBUG"
    reload(hutch_bunny.core.settings)
    reload(hutch_bunny.core.logger)
    assert hutch_bunny.core.logger.logger.level == logging.DEBUG
    os.environ["BUNNY_LOGGER_LEVEL"] = "FLOPPSY"
    reload(hutch_bunny.core.settings)
    reload(hutch_bunny.core.logger)
    assert hutch_bunny.core.logger.logger.level == logging.INFO
