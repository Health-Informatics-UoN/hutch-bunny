import pytest
import os
from importlib import reload

import hutch_bunny.core.logger
import hutch_bunny.core.settings


def test_set_level():
    # Test INFO level
    os.environ["BUNNY_LOGGER_LEVEL"] = "INFO"
    reload(hutch_bunny.core.settings)
    settings = hutch_bunny.core.settings.get_settings()
    assert settings.LOGGER_LEVEL == "INFO"
    reload(hutch_bunny.core.logger)
    assert hutch_bunny.core.logger.logger.level == 20

    # Test DEBUG level
    os.environ["BUNNY_LOGGER_LEVEL"] = "DEBUG" 
    reload(hutch_bunny.core.settings)
    settings = hutch_bunny.core.settings.get_settings()
    assert settings.LOGGER_LEVEL == "DEBUG"
    reload(hutch_bunny.core.logger)
    assert hutch_bunny.core.logger.logger.level == 10

    # Test invalid level raises validation error
    os.environ["BUNNY_LOGGER_LEVEL"] = "FLOPPSY"
    with pytest.raises(ValueError, match="pattern"):
        reload(hutch_bunny.core.settings)
        hutch_bunny.core.settings.get_settings()
