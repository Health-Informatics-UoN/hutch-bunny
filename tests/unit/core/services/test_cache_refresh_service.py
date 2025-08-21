import tempfile
from datetime import datetime, timedelta 
import pytest 
from unittest.mock import Mock, MagicMock, patch 

from hutch_bunny.core.services.cache_refresh_service import CacheRefreshService 


pytest.fixture
@pytest.fixture 
def mock_settings() -> Mock: 
    settings = Mock() 
    settings.CACHE_ENABLED = True 
    settings.CACHE_REFRESH_ON_STARTUP = True
    settings.CACHE_DIR = tempfile.mkdtemp()
    settings.CACHE_TTL_HOURS = 24 
    settings.COLLECTION_ID = "test123"
    settings.LOW_NUMBER_SUPPRESSION_THRESHOLD = 10
    settings.ROUNDING_TARGET = 10
    return settings 


@pytest.fixture 
def service(mock_settings: Mock) -> CacheRefreshService: 
    return CacheRefreshService(mock_settings)


def test_start_with_cache_disabled(mock_settings: Mock) -> None: 
    mock_settings.CACHE_ENABLED = False
    service = CacheRefreshService(mock_settings)

    with patch.object(service, "_refresh_cache") as mock_refresh: 
        service.start()
        assert service.thread is None 
        assert service.running is False 
        mock_refresh.assert_not_called()


def test_start_with_zero_ttl(mock_settings: Mock) -> None: 
    mock_settings.CACHE_TTL_HOURS = 0 
    service = CacheRefreshService(mock_settings)
        
    with patch.object(service, '_refresh_cache') as mock_refresh:
        service.start()
        assert service.thread is None
        assert service.running is False
        mock_refresh.assert_not_called()


@patch('hutch_bunny.core.services.cache_refresh_service.get_db_manager')
@patch('hutch_bunny.core.services.cache_refresh_service.execute_query')
def test_cache_populates_on_startup(
    mock_execute: Mock, 
    mock_get_db: Mock, 
    service: CacheRefreshService, 
    mock_settings: Mock 
) -> None:
    mock_settings.CACHE_REFRESH_ON_STARTUP = True
    
    service.start()
    
    # Verify initial cache population
    assert mock_execute.call_count > 0
    assert service.last_refresh is not None
    assert service.running is True
    assert service.thread is not None
    
    service.stop()
