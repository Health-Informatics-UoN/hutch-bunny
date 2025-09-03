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


@patch('hutch_bunny.core.services.cache_refresh_service.get_db_client')
@patch('hutch_bunny.core.services.cache_refresh_service.execute_query')
def test_cache_populates_on_startup(
    mock_execute: Mock, 
    mock_get_db: Mock, 
    service: CacheRefreshService, 
    mock_settings: Mock 
) -> None:
    mock_settings.CACHE_REFRESH_ON_STARTUP = True
    
    service.start()
    
    assert mock_execute.call_count > 0
    assert service.last_refresh is not None
    assert service.running is True
    assert service.thread is not None
    
    service.stop()


@patch('hutch_bunny.core.services.cache_refresh_service.get_db_client')
@patch('hutch_bunny.core.services.cache_refresh_service.execute_query')
def test_startup_continues_on_cache_failure(
    mock_execute: Mock, 
    mock_get_db_client: Mock, 
    service: CacheRefreshService, 
    mock_settings: Mock
) -> None:
    mock_execute.side_effect = Exception("DB connection failed")
    
    service.start()
    
    # Service should still start despite cache failure
    assert service.running is True
    assert service.thread is not None
    
    service.stop()


@patch('hutch_bunny.core.services.cache_refresh_service.get_db_client')
@patch('hutch_bunny.core.services.cache_refresh_service.execute_query')
def test_refresh_loop_timing(
    mock_execute: Mock, 
    mock_get_db_client: Mock, 
    service: CacheRefreshService, 
    mock_settings: Mock
) -> None:
    mock_settings.CACHE_TTL_HOURS = 0.0167

    with patch("time.sleep") as mock_sleep: 
        current_time = datetime.now() 
        times = [
            current_time, 
            current_time + timedelta(minutes=2), 
            current_time + timedelta(minutes=3)
        ]

        with patch('hutch_bunny.core.services.cache_refresh_service.datetime') as mock_datetime:
            mock_datetime.now.side_effect = times
            
            service.last_refresh = current_time
            service.running = True 
            
            sleep_calls = 0 
            def stop_after_two_sleeps(seconds: int) -> None:  # have to pass positional argument as this is what time.sleep expects 
                nonlocal sleep_calls
                sleep_calls += 1
                if sleep_calls == 2: 
                    service.running = False 
            mock_sleep.side_effect = stop_after_two_sleeps

            service._refresh_loop()

        assert mock_execute.call_count > 0


def test_thread_is_daemon(service: CacheRefreshService, mock_settings: Mock) -> None: 
    with patch.object(service, "_refresh_cache"): 
        service.start() 

        assert service.thread is not None 
        assert service.thread.daemon is True 

        service.stop() 


def test_stop_method(service: CacheRefreshService) -> None: 
    with patch("time.sleep", return_value=None):
        with patch.object(service, '_refresh_cache'):
            service.start()
            assert service.running is True
            
            service.stop()
            assert service.running is False
            
            assert not service.thread.is_alive()


@patch('hutch_bunny.core.services.cache_refresh_service.get_db_client')
@patch('hutch_bunny.core.services.cache_refresh_service.execute_query')
@patch('hutch_bunny.core.services.cache_refresh_service.results_modifiers')
def test_refresh_cache_content(
    mock_modifiers: Mock, 
    mock_execute: Mock, 
    mock_get_db_client: Mock, 
    service: CacheRefreshService, 
    mock_settings: Mock
) -> None:
    mock_modifiers.return_value = [{"id": "test", "value": 10}]

    service._refresh_cache()

    calls = mock_execute.call_args_list
    query_codes = [call[0][0]['code'] for call in calls]

    assert 'DEMOGRAPHICS' in query_codes
    assert 'GENERIC' in query_codes

    mock_modifiers.assert_called_with(
        low_number_suppression_threshold=mock_settings.LOW_NUMBER_SUPPRESSION_THRESHOLD,
        rounding_target=mock_settings.ROUNDING_TARGET
    )
