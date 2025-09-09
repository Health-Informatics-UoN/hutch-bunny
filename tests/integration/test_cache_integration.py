import os
import time 
import threading
import pytest
import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch
from hutch_bunny.core.execute_query import execute_query
from hutch_bunny.core.services.cache_service import DistributionCacheService
from hutch_bunny.core.services.cache_refresh_service import CacheRefreshService 
from hutch_bunny.core.rquest_models.result import RquestResult
from hutch_bunny.core.upstream.polling_service import PollingService
from hutch_bunny.core.upstream.task_handler import handle_task
 

@pytest.fixture
def mock_settings(tmp_path: Path) -> Mock:
    settings = Mock()
    settings.CACHE_ENABLED = True
    settings.CACHE_DIR = str(tmp_path)
    settings.CACHE_TTL_HOURS = 24
    settings.INITIAL_BACKOFF = 1
    settings.MAX_BACKOFF = 5
    settings.POLLING_INTERVAL = 0.01 
    settings.LOW_NUMBER_SUPPRESSION_THRESHOLD = 10
    settings.ROUNDING_TARGET = 10
    return settings


@pytest.fixture 
def distribution_query() -> dict[str, str]: 
    return {
        "code": "DEMOGRAPHICS",
        "analysis": "DISTRIBUTION", 
        "uuid": "test-uuid", 
        "collection": "test-collection", 
        "owner": "test-user" 
    }


@patch('hutch_bunny.core.execute_query.query_solvers.solve_distribution')
def test_first_query_computes_and_caches(
    mock_solve: Mock, 
    distribution_query: dict[str, str], 
    mock_settings: Mock, 
    tmp_path: Path 
) -> None:
    mock_result = RquestResult(
        uuid="test-uuid",
        status="ok",
        collection_id="test-collection",
        count=100
    )

    mock_solve.return_value = mock_result
    mock_db_client = Mock()
    modifiers = []

    result1 = execute_query(
        distribution_query,
        modifiers,
        mock_db_client,
        mock_settings
    )
    assert mock_solve.call_count == 1
    assert result1.count == 100

    cache_service = DistributionCacheService(mock_settings)
    cache_key = cache_service._generate_cache_key(distribution_query, modifiers)
    cache_path = tmp_path / f"{cache_key}.json"
    assert cache_path.exists()

    with open(cache_path) as f: 
        cached_data = json.load(f)
        assert cached_data["count"] == 100  


@patch('hutch_bunny.core.execute_query.query_solvers.solve_distribution')
def test_second_query_uses_cache(
    mock_solve: Mock, 
    distribution_query: dict[str, str], 
    mock_settings: Mock
) -> None:
    mock_result = RquestResult(
        uuid="test-uuid",
        status="ok",
        collection_id="test-collection",
        count=100
    )
    mock_solve.return_value = mock_result

    mock_db_client = Mock()
    modifiers = []

    # First call - should compute 
    result1 = execute_query(
        distribution_query,
        modifiers,
        mock_db_client, 
        mock_settings
    )

    assert mock_solve.call_count == 1
    assert result1.count == 100
    
    # Second call - should use cache
    result2 = execute_query(
        distribution_query,
        modifiers,
        mock_db_client, 
        mock_settings
    )
    
    # solve_distribution should only be called once
    assert mock_solve.call_count == 1
    assert result2.count == 100


@patch('hutch_bunny.core.execute_query.query_solvers.solve_distribution')
def test_different_modifiers_different_cache(
    mock_solve: Mock, 
    distribution_query: dict[str, str], 
    mock_settings: Mock 
) -> None:
    mock_db_manager = Mock()
        
    result_with_rounding_10 = RquestResult(
        uuid="test", 
        status="ok", 
        collection_id="test", 
        count=90  # Already rounded to nearest 10
    )
    result_with_rounding_100 = RquestResult(
        uuid="test", 
        status="ok", 
        collection_id="test", 
        count=100  # Already rounded to nearest 100
    )
    
    mock_solve.side_effect = [result_with_rounding_10, result_with_rounding_100]
    
    modifiers1 = [{"id": "Rounding", "nearest": 10}]
    modifiers2 = [{"id": "Rounding", "nearest": 100}]
    
    res1 = execute_query(distribution_query, modifiers1, mock_db_manager, mock_settings)
    assert res1.count == 90
    
    res2 = execute_query(distribution_query, modifiers2, mock_db_manager, mock_settings)
    assert res2.count == 100
    
    # Both queries should have been computed (not cached)
    assert mock_solve.call_count == 2


@patch('hutch_bunny.core.execute_query.query_solvers.solve_distribution')
def test_expired_cache_recomputes(
    mock_solve: Mock, 
    distribution_query: dict[str, str], 
    mock_settings: Mock,
    tmp_path: Path
) -> None:
    mock_settings.CACHE_TTL_HOURS = 1  
        
    mock_result = RquestResult(
        uuid="test-uuid",
        status="ok",
        collection_id="test-collection",
        count=100
    )
    mock_solve.return_value = mock_result

    mock_db_manager = Mock()
    modifiers = []

    execute_query(distribution_query, modifiers, mock_db_manager, mock_settings)
    assert mock_solve.call_count == 1

    cache_service = DistributionCacheService(mock_settings)
    cache_key = cache_service._generate_cache_key(distribution_query, modifiers)
    cache_path = tmp_path / f"{cache_key}.json"

    assert cache_path.exists()

    # Set file modification time to 2 hours ago
    old_time = (datetime.now() - timedelta(hours=2)).timestamp()
    cache_path.touch()  # Ensure it exists
    os.utime(cache_path, (old_time, old_time))
    
    # Second call - should recompute due to expiration
    execute_query(distribution_query, modifiers, mock_db_manager, mock_settings)
    assert mock_solve.call_count == 2


@patch('hutch_bunny.core.services.cache_refresh_service.get_db_client')
@patch('hutch_bunny.core.services.cache_refresh_service.execute_query')
def test_cache_service_with_polling(mock_execute: Mock, mock_get_db_client: Mock, mock_settings: Mock) -> None:
    cache_service = CacheRefreshService(mock_settings)
    cache_service.start()

    task_calls = []
    def mock_handler(task_data) -> None:
        task_calls.append(task_data)

    polling_service = PollingService(mock_get_db_client, mock_handler, mock_settings)
        
    polling_thread = threading.Thread(
        target=lambda: polling_service.poll_for_tasks(max_iterations=5)
    )
    polling_thread.start()

    time.sleep(0.5)

    assert cache_service.running is True
    assert mock_get_db_client.get.call_count >= 5
    
    cache_service.stop()
    polling_thread.join(timeout=2)


@patch('hutch_bunny.core.execute_query.query_solvers.solve_distribution')
def test_task_handler_uses_cache(mock_solve: Mock, mock_settings: Mock) -> None:
    mock_result = RquestResult(
        uuid="task-uuid",
        status="ok",
        collection_id="test_collection",
        count=150
    )
    mock_solve.return_value = mock_result
        
    mock_db_manager = Mock()
    mock_api_client = Mock()
    
    distribution_task = {
        "code": "DEMOGRAPHICS",
        "analysis": "DISTRIBUTION",
        "uuid": "task-uuid",
        "collection": "test_collection",
        "owner": "user1"
    }

    handle_task(distribution_task, mock_db_manager, mock_settings, mock_api_client)
    assert mock_solve.call_count == 1
    assert mock_api_client.send_results.call_count == 1

    # Second identical task - should use cache
    handle_task(distribution_task, mock_db_manager, mock_settings, mock_api_client)
    assert mock_solve.call_count == 1  # Still 1, used cache
    assert mock_api_client.send_results.call_count == 2


@patch('hutch_bunny.core.execute_query.DistributionCacheService.get')
@patch('hutch_bunny.core.services.cache_refresh_service.get_db_client')
@patch('hutch_bunny.core.execute_query.query_solvers.solve_distribution')
def test_background_refresh_updates_cache(
    mock_solve: Mock, 
    mock_get_db_client: Mock, 
    mock_cache_get: Mock, 
    mock_settings: Mock
) -> None:
    mock_cache_get.return_value = None
    mock_settings.COLLECTION_ID = "test_collection"

    results = [
        RquestResult(uuid="1", status="ok", collection_id="test", count=100),
        RquestResult(uuid="2", status="ok", collection_id="test", count=110),  
    ]
    mock_solve.side_effect = results * 10

    mock_settings.CACHE_TTL_HOURS = 0.0167  # 1 minute
        
    # Start cache service
    cache_service = CacheRefreshService(mock_settings)

    with patch('time.sleep'):  # Speed up test
        # Initial population
        cache_service._refresh_cache()
        initial_time = datetime.now()
        cache_service.last_refresh = initial_time

        with patch('hutch_bunny.core.services.cache_refresh_service.datetime') as mock_datetime:
            # First check - not time yet
            mock_datetime.now.return_value = initial_time + timedelta(seconds=30)
            cache_service.running = True

            should_refresh = mock_datetime.now() >= cache_service.last_refresh + timedelta(hours=mock_settings.CACHE_TTL_HOURS)
            assert not should_refresh

            mock_datetime.now.return_value = initial_time + timedelta(minutes=2)
            should_refresh = mock_datetime.now() >= cache_service.last_refresh + timedelta(hours=mock_settings.CACHE_TTL_HOURS)
            assert should_refresh

            cache_service._refresh_cache()

            assert mock_solve.call_count >= 2


@patch('hutch_bunny.core.services.cache_refresh_service.execute_query')
@patch('hutch_bunny.core.services.cache_refresh_service.get_db_client')
def test_system_stable_with_cache_errors(
    mock_get_db: Mock, 
    mock_execute: Mock, 
    mock_settings: Mock
) -> None:
    mock_execute.side_effect = Exception("Execution of query has failed")
    
    cache_service = CacheRefreshService(mock_settings)
    
    try:
        cache_service._refresh_cache()
    except Exception:
        pytest.fail("Cache refresh should handle errors gracefully")
    
    # Start service - should continue despite errors
    cache_service.start()
    assert cache_service.running is True
    
    mock_task_handler = Mock()
    mock_client = Mock()
    mock_client.get.return_value.status_code = 204
    
    polling_service = PollingService(mock_client, mock_task_handler, mock_settings)
    
    # Should work even with cache failures
    polling_thread = threading.Thread(
        target=lambda: polling_service.poll_for_tasks(max_iterations=3)
    )
    polling_thread.start()
    polling_thread.join(timeout=2)
    
    assert mock_client.get.call_count >= 3
    
    cache_service.stop()


def test_resource_cleanup(mock_settings: Mock) -> None:
    with patch('hutch_bunny.core.services.cache_refresh_service.get_db_client'):  
        with patch('hutch_bunny.core.services.cache_refresh_service.execute_query'): 
            with patch('time.sleep', return_value=None):
                cache_service = CacheRefreshService(mock_settings)
                cache_service.start()
                
                initial_thread = cache_service.thread
                assert initial_thread.is_alive()

                cache_service.stop()
                
                time.sleep(0.1)
                assert not initial_thread.is_alive()
                assert cache_service.running is False
