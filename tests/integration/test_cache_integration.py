import os
import pytest
import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from hutch_bunny.core.execute_query import execute_query
from hutch_bunny.core.services.cache_service import DistributionCacheService
from hutch_bunny.core.rquest_models.result import RquestResult


@pytest.fixture
def mock_settings(tmp_path: Path) -> Mock:
    settings = Mock()
    settings.CACHE_ENABLED = True
    settings.CACHE_DIR = str(tmp_path)
    settings.CACHE_TTL_HOURS = 24
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




