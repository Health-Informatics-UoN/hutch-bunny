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
        query_result = cached_data["queryResult"]
        assert query_result["count"] == 100  

    


    


