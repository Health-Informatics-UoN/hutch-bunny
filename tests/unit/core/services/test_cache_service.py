import pytest 
import tempfile 
from pathlib import Path 
from unittest.mock import Mock, patch 
from datetime import datetime, timedelta 

from hutch_bunny.core.services.cache_service import DistributionCacheService 

@pytest.fixture 
def mock_settings() -> Mock: 
    settings = Mock() 
    settings.CACHE_ENABLED = True 
    settings.CACHE_DIR = tempfile.mkdtemp()
    settings.CACHE_TTL_HOURS = 24 
    return settings 


def test_cache_service_initialisation(mock_settings: Mock) -> None: 
    service = DistributionCacheService(mock_settings)
    assert service.enabled == True
    assert Path(service.cache_dir).exists()


def test_cache_key_generation(mock_settings: Mock) -> None: 
    service = DistributionCacheService(mock_settings)
    query1 = {"code": "DEMOGRAPHICS", "analysis": "DISTRIBUTION"}
    query2 = {"code": "GENERIC", "analysis": "DISTRIBUTION"}
    
    key1 = service._generate_cache_key(query1, [])
    key2 = service._generate_cache_key(query2, [])
    
    assert key1 != key2
    assert len(key1) == len(key2) == 64  # SHA256 hex length


def test_cache_get_set(mock_settings: Mock) -> None: 
    service = DistributionCacheService(mock_settings)
    query = {"code": "DEMOGRAPHICS"}
    modifiers = []
    result = {"status": "ok", "count": 100}

    assert service.get(query, modifiers) is None

    service.set(query, modifiers, result)

    cached = service.get(query, modifiers)
    assert cached == result


def test_cache_ttl_expiration(mock_settings: Mock) -> None: 
    mock_settings.CACHE_TTL_HOURS = 1
    service = DistributionCacheService(mock_settings)
    
    query = {"code": "DEMOGRAPHICS"}
    result = {"status": "ok"}

    service.set(query, [], result)
    cache_path = service._get_cache_path(service._generate_cache_key(query, []))

    fake_stat = Mock(st_mtime=(datetime.now() - timedelta(hours=2)).timestamp())
    with patch("hutch_bunny.core.services.cache_service.Path.stat", return_value=fake_stat):
        assert service.get(query, []) is None


def test_cache_clear(mock_settings: Mock) -> None: 
    service = DistributionCacheService(mock_settings)
    query = {"code": "DEMOGRAPHICS"}
    modifiers = []
    result = {"status": "ok", "count": 100}
    
    service.set(query, modifiers, result)
    cached = service.get(query, modifiers)
    assert cached == result
    
    service.clear()
    assert service.get(query, modifiers) is None
