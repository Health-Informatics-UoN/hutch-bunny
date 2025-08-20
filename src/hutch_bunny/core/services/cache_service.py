import os
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from hutch_bunny.core.logger import logger
from hutch_bunny.core.settings import Settings


class DistributionCacheService: 
    """Service for caching distribution query results."""

    def __init__(self, settings: Settings):
        self.settings = settings 
        self.cache_dir = Path(settings.CACHE_DIR) 
        self.enabled = settings.CACHE_ENABLED 
        self.ttl_hours = settings.CACHE_TTL_HOURS 

        if self.enabled: 
            self._ensure_cache_dir() 
    
    def _ensure_cache_dir(self) -> None: 
        """Create cache directory if it doesn't exist."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _generate_cache_key(self, query_dict: dict, modifiers: list) -> str: 
        """Generate a unique cache key for the query."""
        # Create a deterministic hash from a query and modifiers 
        cache_data = {
            "query": query_dict, 
            "modifiers": modifiers 
        }
        cache_str = json.dumps(cache_data, sort_keys=True)
        return hashlib.sha256(cache_str.encode()).hexdigest()
    
    def _get_cache_path(self, cache_key: str) -> Path: 
        """Get the file path for a cache key."""
        return self.cache_dir / f"{cache_key}.json"

    def _is_cache_valid(self, cache_path: Path) -> bool: 
        """Check if cache file exists and is still valid."""
        if not cache_path.exists():
            return False
        
        if self.ttl_hours == 0:  # No expiration
            return True
        
        # Check cache time-to-live TTL
        file_time = datetime.fromtimestamp(cache_path.stat().st_mtime)
        expiry_time = file_time + timedelta(hours=self.ttl_hours)
        return datetime.now() < expiry_time
    
    def get(self, query_dict: dict[str, str], modifiers: list) -> Optional[dict]: 
        """Retrieve cached result if available and valid."""
        if not self.enabled: 
            return None 
        
        cache_key = self._generate_cache_key(query_dict, modifiers)
        cache_path = self._get_cache_path(cache_key)

        if self._is_cache_valid(cache_path):
            try:
                with open(cache_path, 'r') as f:
                    cached_data = json.load(f)
                logger.info(f"Cache hit for distribution query: {cache_key}")
                return cached_data
            except Exception as e:
                logger.error(f"Error reading cache: {e}")
                return None
        
        return None 
    
    def set(self, query_dict: dict[str, str], modifiers: list, result: dict) -> None: 
        """Store result in cache."""
        if not self.enabled:
            return
        
        cache_key = self._generate_cache_key(query_dict, modifiers)
        cache_path = self._get_cache_path(cache_key)

        try:
            with open(cache_path, 'w') as f:
                json.dump(result, f)
            logger.info(f"Cached distribution query result: {cache_key}")
        except Exception as e:
            logger.error(f"Error writing cache: {e}")

    def clear(self) -> None:
        """Clear all cached results."""
        if not self.enabled:
            return
        
        for cache_file in self.cache_dir.glob("*.json"):
            try:
                cache_file.unlink()
            except Exception as e:
                logger.error(f"Error deleting cache file {cache_file}: {e}")
        
        logger.info("Cache cleared")

