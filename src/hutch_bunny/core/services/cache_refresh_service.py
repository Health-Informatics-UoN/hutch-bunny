import threading
import time
from datetime import datetime, timedelta
from typing import Optional
from hutch_bunny.core.logger import logger
from hutch_bunny.core.settings import DaemonSettings
from hutch_bunny.core.db import get_db_client
from hutch_bunny.core.execute_query import execute_query
from hutch_bunny.core.results_modifiers import results_modifiers


class CacheRefreshService: 
    """Service to periodically refresh distribution cache."""

    def __init__(self, settings: DaemonSettings): 
        self.settings = settings
        self.running = False 
        self.thread: Optional[threading.Thread] = None 
        self.last_refresh = None 

    def start(self) -> None: 
        """Start the cache refresh background thread."""
        if not self.settings.CACHE_ENABLED:
            logger.info("Cache disabled, not starting refresh service")
            return
            
        if self.settings.CACHE_TTL_HOURS <= 0:
            logger.info("Cache TTL is 0 (no expiration), not starting refresh service")
            return
        
        if self.settings.CACHE_REFRESH_ON_STARTUP:
            logger.info("Populating cache on startup...")
            try:
                self._refresh_cache()
                self.last_refresh = datetime.now()
                logger.info("Initial cache population completed")
            except Exception as e:
                logger.error(f"Failed to populate cache on startup: {e}")
                # Continue anyway - don't fail startup due to cache issues

        self.running = True
        self.thread = threading.Thread(target=self._refresh_loop, daemon=True)
        self.thread.start()
        logger.info(f"Cache refresh service started (interval: {self.settings.CACHE_TTL_HOURS} hours)")
    
    def stop(self) -> None:
        """Stop the cache refresh service."""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)

    def _refresh_loop(self) -> None: 
        """Main loop that refreshes the cache at intervals."""
        while self.running: 
            try:
                if self.last_refresh is None:
                    self._refresh_cache()
                    self.last_refresh = datetime.now()
                    continue

                next_refresh = self.last_refresh + timedelta(hours=self.settings.CACHE_TTL_HOURS)

                if datetime.now() >= next_refresh:
                    logger.info(f"{datetime.now() - self.last_refresh} elapsed since last refresh: Starting scheduled cache refresh")
                    self._refresh_cache()
                    self.last_refresh = datetime.now()
                    logger.info("Cache refresh completed")
                
                # Sleep for a minute before checking again
                time.sleep(60)
                
            except Exception as e:
                logger.error(f"Error in cache refresh loop: {e}", exc_info=True)
                time.sleep(300)  

    def _refresh_cache(self) -> None: 
        """Refresh all common distribution queries."""
        db_client = get_db_client()

        queries = [
            {
                "code": "DEMOGRAPHICS",
                "analysis": "DISTRIBUTION", 
                "uuid": "cache_refresh",
                "collection": self.settings.COLLECTION_ID,
                "owner": "system"
            },
            {
                "code": "GENERIC",
                "analysis": "DISTRIBUTION",
                "uuid": "cache_refresh", 
                "collection": self.settings.COLLECTION_ID,
                "owner": "system"
            }
        ]

        modifier_sets = [
            results_modifiers(
                low_number_suppression_threshold=self.settings.LOW_NUMBER_SUPPRESSION_THRESHOLD,
                rounding_target=self.settings.ROUNDING_TARGET
            )
        ]

        for query in queries:
            for modifiers in modifier_sets:
                try:
                    logger.debug(f"Refreshing {query['code']} with modifiers {modifiers}")
                    execute_query(
                        query, 
                        modifiers, 
                        db_client=db_client, 
                        settings=self.settings
                    )
                except Exception as e:
                    logger.error(f"Failed to refresh cache for {query['code']}: {e}")
