import time
import requests
from hutch_bunny.core.execute_query import execute_query
from hutch_bunny.core.rquest_dto.result import RquestResult
import hutch_bunny.core.settings as settings


class PollingService:
    def __init__(self, client, logger, polling_endpoint: str, response_handler):
        self.client = client
        self.logger = logger
        self.polling_endpoint = polling_endpoint
        self.response_handler = response_handler

    def poll_for_jobs(self):
        backoff_time = settings.INITIAL_BACKOFF
        max_backoff_time = settings.MAX_BACKOFF
        polling_interval = settings.POLLING_INTERVAL

        while True:
            try:
                response = self.client.get(endpoint=self.polling_endpoint)
                self.response_handler(response)
                
                # Reset backoff time on success
                backoff_time = settings.INITIAL_BACKOFF  
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Network error occurred: {e}")
                time.sleep(backoff_time)
                # Exponential backoff
                backoff_time = min(backoff_time * 2, max_backoff_time) 
            
            time.sleep(polling_interval)

    def handle_response(self, response, db_manager, result_modifier):
        if response.status_code == 200:
            self.logger.info("Job received. Resolving...")
            self.logger.debug("JSON Response: %s", response.json())
            query_dict: dict = response.json()
            result = execute_query(
                query_dict,
                result_modifier,
                logger=self.logger,
                db_manager=db_manager,
            )
            self.logger.debug(f"Result: {result.to_dict()}")
            if not isinstance(result, RquestResult):
                raise TypeError("Payload does not match RQuest result schema.")
            self.send_results(result)
        elif response.status_code == 204:
            self.logger.info("Looking for job...")
        elif response.status_code == 401:
            self.logger.info("Failed to authenticate with task server.")
        else:
            self.logger.info("Got http status code: %s", response.status_code)

    def send_results(self, result):
        return_endpoint = f"task/result/{result.uuid}/{result.collection_id}"
        self.logger.debug("Return endpoint: %s", return_endpoint)
        for _ in range(4):
            try:
                response = self.client.post(endpoint=return_endpoint, data=result.to_dict())
                if 200 <= response.status_code < 300 or 400 <= response.status_code < 500:
                    self.logger.info("Job resolved.")
                    self.logger.debug("Response status: %s", response.status_code)
                    self.logger.debug("Response: %s", response.text)
                    break
                else:
                    self.logger.warning(
                        f"Bunny failed to post to {return_endpoint} at {time.time()}. Trying again..."
                    )
                    time.sleep(5)
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Network error occurred while posting results: {e}")
                time.sleep(5)
