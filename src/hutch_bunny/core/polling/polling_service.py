from logging import Logger
import time
from typing import Callable
import requests
from hutch_bunny.core.settings import get_settings, DaemonSettings
from hutch_bunny.core.polling.task_api_client import TaskApiClient

settings: DaemonSettings = get_settings(daemon=True)


class PollingService:
    """
    Polls the task API for tasks and processes them.
    """

    def __init__(
        self, client: TaskApiClient, logger: Logger, task_handler: Callable
    ) -> None:
        """
        Initializes the PollingService.

        Args:
            client (TaskApiClient): The client to use to poll the task API.
            logger (Logger): The logger to use to log messages.
            task_handler (Callable): The function to call to handle the task.

        Returns:
            None
        """
        self.client = client
        self.logger = logger
        self.task_handler = task_handler
        self.polling_endpoint = self._construct_polling_endpoint()

    def _construct_polling_endpoint(self) -> str:
        """
        Constructs the polling endpoint for the task API.

        Returns:
            str: The polling endpoint for the task API.
        """
        return (
            f"task/nextjob/{settings.COLLECTION_ID}.{settings.TASK_API_TYPE}"
            if settings.TASK_API_TYPE
            else f"task/nextjob/{settings.COLLECTION_ID}"
        )

    def poll_for_tasks(self, max_iterations=None):
        """
        Polls the task API for tasks and processes them.

        Returns:
            None
        """
        backoff_time = settings.INITIAL_BACKOFF
        max_backoff_time = settings.MAX_BACKOFF
        polling_interval = settings.POLLING_INTERVAL
        iteration = 0

        self.logger.info("Polling for tasks...")
        while True:
            if max_iterations is not None and iteration >= max_iterations:
                break
            try:
                response = self.client.get(endpoint=self.polling_endpoint)
                if response.status_code == 200:
                    self.logger.info("Task received. Resolving...")
                    self.logger.debug(f"Task: {response.json()}")
                    task_data = response.json()
                    self.task_handler(task_data)

                    backoff_time = settings.INITIAL_BACKOFF
                elif response.status_code == 204:
                    self.logger.debug("No task found. Looking for job...")
                elif response.status_code == 401:
                    self.logger.info("Failed to authenticate with task server.")
                else:
                    self.logger.info(f"Got http status code: {response.status_code}")
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Network error occurred: {e}")

                # Exponential backoff
                time.sleep(backoff_time)
                backoff_time = min(backoff_time * 2, max_backoff_time)

            time.sleep(polling_interval)
            iteration += 1
