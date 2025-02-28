from hutch_bunny.core.logger import logger
import time
from typing import Callable
import requests
from hutch_bunny.core.settings import DaemonSettings
from hutch_bunny.core.upstream.task_api_client import TaskApiClient


class PollingService:
    """
    Polls the task API for tasks and processes them.
    """

    def __init__(
        self,
        client: TaskApiClient,
        task_handler: Callable,
        settings: DaemonSettings,
    ) -> None:
        """
        Initializes the PollingService.

        Args:
            client (TaskApiClient): The client to use to poll the task API.
            task_handler (Callable): The function to call to handle the task.
            settings (DaemonSettings): The settings to use to poll the task API.
            logger (Logger): The logger to use to log messages.

        Returns:
            None
        """
        self.client = client
        self.task_handler = task_handler
        self.settings = settings
        self.polling_endpoint = self._construct_polling_endpoint()

    def _construct_polling_endpoint(self) -> str:
        """
        Constructs the polling endpoint for the task API.

        Returns:
            str: The polling endpoint for the task API.
        """
        return (
            f"task/nextjob/{self.settings.COLLECTION_ID}.{self.settings.TASK_API_TYPE}"
            if self.settings.TASK_API_TYPE
            else f"task/nextjob/{self.settings.COLLECTION_ID}"
        )

    def poll_for_tasks(self, max_iterations: int | None = None) -> None:
        """
        Poll the API for tasks, handle the task, and continue to poll, until the maximum number of iterations is reached.

        If `max_iterations` is not provided, the polling will continue indefinitely.
        `max_iterations` is used in testing to limit the number of iterations.

        It handles network errors by catching
        requests.exceptions.RequestException. In the event of a network error,
        it will log the error and implement an exponential backoff strategy
        before retrying the request. The backoff time starts at
        `INITIAL_BACKOFF` and is capped at `MAX_BACKOFF`. These values are
        defined in the `DaemonSettings`.


        Args:
            max_iterations: Optional[int] = None: The maximum number of iterations to poll for tasks.

        Raises:
            requests.exceptions.RequestException: If a network error occurs.

        Returns:
            None
        """
        backoff_time = self.settings.INITIAL_BACKOFF
        max_backoff_time = self.settings.MAX_BACKOFF
        polling_interval = self.settings.POLLING_INTERVAL
        iteration = 0

        logger.info("Polling for tasks...")
        while True:
            if max_iterations is not None and iteration >= max_iterations:
                break
            try:
                response = self.client.get(endpoint=self.polling_endpoint)
                response.raise_for_status()

                if response.status_code == 200:
                    logger.info("Task received. Resolving...")
                    logger.debug(f"Task: {response.json()}")
                    task_data = response.json()
                    self.task_handler(task_data)

                elif response.status_code == 204:
                    logger.debug("No task found. Looking for task...")
                else:
                    logger.info(f"Got http status code: {response.status_code}")

                backoff_time = self.settings.INITIAL_BACKOFF
            except requests.exceptions.RequestException as e:
                logger.error(f"Network error occurred: {e}")
                # Exponential backoff
                time.sleep(backoff_time)
                backoff_time = min(backoff_time * 2, max_backoff_time)

            time.sleep(polling_interval)
            iteration += 1
