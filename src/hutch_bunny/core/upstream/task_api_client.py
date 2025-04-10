from hutch_bunny.core.logger import logger
import time
from requests.models import Response
from enum import Enum
import requests
from requests.auth import HTTPBasicAuth
from hutch_bunny.core.rquest_dto.result import RquestResult
from hutch_bunny.core.settings import DaemonSettings
from typing import Optional


class SupportedMethod(Enum):
    POST = "post"
    GET = "get"
    PUT = "put"
    PATCH = "patch"
    DELETE = "delete"


class TaskApiClient:
    def __init__(
        self,
        settings: DaemonSettings,
    ):
        self.base_url = settings.TASK_API_BASE_URL
        self.username = settings.TASK_API_USERNAME
        self.password = settings.TASK_API_PASSWORD
        self.enforce_https = settings.TASK_API_ENFORCE_HTTPS

    def _request(
        self,
        method: SupportedMethod,
        url: str,
        data: Optional[dict[str, object]] = None,
        headers: Optional[dict[str, str | bytes | None]] = None,
    ) -> Response:
        """
        Sends an HTTP request using the specified method to the given URL with optional data and additional parameters.

        Args:
            method (SupportedMethod): The HTTP method to use for the request. Must be one of the SupportedMethod enum values.
            url (str): The URL to which the request is sent.
            data (dict, optional): The data to send in the body of the request. Defaults to None.
            headers (dict, optional): The headers to send in the request. Defaults to None.

        Returns:
            Response: The response object returned by the requests library.
        """
        logger.debug(
            "Sending %s request to %s with data %s and headers %s"
            % (method.value, url, data, headers)
        )
        basicAuth = HTTPBasicAuth(self.username, self.password)
        response = requests.request(
            method=method.value,
            url=url,
            json=data,
            auth=basicAuth,
            headers=headers,
        )
        logger.debug("Response Status: %s", response.status_code)
        logger.debug("Response Text: %s", response.text)
        return response

    def post(
        self,
        endpoint: Optional[str] = None,
        data: dict[str, object] = dict(),
    ) -> Response:
        """
        Sends a POST request to the specified endpoint with data and additional parameters.

        Args:
            endpoint (str): The endpoint to which the POST request is sent.
            data (dict): The data to send in the body of the request.

        Returns:
            Response: The response object returned by the requests library.
        """
        url = f"{self.base_url}/{endpoint}"
        return self._request(
            SupportedMethod.POST,
            url,
            data,
            headers={"Content-Type": "application/json"},
        )

    def get(self, endpoint: Optional[str] = None) -> Response:
        """
        Sends a GET request to the specified endpoint with optional additional parameters.

        Args:
            endpoint (str): The endpoint to which the GET request is sent.

        Returns:
            Response: The response object returned by the requests library.
        """
        url = f"{self.base_url}/{endpoint}"
        return self._request(SupportedMethod.GET, url)

    def send_results(
        self, result: RquestResult, retry_count: int = 4, retry_delay: int = 5
    ) -> None:
        """
        Sends a POST request to the specified endpoint with data and additional parameters.

        Args:
            result (RquestResult): The result object containing data to send.
            retry_count (int): The number of times to retry the request. Defaults to 4.
            retry_delay (int): The delay between retries in seconds. Defaults to 5.
        """
        return_endpoint = f"task/result/{result.uuid}/{result.collection_id}"
        for _ in range(retry_count):
            try:
                response = self.post(endpoint=return_endpoint, data=result.to_dict())
                if (
                    200 <= response.status_code < 300
                    or 400 <= response.status_code < 500
                ):
                    logger.info("Task resolved.")
                    logger.debug(f"Response status: {response.status_code}")
                    logger.debug(f"Response: {response.text}")
                    break
                else:
                    logger.warning(
                        f"Failed to post to {return_endpoint} at {time.time()}. Trying again..."
                    )
                    time.sleep(retry_delay)
            except requests.exceptions.RequestException as e:
                logger.error(f"Network error occurred while posting results: {e}")
                time.sleep(retry_delay)
