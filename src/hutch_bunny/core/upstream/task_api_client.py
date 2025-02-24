from logging import Logger
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
        logger: Logger,
    ):
        self.base_url = settings.TASK_API_BASE_URL
        self.username = settings.TASK_API_USERNAME
        self.password = settings.TASK_API_PASSWORD
        self.logger = logger

    def request(
        self, method: SupportedMethod, url: str, data: Optional[dict] = None, **kwargs
    ) -> Response:
        """
        Sends an HTTP request using the specified method to the given URL with optional data and additional parameters.

        Args:
            method (SupportedMethod): The HTTP method to use for the request. Must be one of the SupportedMethod enum values.
            url (str): The URL to which the request is sent.
            data (dict, optional): The data to send in the body of the request. Defaults to None.
            **kwargs: Additional keyword arguments to pass to the requests method. This can include parameters such as headers, params, verify, etc.

        Returns:
            Response: The response object returned by the requests library.
        """
        self.logger.debug(
            "Sending %s request to %s with data %s and kwargs %s"
            % (method.value, url, data, kwargs)
        )
        basicAuth = HTTPBasicAuth(self.username, self.password)
        response = requests.request(
            method=method.value, url=url, json=data, auth=basicAuth, **kwargs
        )
        self.logger.debug("Response Status: %s", response.status_code)
        self.logger.debug("Response Text: %s", response.text)
        return response

    def post(
        self, endpoint: Optional[str] = None, data: dict = dict(), **kwargs
    ) -> Response:
        """
        Sends a POST request to the specified endpoint with data and additional parameters.

        Args:
            endpoint (str): The endpoint to which the POST request is sent.
            data (dict): The data to send in the body of the request.
            **kwargs: Additional keyword arguments to pass to the requests method.

        Returns:
            Response: The response object returned by the requests library.
        """
        url = f"{self.base_url}/{endpoint}"
        return self.request(
            SupportedMethod.POST,
            url,
            data,
            headers={"Content-Type": "application/json"},
        )

    def get(self, endpoint: Optional[str] = None, **kwargs) -> Response:
        """
        Sends a GET request to the specified endpoint with optional additional parameters.

        Args:
            endpoint (str): The endpoint to which the GET request is sent.
            **kwargs: Additional keyword arguments to pass to the requests method. This can include parameters such as headers, params, verify, etc.

        Returns:
            Response: The response object returned by the requests library.
        """
        url = f"{self.base_url}/{endpoint}"
        return self.request(SupportedMethod.GET, url, **kwargs)

    def send_results(self, result: RquestResult):
        """
        Sends a POST request to the specified endpoint with data and additional parameters.

        Args:
            endpoint (str): The endpoint to which the POST request is sent.
            data (dict): The data to send in the body of the request.
        """
        return_endpoint = f"task/result/{result.uuid}/{result.collection_id}"
        for _ in range(4):
            try:
                response = self.post(endpoint=return_endpoint, data=result.to_dict())
                if (
                    200 <= response.status_code < 300
                    or 400 <= response.status_code < 500
                ):
                    self.logger.info("Task resolved.")
                    self.logger.debug(f"Response status: {response.status_code}")
                    self.logger.debug(f"Response: {response.text}")
                    break
                else:
                    self.logger.warning(
                        f"Failed to post to {return_endpoint} at {time.time()}. Trying again..."
                    )
                    time.sleep(5)
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Network error occurred while posting results: {e}")
                time.sleep(5)
