import pytest
from unittest.mock import Mock, patch
from hutch_bunny.core.upstream.polling_service import PollingService
from hutch_bunny.core.upstream.task_api_client import TaskApiClient
from logging import Logger
import requests


@pytest.fixture
def mock_settings():
    mock_settings = Mock()
    mock_settings.COLLECTION_ID = "test_collection"
    mock_settings.TASK_API_TYPE = "test_type"
    mock_settings.INITIAL_BACKOFF = 1
    mock_settings.MAX_BACKOFF = 8
    mock_settings.POLLING_INTERVAL = 0.1
    return mock_settings


@pytest.fixture
def mock_client():
    return Mock(spec=TaskApiClient)


@pytest.fixture
def mock_logger():
    with patch("hutch_bunny.core.upstream.polling_service.logger") as mock_logger:
        yield mock_logger


@pytest.fixture
def mock_task_handler():
    return Mock()


def test_poll_for_tasks_success(
    mock_logger, mock_settings, mock_client, mock_task_handler
):
    # Arrange
    mock_client.get.return_value.status_code = 200
    mock_client.get.return_value.json.return_value = {"task": "data"}

    polling_service = PollingService(mock_client, mock_task_handler, mock_settings)

    # Act
    with patch("time.sleep", return_value=None):  # To speed up the test
        polling_service.poll_for_tasks(max_iterations=1)

    # Assert
    mock_logger.info.assert_called_with("Task received. Resolving...")
    mock_task_handler.assert_called_once_with({"task": "data"})


def test_poll_for_tasks_no_task(
    mock_logger, mock_settings, mock_client, mock_task_handler
):
    # Arrange
    mock_client.get.return_value.status_code = 204

    polling_service = PollingService(mock_client, mock_task_handler, mock_settings)

    # Act
    with patch("time.sleep", return_value=None):  # To speed up the test
        polling_service.poll_for_tasks(max_iterations=1)

    # Assert
    mock_logger.debug.assert_called_with("No task found. Looking for task...")
    mock_task_handler.assert_not_called()


def test_construct_polling_endpoint_with_type(
    mock_settings, mock_client, mock_task_handler
):
    # Arrange
    polling_service = PollingService(mock_client, mock_task_handler, mock_settings)

    # Act
    endpoint = polling_service._construct_polling_endpoint()

    # Assert
    assert endpoint == "task/nextjob/test_collection.test_type"


def test_construct_polling_endpoint_without_type(
    mock_settings, mock_client, mock_task_handler
):
    # Arrange
    mock_settings = Mock()
    mock_settings.COLLECTION_ID = "test_collection"
    mock_settings.TASK_API_TYPE = None

    polling_service = PollingService(mock_client, mock_task_handler, mock_settings)

    # Act
    endpoint = polling_service._construct_polling_endpoint()

    # Assert
    assert endpoint == "task/nextjob/test_collection"


@patch("time.sleep", return_value=None)
def test_unauthorized_status_code(
    mock_sleep, mock_logger, mock_settings, mock_client, mock_task_handler
):
    # Arrange
    mock_client.get.return_value.status_code = 401
    mock_client.get.return_value.raise_for_status.side_effect = (
        requests.exceptions.RequestException()
    )

    polling_service = PollingService(mock_client, mock_task_handler, mock_settings)

    # Act
    polling_service.poll_for_tasks(max_iterations=1)

    # Assert
    mock_logger.error.assert_called_with("Network error occurred: ")


@patch("time.sleep", return_value=None)
def test_other_status_code(
    mock_sleep, mock_logger, mock_settings, mock_client, mock_task_handler
):
    # Arrange
    mock_client.get.return_value.status_code = 500
    polling_service = PollingService(mock_client, mock_task_handler, mock_settings)

    # Act
    polling_service.poll_for_tasks(max_iterations=1)

    # Assert
    mock_logger.info.assert_called_with("Got http status code: 500")


@patch("time.sleep", return_value=None)
def test_network_error(
    mock_sleep, mock_logger, mock_settings, mock_client, mock_task_handler
):
    # Arrange
    mock_client.get.side_effect = requests.exceptions.RequestException("Network error")
    polling_service = PollingService(mock_client, mock_task_handler, mock_settings)

    # Act
    polling_service.poll_for_tasks(max_iterations=1)

    # Assert
    mock_logger.error.assert_called_with("Network error occurred: Network error")
