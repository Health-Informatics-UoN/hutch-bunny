import pytest
from unittest.mock import patch, Mock
from requests.models import Response
from requests.exceptions import RequestException
from src.hutch_bunny.core.upstream.task_api_client import TaskApiClient, SupportedMethod
from requests.auth import HTTPBasicAuth


@pytest.fixture
def mock_settings():
    mock_settings = Mock()
    mock_settings.TASK_API_BASE_URL = "https://example.com"
    mock_settings.TASK_API_USERNAME = "user"
    mock_settings.TASK_API_PASSWORD = "password"
    mock_settings.TASK_API_ENFORCE_HTTPS = True
    return mock_settings


@pytest.fixture
def task_api_client(mock_settings):
    return TaskApiClient(mock_settings)


@pytest.mark.unit
@patch("src.hutch_bunny.core.upstream.task_api_client.requests.request")
def test_request_success(mock_request, mock_settings, task_api_client):
    """
    Verifies that a request is made successfully.
    """
    # Arrange
    mock_response = Mock(spec=Response)
    mock_response.status_code = 200
    mock_response.text = "Success"
    mock_request.return_value = mock_response

    # Act
    response = task_api_client._request(SupportedMethod.GET, "https://example.com/test")

    # Assert
    mock_request.assert_called_once_with(
        method="get",
        url="https://example.com/test",
        json=None,
        auth=HTTPBasicAuth(
            mock_settings.TASK_API_USERNAME, mock_settings.TASK_API_PASSWORD
        ),
        headers=None,
    )
    assert response.status_code == 200
    assert response.text == "Success"


@pytest.mark.unit
@patch("src.hutch_bunny.core.upstream.task_api_client.requests.request")
def test_post_request(mock_request, mock_settings, task_api_client):
    """
    Verifies that a POST request is made successfully.
    """
    # Arrange
    mock_response = Mock(spec=Response)
    mock_response.status_code = 201
    mock_request.return_value = mock_response

    # Act
    response = task_api_client.post(endpoint="test", data={"key": "value"})

    # Assert
    mock_request.assert_called_once_with(
        method="post",
        url=f"{mock_settings.TASK_API_BASE_URL}/test",
        json={"key": "value"},
        auth=HTTPBasicAuth(
            mock_settings.TASK_API_USERNAME, mock_settings.TASK_API_PASSWORD
        ),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 201


@pytest.mark.unit
@patch("src.hutch_bunny.core.upstream.task_api_client.requests.request")
def test_get_request(mock_request, mock_settings, task_api_client):
    """
    Verifies that a GET request is made successfully.
    """
    # Arrange
    mock_response = Mock(spec=Response)
    mock_response.status_code = 200
    mock_request.return_value = mock_response
    # Act
    response = task_api_client.get(endpoint="test")

    # Assert
    mock_request.assert_called_once_with(
        method="get",
        url=f"{mock_settings.TASK_API_BASE_URL}/test",
        json=None,
        auth=HTTPBasicAuth(
            mock_settings.TASK_API_USERNAME, mock_settings.TASK_API_PASSWORD
        ),
        headers=None,
    )
    assert response.status_code == 200


@pytest.mark.unit
@patch("src.hutch_bunny.core.upstream.task_api_client.requests.request")
def test_send_results(mock_request, mock_settings, task_api_client):
    """
    Verifies that a POST request is made successfully.
    """
    # Arrange
    mock_response = Mock(spec=Response)
    mock_response.status_code = 200
    mock_request.return_value = mock_response
    mock_result = Mock()
    mock_result.uuid = "1234"
    mock_result.collection_id = "5678"
    mock_result.to_dict.return_value = {"key": "value"}

    # Act
    task_api_client.send_results(mock_result)

    # Assert
    mock_request.assert_called_with(
        method="post",
        url=f"{mock_settings.TASK_API_BASE_URL}/task/result/1234/5678",
        json={"key": "value"},
        auth=HTTPBasicAuth(
            mock_settings.TASK_API_USERNAME, mock_settings.TASK_API_PASSWORD
        ),
        headers={"Content-Type": "application/json"},
    )


@pytest.mark.unit
@patch("src.hutch_bunny.core.upstream.task_api_client.requests.request")
def test_request_network_error(mock_request, mock_settings, task_api_client):
    """
    Verifies that a network error is raised when the request fails.
    """
    # Arrange
    mock_request.side_effect = RequestException("Network error")

    # Act & Assert
    with pytest.raises(RequestException, match="Network error"):
        task_api_client._request(
            SupportedMethod.GET, f"{mock_settings.TASK_API_BASE_URL}/test"
        )


@pytest.mark.unit
@patch("src.hutch_bunny.core.upstream.task_api_client.requests.request")
def test_request_unauthorized(mock_request, mock_settings, task_api_client):
    """
    Verifies that a 401 error is returned when the request is unauthorized.
    """
    # Arrange
    mock_response = Mock(spec=Response)
    mock_response.status_code = 401
    mock_request.return_value = mock_response

    # Act
    response = task_api_client._request(
        SupportedMethod.GET, f"{mock_settings.TASK_API_BASE_URL}/test"
    )

    # Assert
    assert response.status_code == 401


@pytest.mark.unit
@patch("src.hutch_bunny.core.upstream.task_api_client.requests.request")
def test_send_results_retry_logic(mock_request, mock_settings, task_api_client):
    """
    Verifies that the send_results method retries the request when it fails.
    """
    # Arrange
    mock_response = Mock(spec=Response)
    mock_response.status_code = 500
    mock_request.return_value = mock_response
    mock_result = Mock()
    mock_result.uuid = "1234"
    mock_result.collection_id = "5678"
    mock_result.to_dict.return_value = {"key": "value"}

    # Act
    task_api_client.send_results(mock_result, retry_count=4, retry_delay=0.1)

    # Assert
    assert mock_request.call_count == 4


@pytest.mark.unit
@patch("src.hutch_bunny.core.upstream.task_api_client.requests.request")
@patch("src.hutch_bunny.core.upstream.task_api_client.logger")
def test_send_results_network_error(
    mock_logger, mock_request, mock_settings, task_api_client
):
    """
    Verifies that a network error is logged when the request fails.
    """
    # Arrange
    mock_request.side_effect = RequestException("Network error")
    mock_result = Mock()
    mock_result.uuid = "1234"
    mock_result.collection_id = "5678"
    mock_result.to_dict.return_value = {"key": "value"}

    # Act
    task_api_client.send_results(mock_result, retry_count=4, retry_delay=0.1)

    # Assert
    assert mock_request.call_count == 4
    mock_logger.error.assert_called_with(
        "Network error occurred while posting results: Network error"
    )
