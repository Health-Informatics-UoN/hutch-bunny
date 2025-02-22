import pytest
from unittest.mock import patch, Mock
from requests.models import Response
from requests.exceptions import RequestException
from src.hutch_bunny.core.upstream.task_api_client import TaskApiClient, SupportedMethod
from requests.auth import HTTPBasicAuth


@pytest.fixture
def task_api_client():
    return TaskApiClient(
        base_url="http://example.com", username="user", password="password"
    )


@patch("src.hutch_bunny.core.upstream.task_api_client.requests.request")
def test_request_success(mock_request, task_api_client):
    # Arrange
    mock_response = Mock(spec=Response)
    mock_response.status_code = 200
    mock_response.text = "Success"
    mock_request.return_value = mock_response

    # Act
    response = task_api_client.request(SupportedMethod.GET, "http://example.com/test")

    # Assert
    mock_request.assert_called_once_with(
        method="get",
        url="http://example.com/test",
        json=None,
        auth=HTTPBasicAuth(task_api_client.username, task_api_client.password),
    )
    assert response.status_code == 200
    assert response.text == "Success"


@patch("src.hutch_bunny.core.upstream.task_api_client.requests.request")
def test_post_request(mock_request, task_api_client):
    # Arrange
    mock_response = Mock(spec=Response)
    mock_response.status_code = 201
    mock_request.return_value = mock_response

    # Act
    response = task_api_client.post(endpoint="test", data={"key": "value"})

    # Assert
    mock_request.assert_called_once_with(
        method="post",
        url="http://example.com/test",
        json={"key": "value"},
        auth=HTTPBasicAuth(task_api_client.username, task_api_client.password),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 201


@patch("src.hutch_bunny.core.upstream.task_api_client.requests.request")
def test_get_request(mock_request, task_api_client):
    # Arrange
    mock_response = Mock(spec=Response)
    mock_response.status_code = 200
    mock_request.return_value = mock_response

    # Act
    response = task_api_client.get(endpoint="test")

    # Assert
    mock_request.assert_called_once_with(
        method="get",
        url="http://example.com/test",
        json=None,
        auth=HTTPBasicAuth(task_api_client.username, task_api_client.password),
    )
    assert response.status_code == 200


@patch("src.hutch_bunny.core.upstream.task_api_client.requests.request")
def test_send_results(mock_request, task_api_client):
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
        url="http://example.com/task/result/1234/5678",
        json={"key": "value"},
        auth=HTTPBasicAuth(task_api_client.username, task_api_client.password),
        headers={"Content-Type": "application/json"},
    )


@patch("src.hutch_bunny.core.upstream.task_api_client.requests.request")
def test_request_network_error(mock_request, task_api_client):
    # Arrange
    mock_request.side_effect = RequestException("Network error")

    # Act & Assert
    with pytest.raises(RequestException, match="Network error"):
        task_api_client.request(SupportedMethod.GET, "http://example.com/test")


@patch("src.hutch_bunny.core.upstream.task_api_client.requests.request")
def test_request_unauthorized(mock_request, task_api_client):
    # Arrange
    mock_response = Mock(spec=Response)
    mock_response.status_code = 401
    mock_request.return_value = mock_response

    # Act
    response = task_api_client.request(SupportedMethod.GET, "http://example.com/test")

    # Assert
    assert response.status_code == 401


@patch("src.hutch_bunny.core.upstream.task_api_client.requests.request")
def test_send_results_retry_logic(mock_request, task_api_client):
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


@patch("src.hutch_bunny.core.upstream.task_api_client.requests.request")
@patch("src.hutch_bunny.core.upstream.task_api_client.logger")
def test_send_results_network_error(mock_logger, mock_request, task_api_client):
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