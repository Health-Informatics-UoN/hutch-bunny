import pytest
from unittest.mock import Mock, patch
from src.hutch_bunny.core.upstream.task_handler import handle_task
from src.hutch_bunny.core.rquest_dto.result import RquestResult


@pytest.fixture
def mock_db_manager():
    return Mock()


@pytest.fixture
def mock_settings():
    settings = Mock()
    settings.LOW_NUMBER_SUPPRESSION_THRESHOLD = 10
    settings.ROUNDING_TARGET = 2
    return settings


@pytest.fixture
def mock_task_api_client():
    return Mock()


@pytest.mark.unit
def test_handle_task_success(mock_db_manager, mock_settings, mock_task_api_client):
    # Arrange
    task_data = {"query": "SELECT * FROM table"}
    mock_result = RquestResult(
        status="success", uuid="1234", collection_id="5678", count=10
    )

    expected_result_modifier = [
        {"id": "Low Number Suppression", "threshold": 10},
        {"id": "Rounding", "nearest": 2},
    ]

    with patch(
        "src.hutch_bunny.core.upstream.task_handler.execute_query",
        return_value=mock_result,
    ) as mock_execute_query:
        # Act
        handle_task(task_data, mock_db_manager, mock_settings, mock_task_api_client)

        # Assert
        mock_execute_query.assert_called_once_with(
            task_data,
            expected_result_modifier,
            db_manager=mock_db_manager,
        )
        mock_task_api_client.send_results.assert_called_once_with(mock_result)


@pytest.mark.unit
def test_handle_task_not_implemented_error(
    mock_db_manager, mock_settings, mock_task_api_client
):
    # Arrange
    task_data = {"query": "SELECT * FROM table"}
    error_message = "Query type not supported"

    with patch(
        "src.hutch_bunny.core.upstream.task_handler.execute_query",
        side_effect=NotImplementedError(error_message),
    ) as mock_execute_query:
        # Act
        handle_task(task_data, mock_db_manager, mock_settings, mock_task_api_client)

        # Assert
        mock_execute_query.assert_called_once()
        mock_task_api_client.send_results.assert_not_called()


@pytest.mark.unit
def test_handle_task_value_error(mock_db_manager, mock_settings, mock_task_api_client):
    # Arrange
    task_data = {"query": "SELECT * FROM table"}
    error_message = "Invalid query parameters"

    with patch(
        "src.hutch_bunny.core.upstream.task_handler.execute_query",
        side_effect=ValueError(error_message),
    ) as mock_execute_query:
        # Act
        handle_task(task_data, mock_db_manager, mock_settings, mock_task_api_client)

        # Assert
        mock_execute_query.assert_called_once()
        mock_task_api_client.send_results.assert_not_called()


@pytest.mark.unit
def test_handle_task_unexpected_error(
    mock_db_manager, mock_settings, mock_task_api_client
):
    # Arrange
    task_data = {"query": "SELECT * FROM table"}
    error_message = "Database connection failed"

    with patch(
        "src.hutch_bunny.core.upstream.task_handler.execute_query",
        side_effect=Exception(error_message),
    ) as mock_execute_query:
        # Act
        handle_task(task_data, mock_db_manager, mock_settings, mock_task_api_client)

        # Assert
        mock_execute_query.assert_called_once()
        mock_task_api_client.send_results.assert_not_called()
