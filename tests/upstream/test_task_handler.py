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
def mock_logger():
    return Mock()


@pytest.fixture
def mock_task_api_client():
    return Mock()


def test_handle_task_success(
    mock_db_manager, mock_settings, mock_logger, mock_task_api_client
):
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
        handle_task(
            task_data, mock_db_manager, mock_settings, mock_logger, mock_task_api_client
        )

        # Assert
        mock_execute_query.assert_called_once_with(
            task_data,
            expected_result_modifier,
            logger=mock_logger,
            db_manager=mock_db_manager,
        )
        mock_task_api_client.send_results.assert_called_once_with(mock_result)
