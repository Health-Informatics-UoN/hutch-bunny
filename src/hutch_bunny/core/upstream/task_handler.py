from hutch_bunny.core.db_manager import BaseDBManager
from hutch_bunny.core.settings import DaemonSettings
from hutch_bunny.core.execute_query import execute_query
from hutch_bunny.core.upstream.task_api_client import TaskApiClient
from hutch_bunny.core.results_modifiers import results_modifiers


def handle_task(
    task_data: dict,
    db_manager: BaseDBManager,
    settings: DaemonSettings,
    task_api_client: TaskApiClient,
) -> None:
    """
    Handles a task by executing a query and sending the results to the task API.

    Args:
        task_data (dict): The task data to execute the query on.
        db_manager (BaseDBManager): The database manager to use to execute the query.
        settings (DaemonSettings): The settings to use to execute the query.
        task_api_client (TaskApiClient): The task API client to use to send the results.

    Returns:
        None
    """
    result_modifier: list[dict] = results_modifiers(
        low_number_suppression_threshold=int(
            settings.LOW_NUMBER_SUPPRESSION_THRESHOLD or 0
        ),
        rounding_target=int(settings.ROUNDING_TARGET or 0),
    )
    result = execute_query(
        task_data,
        result_modifier,
        db_manager=db_manager,
    )
    task_api_client.send_results(result)
