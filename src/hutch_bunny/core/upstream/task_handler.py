from hutch_bunny.core.db_manager import SyncDBManager
from hutch_bunny.core.settings import DaemonSettings
from hutch_bunny.core.execute_query import execute_query
from hutch_bunny.core.upstream.task_api_client import TaskApiClient
from hutch_bunny.core.results_modifiers import results_modifiers
from hutch_bunny.core.logger import logger


def handle_task(
    task_data: dict[str, object],
    db_manager: SyncDBManager,
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
    result_modifier: list[dict[str, str | int]] = results_modifiers(
        low_number_suppression_threshold=int(
            settings.LOW_NUMBER_SUPPRESSION_THRESHOLD or 0
        ),
        rounding_target=int(settings.ROUNDING_TARGET or 0),
    )
    try:
        result = execute_query(
            task_data,
            result_modifier,
            db_manager=db_manager,
        )
        task_api_client.send_results(result)
    except NotImplementedError as e:
        logger.error(f"Not implemented: {e}")
