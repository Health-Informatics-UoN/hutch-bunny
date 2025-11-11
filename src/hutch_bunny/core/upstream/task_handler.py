from opentelemetry import trace

from hutch_bunny.core.db import BaseDBClient
from hutch_bunny.core.config import DaemonSettings
from hutch_bunny.core.config.obfuscation import ObfuscationSettings
from hutch_bunny.core.execute_query import execute_query
from hutch_bunny.core.upstream.task_api_client import TaskApiClient
from hutch_bunny.core.results_modifiers import results_modifiers
from hutch_bunny.core.logger import logger
from hutch_bunny.core.telemetry import trace_operation


@trace_operation("handle_task", span_kind=trace.SpanKind.CONSUMER)
def handle_task(
    task_data: dict[str, object],
    db_client: BaseDBClient,
    settings: DaemonSettings,
    task_api_client: TaskApiClient,
) -> None:
    """
    Handles a task by executing a query and sending the results to the task API.

    Args:
        task_data (dict): The task data to execute the query on.
        db_client (BaseDBClient): The database client to use to execute the query.
        settings (DaemonSettings): The settings to use to execute the query.
        task_api_client (TaskApiClient): The task API client to use to send the results.

    Returns:
        None
    """
    obfuscation_settings = ObfuscationSettings()
    result_modifier: list[dict[str, str | int]] = results_modifiers(
        low_number_suppression_threshold=int(
            obfuscation_settings.LOW_NUMBER_SUPPRESSION_THRESHOLD or 0
        ),
        rounding_target=int(obfuscation_settings.ROUNDING_TARGET or 0),
    )
    try:
        result = execute_query(
            task_data,
            result_modifier,
            db_client=db_client,
            settings=settings
        )
        task_api_client.send_results(result)
    except NotImplementedError as e:
        logger.error(f"Not implemented: {e}. Data: {task_data}")
    except ValueError as e:
        logger.error(f"Invalid task input: {e}. Data: {task_data}")
    except Exception as e:
        logger.error(f"Unexpected error handling task: {e}", exc_info=True)
