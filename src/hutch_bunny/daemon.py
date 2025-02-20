from logging import Logger
from hutch_bunny.core.db_manager import BaseDBManager
from hutch_bunny.core.settings import get_settings, DaemonSettings
from hutch_bunny.core.execute_query import execute_query
from hutch_bunny.core.rquest_dto.result import RquestResult
from hutch_bunny.core.task_api_client import TaskApiClient
from hutch_bunny.core.results_modifiers import results_modifiers
from hutch_bunny.core.logger import logger
from hutch_bunny.core.setting_database import setting_database
from hutch_bunny.core.polling_service import PollingService
from importlib.metadata import version

def handle_task(task_data: dict, db_manager: BaseDBManager, settings: DaemonSettings, logger: Logger, task_api_client: TaskApiClient) -> None:
    """
    Handles a task by executing a query and sending the results to the task API.

    Args:
        task_data (dict): The task data to execute the query on.
        db_manager (BaseDBManager): The database manager to use to execute the query.
        settings (DaemonSettings): The settings to use to execute the query.
        logger (Logger): The logger to use to log messages.
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
        logger=logger,
        db_manager=db_manager,
    )
    if not isinstance(result, RquestResult):
        raise TypeError("Payload does not match RQuest result schema.")
    task_api_client.send_results(result)


def main() -> None:
    """
    Main function to start the daemon process.
    """
    logger.info(f"Starting Bunny version {version('hutch_bunny')} ")
    settings: DaemonSettings = get_settings(daemon=True)
    logger.debug("Settings: %s", settings.safe_model_dump())

    # Setting database connection
    db_manager = setting_database(logger=logger)
    
    client = TaskApiClient()
    polling_service = PollingService(
        client, logger, 
        lambda task_data: handle_task(task_data, db_manager, settings, logger, client)
    )
    polling_service.poll_for_tasks()


if __name__ == "__main__":
    main()
