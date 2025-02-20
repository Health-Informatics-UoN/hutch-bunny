import time
from hutch_bunny.core.settings import get_settings, DaemonSettings
from hutch_bunny.core.execute_query import execute_query
from hutch_bunny.core.rquest_dto.result import RquestResult
from hutch_bunny.core.task_api_client import TaskApiClient
from hutch_bunny.core.results_modifiers import results_modifiers
from hutch_bunny.core.logger import logger
from hutch_bunny.core.setting_database import setting_database
from hutch_bunny.core.polling_service import PollingService
from importlib.metadata import version

def handle_task(task_data, db_manager, settings, logger, task_api_client):
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
