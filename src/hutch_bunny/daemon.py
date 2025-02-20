import hutch_bunny.core.settings as settings
from hutch_bunny.core.execute_query import execute_query
from hutch_bunny.core.rquest_dto.result import RquestResult
from hutch_bunny.core.task_api_client import TaskApiClient
from hutch_bunny.core.results_modifiers import results_modifiers
from hutch_bunny.core.logger import logger
from hutch_bunny.core.setting_database import setting_database
from hutch_bunny.core.polling_service import PollingService

def handle_response(response, db_manager, result_modifier):
    if response.status_code == 200:
        logger.info("Job received. Resolving...")
        logger.debug("JSON Response: %s", response.json())
        query_dict: dict = response.json()
        result = execute_query(
            query_dict,
            result_modifier,
            logger=logger,
            db_manager=db_manager,
        )
        logger.debug(f"Result: {result.to_dict()}")
        if not isinstance(result, RquestResult):
            raise TypeError("Payload does not match RQuest result schema.")
        # send_results(result)
    elif response.status_code == 204:
        logger.info("Looking for job...")
    elif response.status_code == 401:
        logger.info("Failed to authenticate with task server.")
    else:
        logger.info("Got http status code: %s", response.status_code)

def main() -> None:
    settings.log_settings()
    db_manager = setting_database(logger=logger)
    client = TaskApiClient()
    result_modifier: list[dict] = results_modifiers(
        low_number_suppression_threshold=int(
            settings.LOW_NUMBER_SUPPRESSION_THRESHOLD or 0
        ),
        rounding_target=int(settings.ROUNDING_TARGET or 0),
    )
    polling_endpoint = (
        f"task/nextjob/{settings.COLLECTION_ID}.{settings.TASK_API_TYPE}"
        if settings.TASK_API_TYPE
        else f"task/nextjob/{settings.COLLECTION_ID}"
    )

    polling_service = PollingService(client, logger, polling_endpoint, 
                                     lambda response: handle_response(response, db_manager, result_modifier))
    polling_service.poll_for_jobs()


if __name__ == "__main__":
    main()
