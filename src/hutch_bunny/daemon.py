import hutch_bunny.core.settings as settings
from hutch_bunny.core.execute_query import execute_query
from hutch_bunny.core.rquest_dto.result import RquestResult
from hutch_bunny.core.task_api_client import TaskApiClient
from hutch_bunny.core.results_modifiers import results_modifiers
from hutch_bunny.core.logger import logger
from hutch_bunny.core.setting_database import setting_database
from hutch_bunny.core.polling_service import PollingService


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

    polling_service = PollingService(client, logger, polling_endpoint)
    polling_service.poll_for_jobs(db_manager, result_modifier)


if __name__ == "__main__":
    main()
