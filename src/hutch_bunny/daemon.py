from hutch_bunny.core.settings import get_settings, DaemonSettings
from hutch_bunny.core.db_manager import SyncDBManager
from hutch_bunny.core.upstream.task_api_client import TaskApiClient
from hutch_bunny.core.logger import logger
from hutch_bunny.core.setting_database import setting_database
from hutch_bunny.core.upstream.polling_service import PollingService
from importlib.metadata import version
from hutch_bunny.core.upstream.task_handler import handle_task


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
        client,
        lambda task_data: handle_task(task_data, db_manager, settings, logger, client),
        settings,
        logger,
    )
    polling_service.poll_for_tasks()


if __name__ == "__main__":
    main()
