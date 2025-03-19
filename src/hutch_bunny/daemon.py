from hutch_bunny.core.settings import get_settings, DaemonSettings
from hutch_bunny.core.upstream.task_api_client import TaskApiClient
from hutch_bunny.core.logger import configure_logger, logger
from hutch_bunny.core.db import get_db_manager
from hutch_bunny.core.upstream.polling_service import PollingService
from importlib.metadata import version
from hutch_bunny.core.upstream.task_handler import handle_task


def main() -> None:
    """
    Main function to start the daemon process.
    """
    settings: DaemonSettings = get_settings(daemon=True)
    configure_logger(settings)
    logger.info(f"Starting Bunny version {version('hutch_bunny')} ")
    logger.debug("Settings: %s", settings.safe_model_dump())

    # Setting database connection
    db_manager = get_db_manager()

    client = TaskApiClient(settings=settings)
    polling_service = PollingService(
        client,
        lambda task_data: handle_task(task_data, db_manager, settings, client),
        settings,
    )
    polling_service.poll_for_tasks()


if __name__ == "__main__":
    main()
