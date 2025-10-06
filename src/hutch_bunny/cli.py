import json

from hutch_bunny.core.results_modifiers import (
    get_results_modifiers_from_str,
)
from hutch_bunny.core.execute_query import execute_query
from hutch_bunny.core.rquest_models.result import RquestResult
from hutch_bunny.core.parser import parser
from hutch_bunny.core.logger import configure_logger, logger
from hutch_bunny.core.db import get_db_client
from hutch_bunny.core.settings import Settings
from importlib.metadata import version


def save_to_output(result: RquestResult, destination: str) -> None:
    """Save the result to a JSON file.

    Args:
        result (RquestResult): The object containing the result of a query.
        destination (str): The name of the JSON file to save the results.

    Raises:
        ValueError: A path to a non-JSON file was passed as the destination.
    """
    if not destination.endswith(".json"):
        raise ValueError("Please specify a JSON file (ending in '.json').")

    try:
        with open(destination, "w") as output_file:
            file_body = json.dumps(result.to_dict())
            output_file.write(file_body)
    except Exception as e:
        logger.error(str(e), exc_info=True)


def main(argv=None) -> None:
    settings: Settings = Settings()
    configure_logger(settings)
    logger.info(f"Starting Bunny version: {version('hutch_bunny')}")
    logger.debug("Settings: %s", settings.safe_model_dump())
    # Setting database connection
    db_client = get_db_client(settings)
    # Bunny passed args.
    args = parser.parse_args(argv)

    with open(args.body) as body:
        query_dict = json.load(body)

    results_modifier: list[dict] = get_results_modifiers_from_str(
        args.results_modifiers
    )

    result = execute_query(query_dict, results_modifier, db_client=db_client)
    logger.debug(f"Results: {result.to_dict()}")
    save_to_output(result, args.output)
    logger.info(f"Saved results to {args.output}")


if __name__ == "__main__":
    main()
