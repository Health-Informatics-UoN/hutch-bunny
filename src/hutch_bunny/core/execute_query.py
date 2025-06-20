from hutch_bunny.core.logger import logger
from hutch_bunny.core.solvers import query_solvers
from hutch_bunny.core.rquest_models.availability import AvailabilityQuery
from hutch_bunny.core.rquest_models.distribution import (
    DistributionQuery,
    DistributionQueryType,
)
from hutch_bunny.core.rquest_models.result import RquestResult
from hutch_bunny.core.db_manager import SyncDBManager


def execute_query(
    query_dict: dict[str, object],
    results_modifier: list[dict[str, str | int]],
    db_manager: SyncDBManager,
) -> RquestResult:
    """
    Executes either an availability query or a distribution query, and returns results filtered by modifiers

    Parameters
    ----------
    results_modifier: List
        A list of modifiers to be applied to the results of the query before returning them to Relay

    query_dict: Dict
        A dictionary carrying the payload for the query. If there is an 'analysis' item in the query, it's a distribution query. Otherwise, it executes an availability query

    Returns
        RquestResult
    """

    logger.info("Processing query...")
    logger.debug(query_dict)

    if "analysis" in query_dict.keys():
        logger.debug("Processing distribution query...")
        try:
            distribution_query = DistributionQuery.model_validate(query_dict)

            # Check for ICD-MAIN queries before calling the solver
            # So we dont return results upstream
            if distribution_query.code == DistributionQueryType.ICD_MAIN:
                raise NotImplementedError(
                    "ICD-MAIN queries are not yet supported. See: https://github.com/Health-Informatics-UoN/hutch-bunny/issues/30"
                )

            result = query_solvers.solve_distribution(
                results_modifier, db_manager=db_manager, query=distribution_query
            )

            return result
        except TypeError as te:  # raised if the distribution query json format is wrong
            logger.error(str(te), exc_info=True)
        except ValueError as ve:
            # raised if there was an issue saving the output or
            # the query json has incorrect values
            logger.error(str(ve), exc_info=True)

    else:
        logger.debug("Processing availability query...")
        try:
            availability_query = AvailabilityQuery.model_validate(query_dict)

            result = query_solvers.solve_availability(
                results_modifier, db_manager=db_manager, query=availability_query
            )
            return result
        except TypeError as te:  # raised if the distribution query json format is wrong
            logger.error(str(te), exc_info=True)
        except ValueError as ve:
            # raised if there was an issue saving the output or
            # the query json has incorrect values
            logger.error(str(ve), exc_info=True)
    raise ValueError("Invalid query type")
