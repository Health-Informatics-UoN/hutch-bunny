from typing import Dict, List
from logging import Logger
from hutch_bunny.core import query_solvers
from hutch_bunny.core.rquest_dto.query import AvailabilityQuery, DistributionQuery
from hutch_bunny.core.obfuscation import (
    apply_filters,
)
from hutch_bunny.core.rquest_dto.result import RquestResult


def execute_query(
    query_dict: Dict,
    results_modifiers: List,
    logger: Logger,
    db_manager,
) -> RquestResult:
    """
    Executes either an availability query or a distribution query, and returns results filtered by modifiers

    Parameters
    ----------
    query_dict: Dict
        A dictionary carrying the payload for the query. If there is an 'analysis' item in the query, it's a distribution query. Otherwise, it executes an availability query
    results_modifers: List
        A list of modifiers applied to the results of the query before returning them to Relay

    Returns
        RquestResult
    """

    logger.info("Processing query...")

    if "analysis" in query_dict.keys():
        try:
            query = DistributionQuery.from_dict(query_dict)
            result = query_solvers.solve_distribution(
                db_manager=db_manager, query=query
            )
            return result
        except TypeError as te:  # raised if the distribution query json format is wrong
            logger.error(str(te), exc_info=True)
        except ValueError as ve:
            # raised if there was an issue saving the output or
            # the query json has incorrect values
            logger.error(str(ve), exc_info=True)

    else:
        try:
            query = AvailabilityQuery.from_dict(query_dict)
            result = query_solvers.solve_availability(
                db_manager=db_manager, query=query
            )
            result.count = apply_filters(result.count, results_modifiers)
            return result
        except TypeError as te:  # raised if the distribution query json format is wrong
            logger.error(str(te), exc_info=True)
        except ValueError as ve:
            # raised if there was an issue saving the output or
            # the query json has incorrect values
            logger.error(str(ve), exc_info=True)
