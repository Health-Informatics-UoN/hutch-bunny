from opentelemetry import trace

from hutch_bunny.core.logger import logger
from hutch_bunny.core.solvers.availability_solver import AvailabilitySolver
from hutch_bunny.core.db import BaseDBClient
from hutch_bunny.core.rquest_models.availability import AvailabilityQuery
from hutch_bunny.core.rquest_models.file import File
from hutch_bunny.core.rquest_models.distribution import (
    DistributionQuery,
    DistributionQueryType,
)

from hutch_bunny.core.rquest_models.result import RquestResult
from hutch_bunny.core.settings import Settings
from hutch_bunny.core.solvers.demographics_solver import (
    DemographicsDistributionQuerySolver,
)
from hutch_bunny.core.solvers.distribution_solver import CodeDistributionQuerySolver
from hutch_bunny.core.services.metadata_service import MetadataService
from hutch_bunny.core.telemetry import trace_operation
from hutch_bunny.core.obfuscation import encode_output


settings = Settings()
metadata_service = MetadataService()

@trace_operation("solve_availability", span_kind=trace.SpanKind.INTERNAL)
def solve_availability(
    results_modifier: list[dict[str, str | int]],
    db_client: BaseDBClient,
    query: AvailabilityQuery,
) -> RquestResult:
    """Solve an availability query.

    Args:
        results_modifier (list[dict[str, str | int]]): The results modifier.
        db_client (BaseDBClient): The database client.
        query (AvailabilityQuery): The query to solve.

    Returns:
        RquestResult: The result of the query.
    """
    solver = AvailabilitySolver(db_client, query)
    try:
        count_ = solver.solve_query(results_modifier)
        result = RquestResult(
            status="ok", count=count_, collection_id=query.collection, uuid=query.uuid
        )
        logger.info("Solved availability query")
    except Exception as e:
        logger.error(str(e))
        result = RquestResult(
            status="error", count=0, collection_id=query.collection, uuid=query.uuid
        )

    return result


def _get_distribution_solver(
    db_client: BaseDBClient, query: DistributionQuery
) -> CodeDistributionQuerySolver | DemographicsDistributionQuerySolver:
    """Return a distribution query solver depending on the query.
    If `query.code` is "GENERIC", return a `CodeDistributionQuerySolver`.
    If `query.code` is "DEMOGRAPHICS", return a `DemographicsDistributionQuerySolver`.

    Args:
        db_client (BaseDBClient): The database client.
        query (DistributionQuery): The distribution query to solve.

    Returns:
        CodeDistributionQuerySolver | DemographicsDistributionQuerySolver: The solver for the distribution query type.
    """

    if query.code == DistributionQueryType.GENERIC:
        return CodeDistributionQuerySolver(db_client, query)
    if query.code == DistributionQueryType.DEMOGRAPHICS:
        return DemographicsDistributionQuerySolver(db_client, query)
    raise NotImplementedError(f"Queries with code: {query.code} are not yet supported.")


@trace_operation("solve_distribution", span_kind=trace.SpanKind.INTERNAL)
def solve_distribution(
    results_modifier: list[dict[str, str | int]],
    db_client: BaseDBClient,
    query: DistributionQuery,
    encode_result: bool = True 
) -> RquestResult:
    """Solve a distribution query.

    Args:
        results_modifier (list[dict[str, str | int]]): The results modifier.
        db_client (BaseDBClient): The database client.
        query (DistributionQuery): The query to solve.

    Returns:
        RquestResult: The result of the query.
    """
    solver = _get_distribution_solver(db_client, query)
    try:
        res, count = solver.solve_query(results_modifier)
        
        if encode_result: 
            res, size = encode_output(res)
        else: 
            size = len(res.encode("utf-8")) / 1000

        result_file = File(
            data=res,
            description="Result of code.distribution analysis",
            name=query.code.file_name,
            sensitive=True,
            reference="",
            size=size,
            type_="BCOS",
        )
        # Metadata file is only for distribution queries
        if query.code == DistributionQueryType.GENERIC:
            metadata_file = metadata_service.generate_metadata(encode_result)
        else:
            metadata_file = None

        result = RquestResult(
            uuid=query.uuid,
            status="ok",
            count=count,
            datasets_count=1,
            files=[result_file, metadata_file] if metadata_file else [result_file],
            collection_id=query.collection,
        )
    except Exception as e:
        logger.error(str(e))
        result = RquestResult(
            uuid=query.uuid,
            status="error",
            count=0,
            datasets_count=0,
            files=[],
            collection_id=query.collection,
        )

    return result
