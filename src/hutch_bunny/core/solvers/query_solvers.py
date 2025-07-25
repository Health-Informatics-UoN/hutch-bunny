import base64
from hutch_bunny.core.logger import logger


from hutch_bunny.core.solvers.availability_solver import AvailabilitySolver
from hutch_bunny.core.db_manager import SyncDBManager
from hutch_bunny.core.rquest_models.distribution import (
    DistributionQuery,
    DistributionQueryType,
)
from hutch_bunny.core.rquest_models.availability import AvailabilityQuery
from hutch_bunny.core.rquest_models.file import File
from hutch_bunny.core.solvers.availability_solver import ResultModifier

from hutch_bunny.core.rquest_models.result import RquestResult
from hutch_bunny.core.settings import Settings
from hutch_bunny.core.solvers.demographics_solver import (
    DemographicsDistributionQuerySolver,
)
from hutch_bunny.core.solvers.distribution_solver import CodeDistributionQuerySolver
from hutch_bunny.core.services.metadata_service import MetadataService

settings = Settings()
metadata_service = MetadataService()


def solve_availability(
    results_modifier: list[ResultModifier],
    db_manager: SyncDBManager,
    query: AvailabilityQuery,
) -> RquestResult:
    """Solve RQuest availability queries.

    Args:
        results_modifier: List
            A list of modifiers to be applied to the results of the query before returning them to Relay

        db_manager (SyncDBManager): The database manager
        query (AvailabilityQuery): The availability query object


    Returns:
        RquestResult: Result object for the query
    """
    solver = AvailabilitySolver(db_manager, query)
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
    db_manager: SyncDBManager, query: DistributionQuery
) -> CodeDistributionQuerySolver | DemographicsDistributionQuerySolver:
    """Return a distribution query solver depending on the query.
    If `query.code` is "GENERIC", return a `CodeDistributionQuerySolver`.
    If `query.code` is "DEMOGRAPHICS", return a `DemographicsDistributionQuerySolver`.

    Args:
        db_manager (SyncDBManager): The database manager.
        query (DistributionQuery): The distribution query to solve.

    Returns:
        CodeDistributionQuerySolver | DemographicsDistributionQuerySolver: The solver for the distribution query type.
    """

    if query.code == DistributionQueryType.GENERIC:
        return CodeDistributionQuerySolver(db_manager, query)
    if query.code == DistributionQueryType.DEMOGRAPHICS:
        return DemographicsDistributionQuerySolver(db_manager, query)
    raise NotImplementedError(f"Queries with code: {query.code} are not yet supported.")


def solve_distribution(
    results_modifier: list[ResultModifier],
    db_manager: SyncDBManager,
    query: DistributionQuery,
) -> RquestResult:
    """Solve RQuest distribution queries.

    Args:
        db_manager (SyncDBManager): The database manager
        query (DistributionQuery): The distribution query object
        results_modifier: List
            A list of modifiers to be applied to the results of the query before returning them to Relay

    Returns:
        DistributionResult: Result object for the query
    """
    solver = _get_distribution_solver(db_manager, query)
    try:
        res, count = solver.solve_query(results_modifier)
        # Convert file data to base64
        res_b64_bytes = base64.b64encode(res.encode("utf-8"))  # bytes
        size = len(res_b64_bytes) / 1000  # length of file data in KB
        res_b64 = res_b64_bytes.decode("utf-8")  # convert back to string, now base64

        result_file = File(
            data=res_b64,
            description="Result of code.distribution analysis",
            name=query.code.file_name,
            sensitive=True,
            reference="",
            size=size,
            type_="BCOS",
        )
        # Metadata file is only for distribution queries
        if query.code == DistributionQueryType.GENERIC:
            metadata_file = metadata_service.generate_metadata()
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
