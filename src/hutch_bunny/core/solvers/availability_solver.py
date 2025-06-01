from hutch_bunny.core.db_manager import SyncDBManager

from tenacity import (
    retry,
    stop_after_attempt,
    wait_fixed,
    before_sleep_log,
    after_log,
)


from hutch_bunny.core.obfuscation import apply_filters
from hutch_bunny.core.rquest_dto.query import AvailabilityQuery

from hutch_bunny.core.logger import logger, INFO

from hutch_bunny.core.settings import Settings
from hutch_bunny.core.solvers.availability_query_builder import (
    AvailabilityQueryBuilder,
    ResultModifier,
)


settings = Settings()


# Class for availability queries
class AvailabilitySolver:
    def __init__(self, db_manager: SyncDBManager, query: AvailabilityQuery) -> None:
        self.db_manager = db_manager
        self.query = query

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(60),
        before_sleep=before_sleep_log(logger, INFO),
        after=after_log(logger, INFO),
    )
    def solve_query(self, results_modifier: list[ResultModifier]) -> int:
        """
        Solves a query for availability.

        Args:
            results_modifier (list[ResultModifier]): The results modifier to apply to the query.

        Returns:
            int: The number of results that match the query.
        """
        query_builder = AvailabilityQueryBuilder(self.db_manager.engine, self.query)
        full_query = query_builder.build_query(results_modifier)

        logger.debug(
            str(
                full_query.compile(
                    dialect=self.db_manager.engine.dialect,
                    compile_kwargs={"literal_binds": True},
                )
            )
        )

        with self.db_manager.engine.connect() as con:
            output = con.execute(full_query).fetchone()
            count = int(output[0]) if output is not None else 0

        return apply_filters(count, results_modifier)
