import os
from hutch_bunny.core.logger import logger, INFO
from typing import Tuple, Type, Union

from sqlalchemy import distinct, func

from hutch_bunny.core.obfuscation import apply_filters
from hutch_bunny.core.db import BaseDBClient
from hutch_bunny.core.db.entities import (
    Concept,
    ConditionOccurrence,
    Measurement,
    Observation,
    Person,
    DrugExposure,
    ProcedureOccurrence,
)
from tenacity import (
    retry,
    stop_after_attempt,
    wait_fixed,
    before_sleep_log,
    after_log,
)
from hutch_bunny.core.rquest_models.distribution import DistributionQuery
from sqlalchemy import select
from hutch_bunny.core.solvers.availability_solver import ResultModifier
from hutch_bunny.core.db.utils import log_query

# Type alias for tables that have person_id
PersonTable = Union[
    ConditionOccurrence,
    Measurement,
    Observation,
    Person,
    DrugExposure,
    ProcedureOccurrence,
]


class CodeDistributionQuerySolver:
    """
    Solve distribution queries for code queries.

    Args:
        db_client (SyncDBClient): The database client.
        query (DistributionQuery): The distribution query to solve.

    Attributes:
        allowed_domains_map (dict): A dictionary mapping domain IDs to their respective SQLAlchemy models.
        domain_concept_id_map (dict): A dictionary mapping domain IDs to their respective concept ID columns.
        output_cols (list): A list of column names for the output table.
    """

    allowed_domains_map: dict[str, Type[PersonTable]] = {
        "Condition": ConditionOccurrence,
        "Ethnicity": Person,
        "Drug": DrugExposure,
        "Gender": Person,
        "Race": Person,
        "Measurement": Measurement,
        "Observation": Observation,
        "Procedure": ProcedureOccurrence,
    }
    domain_concept_id_map = {
        "Condition": ConditionOccurrence.condition_concept_id,
        "Ethnicity": Person.ethnicity_concept_id,
        "Drug": DrugExposure.drug_concept_id,
        "Gender": Person.gender_concept_id,
        "Race": Person.race_concept_id,
        "Measurement": Measurement.measurement_concept_id,
        "Observation": Observation.observation_concept_id,
        "Procedure": ProcedureOccurrence.procedure_concept_id,
    }

    # this one is unique for this resolver
    output_cols = [
        "BIOBANK",
        "CODE",
        "COUNT",
        "DESCRIPTION",
        "MIN",
        "Q1",
        "MEDIAN",
        "MEAN",
        "Q3",
        "MAX",
        "ALTERNATIVES",
        "DATASET",
        "OMOP",
        "OMOP_DESCR",
        "CATEGORY",
    ]

    def __init__(self, db_client: BaseDBClient, query: DistributionQuery) -> None:
        self.db_client = db_client
        self.query = query

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(60),
        before_sleep=before_sleep_log(logger, INFO),
        after=after_log(logger, INFO),
    )
    def solve_query(self, results_modifier: list[ResultModifier]) -> Tuple[str, int]:

        """Build table of distribution query and return as a TAB separated string
        along with the number of rows.

        Parameters
        ----------
            results_modifier: List
            A list of modifiers to be applied to the results of the query before returning them to Relay
        """
        low_number: int = next(
            (
                item["threshold"] if item["threshold"] is not None else 10
                for item in results_modifier
                if item["id"] == "Low Number Suppression"
            ),
            10,
        )
        rounding: int = next(
            (
                item["nearest"] if item["nearest"] is not None else 10
                for item in results_modifier
                if item["id"] == "Rounding"
            ),
            10,
        )

        counts: list[int] = []
        concepts: list[int] = []
        categories: list[str] = []
        omop_desc: list[str] = []

        with self.db_client.engine.connect() as con:
            for domain_id in self.allowed_domains_map:
                logger.debug(domain_id)

                # Get table and concept column for this domain
                table = self.allowed_domains_map[domain_id]
                concept_col = self.domain_concept_id_map[domain_id]

                # Step 1: subquery to count distinct person_id per concept_id
                subq = (
                    select(
                        concept_col.label("concept_id"),
                        func.count(distinct(table.person_id)).label("count_agg")
                    )
                    .group_by(concept_col)
                    .subquery()
                )

                # Step 2: join with Concept table
                stmnt = (
                    select(
                        # Apply rounding only here, after the join
                        (func.round(subq.c.count_agg / rounding, 0) * rounding).label("count_agg_rounded")
                        if rounding > 0 else subq.c.count_agg,
                        Concept.concept_id,
                        Concept.concept_name
                    )
                    .join(Concept, subq.c.concept_id == Concept.concept_id)
                )

                # Step 3: optional low-number filter
                if low_number > 0:
                    stmnt = stmnt.where(subq.c.count_agg > low_number)

                # Execute
                result = con.execute(stmnt)
                res = result.fetchall()

                for row in res:
                    counts.append(row[0])
                    concepts.append(row[1])
                    omop_desc.append(row[2])

                # Track categories
                num_results = len(res)
                categories.extend([domain_id] * num_results)

                log_query(stmnt, self.db_client.engine)

        # Suppression modifiers applied AFTER the query (unchanged)
        for i in range(len(counts)):
            counts[i] = apply_filters(counts[i], results_modifier)

        counts = list(map(int, counts))

        results = ["\t".join(self.output_cols)]
        for i in range(len(counts)):
            row_values: list[str] = [
                self.query.collection,
                f"OMOP:{concepts[i]}" if i < len(concepts) else "",
                str(counts[i] if i < len(counts) else 0),
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                str(concepts[i] if i < len(concepts) else ""),
                omop_desc[i] if i < len(omop_desc) else "",
                categories[i] if i < len(categories) else "",
            ]
            results.append("\t".join(row_values))

        return os.linesep.join(results), len(counts)

