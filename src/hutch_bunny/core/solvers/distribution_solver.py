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

                table = self.allowed_domains_map[domain_id]
                concept_col = self.domain_concept_id_map[domain_id]

                raw_count = func.count(distinct(table.person_id))

                if rounding > 0:
                    rounded_count = func.round((raw_count / rounding), 0) * rounding
                else:
                    rounded_count = raw_count

                stmnt = (
                    select(
                        rounded_count.label("count"),
                        Concept.concept_id,
                        Concept.concept_name,
                    )
                    .join(Concept, concept_col == Concept.concept_id)
                    .group_by(Concept.concept_id, Concept.concept_name)
                )

                # HAVING applies to raw counts, not rounded counts
                if low_number > 0:
                    stmnt = stmnt.having(raw_count > low_number)

                result = con.execute(stmnt)
                res = result.fetchall()

                for row in res:
                    counts.append(row[0])
                    concepts.append(row[1])
                    omop_desc.append(row[2])

                categories.extend([domain_id] * len(res))
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

