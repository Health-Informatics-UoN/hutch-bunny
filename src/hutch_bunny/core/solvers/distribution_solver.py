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
            (item["threshold"] if item["threshold"] is not None else 10
             for item in results_modifier
             if item["id"] == "Low Number Suppression"),
            10,
        )
        rounding: int = next(
            (item["nearest"] if item["nearest"] is not None else 10
             for item in results_modifier
             if item["id"] == "Rounding"),
            10,
        )

        counts: list[int] = []
        concepts: list[int] = []
        categories: list[str] = []
        omop_desc: list[str] = []

        with self.db_client.engine.connect() as con:
            for domain_id in self.allowed_domains_map:
                table = self.allowed_domains_map[domain_id]
                concept_col = self.domain_concept_id_map[domain_id]

                # Step 1: subquery with aggregated counts
                if rounding > 0:
                    subq = (
                        select(
                            concept_col.label("concept_id"),
                            (func.round(
                                func.count(distinct(table.person_id)) / rounding, 0
                            ) * rounding).label("count_agg")
                        )
                        .group_by(concept_col)
                        .subquery()
                    )
                else:
                    subq = (
                        select(
                            concept_col.label("concept_id"),
                            func.count(distinct(table.person_id)).label("count_agg")
                        )
                        .group_by(concept_col)
                        .subquery()
                    )

                # Step 2: join to Concept for concept_name
                stmnt = (
                    select(subq.c.count_agg, Concept.concept_id, Concept.concept_name)
                    .join(Concept, subq.c.concept_id == Concept.concept_id)
                )

                # Step 3: apply low_number filter if needed
                if low_number > 0:
                    stmnt = stmnt.where(subq.c.count_agg > low_number)

                # Execute query
                result = con.execute(stmnt)
                res = result.fetchall()

                # Step 4: populate lists, safely handle empty results
                if res:
                    for row in res:
                        counts.append(row[0])
                        concepts.append(row[1])
                        omop_desc.append(row[2])
                    categories.extend([domain_id] * len(res))
                else:
                    # Ensure empty result does not break downstream
                    counts.append(0)
                    concepts.append(None)
                    omop_desc.append("")
                    categories.append(domain_id)

                log_query(stmnt, self.db_client.engine)

        # Step 5: apply modifiers safely
        for i in range(len(counts)):
            counts[i] = apply_filters(counts[i], results_modifier)

        counts = list(map(int, counts))

        # Step 6: build tab-separated results safely
        results = ["\t".join(self.output_cols)]
        for i in range(len(counts)):
            row_values = [
                self.query.collection,
                f"OMOP:{concepts[i]}" if concepts[i] is not None else "",
                str(counts[i]),
                "", "", "", "", "", "", "", "", "",
                str(concepts[i]) if concepts[i] is not None else "",
                omop_desc[i],
                categories[i],
            ]
            results.append("\t".join(row_values))

        return os.linesep.join(results), len(counts)

