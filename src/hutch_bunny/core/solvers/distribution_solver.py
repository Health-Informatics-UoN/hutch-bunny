import os
from hutch_bunny.core.logger import logger, INFO
from typing import Tuple, Type, Union
import pandas as pd

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
from hutch_bunny.core.rquest_dto.query import DistributionQuery
from sqlalchemy import select
from hutch_bunny.core.solvers.availability_solver import ResultModifier

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

         Returns:
             Tuple[str, int]: The table as a string and the number of rows.
        """
        # Prepare the empty results data frame
        df = pd.DataFrame(columns=self.output_cols)

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

        # Get the counts for each concept ID
        counts: list[int] = []
        concepts: list[int] = []
        categories: list[str] = []
        biobanks: list[str] = []
        omop_desc: list[str] = []

        with self.db_client.engine.connect() as con:
            for domain_id in self.allowed_domains_map:
                logger.debug(domain_id)
                # get the right table and column based on the domain
                table = self.allowed_domains_map[domain_id]
                concept_col = self.domain_concept_id_map[domain_id]

                # gets a list of all concepts within this given table and their respective counts

                if rounding > 0:
                    stmnt = (
                        select(
                            func.round(
                                (func.count(distinct(table.person_id)) / rounding), 0
                            )
                            * rounding,
                            Concept.concept_id,
                            Concept.concept_name,
                        )
                        .join(Concept, concept_col == Concept.concept_id)
                        .group_by(Concept.concept_id, Concept.concept_name)
                    )
                else:
                    stmnt = (
                        select(
                            func.count(distinct(table.person_id)),
                            Concept.concept_id,
                            Concept.concept_name,
                        )
                        .join(Concept, concept_col == Concept.concept_id)
                        .group_by(Concept.concept_id, Concept.concept_name)
                    )

                if low_number > 0:
                    stmnt = stmnt.having(
                        func.count(distinct(table.person_id)) > low_number
                    )

                res = pd.read_sql(stmnt, con)

                counts.extend(res.iloc[:, 0])

                concepts.extend(res.iloc[:, 1])
                omop_desc.extend(res.iloc[:, 2])
                # add the same category and collection if, for the number of results received
                categories.extend([domain_id] * len(res))
                biobanks.extend([self.query.collection] * len(res))

        for i in range(len(counts)):
            counts[i] = apply_filters(counts[i], results_modifier)

        counts = list(map(int, counts))

        df["COUNT"] = counts
        # todo: dont think concepts contains anything?
        df["OMOP"] = concepts
        df["CATEGORY"] = categories
        df["CODE"] = df["OMOP"].apply(lambda x: f"OMOP:{x}")
        df["BIOBANK"] = biobanks
        df["OMOP_DESCR"] = omop_desc

        # replace NaN values with empty string
        df = df.fillna("")
        # Convert df to tab separated string
        results = list(["\t".join(df.columns)])
        for _, row in df.iterrows():
            results.append("\t".join([str(r) for r in row.values]))

        return os.linesep.join(results), len(df)
