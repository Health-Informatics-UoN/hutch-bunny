import os
from typing import Tuple, List

from sqlalchemy import distinct, func, select

from hutch_bunny.core.obfuscation import apply_filters
from hutch_bunny.core.db_manager import SyncDBManager
from hutch_bunny.core.entities import (
    Concept,
    Person,
)
from hutch_bunny.core.logger import logger, INFO
from tenacity import (
    retry,
    stop_after_attempt,
    wait_fixed,
    before_sleep_log,
    after_log,
)
from hutch_bunny.core.rquest_dto.query import DistributionQuery
from hutch_bunny.core.solvers.availability_solver import ResultModifier


class DemographicsDistributionQuerySolver:
    """
    Solve distribution queries for demographics queries.

    Args:
        db_manager (SyncDBManager): The database manager.
        query (DistributionQuery): The distribution query to solve.

    Attributes:
        output_cols (list): A list of column names for the output table.
    """

    output_cols = [
        "BIOBANK",
        "CODE",
        "DESCRIPTION",
        "COUNT",
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

    def __init__(self, db_manager: SyncDBManager, query: DistributionQuery) -> None:
        self.db_manager = db_manager
        self.query = query

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(60),
        before_sleep=before_sleep_log(logger, INFO),
        after=after_log(logger, INFO),
    )
    def solve_query(self, results_modifier: List[ResultModifier]) -> Tuple[str, int]:
        """Build table of demographics query and return as a TAB separated string
        along with the number of rows.

        Parameters
        ----------
        results_modifier: List
        A list of modifiers to be applied to the results of the query before returning them to Relay

        Returns:
            Tuple[str, int]: The table as a string and the number of rows.
        """
        # Get modifier values
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

        # People count statement
        if rounding > 0:
            stmnt = select(
                func.round((func.count(distinct(Person.person_id)) / rounding), 0)
                * rounding,
                Person.gender_concept_id,
            ).group_by(Person.gender_concept_id)
        else:
            stmnt = select(
                func.count(distinct(Person.person_id)), Person.gender_concept_id
            ).group_by(Person.gender_concept_id)

        if low_number > 0:
            stmnt = stmnt.having(func.count(distinct(Person.person_id)) > low_number)

        # Get concept IDs for gender
        concept_ids = [8507, 8532]
        concept_query = select(Concept.concept_id, Concept.concept_name).where(
            Concept.concept_id.in_(concept_ids)
        )

        # Get the data
        with self.db_manager.engine.connect() as con:
            # Get counts
            result = con.execute(stmnt)
            counts_by_gender = {gender_id: count for count, gender_id in result}
            
            # Get concept descriptions
            concept_result = con.execute(concept_query)
            concept_names = {concept_id: name for concept_id, name in concept_result}

        # Calculate total count with suppression
        total_count = apply_filters(sum(counts_by_gender.values()), results_modifier)

        # Build alternatives string
        alternatives = "^"
        for concept_id in concept_ids:
            if concept_id in counts_by_gender:
                count = apply_filters(counts_by_gender[concept_id], results_modifier)
                name = concept_names.get(concept_id, "Unknown")
                alternatives += f"{name}|{count}^"

        # Create rows of data
        rows = [
            {
                "COUNT": total_count,
                "CATEGORY": "DEMOGRAPHICS",
                "CODE": "SEX",
                "BIOBANK": self.query.collection,
                "DATASET": "person",
                "DESCRIPTION": "Sex",
                "ALTERNATIVES": alternatives,
                "MIN": "",
                "Q1": "",
                "MEDIAN": "",
                "MEAN": "",
                "Q3": "",
                "MAX": "",
                "OMOP": "",
                "OMOP_DESCR": "",
            },
            {
                "COUNT": total_count,
                "CATEGORY": "DEMOGRAPHICS",
                "CODE": "GENOMICS",
                "BIOBANK": self.query.collection,
                "DATASET": "person",
                "DESCRIPTION": "Genomics",
                "ALTERNATIVES": f"^No|{total_count}^",
                "MIN": "",
                "Q1": "",
                "MEDIAN": "",
                "MEAN": "",
                "Q3": "",
                "MAX": "",
                "OMOP": "",
                "OMOP_DESCR": "",
            }
        ]

        # Format as tab-separated string
        header = "\t".join(self.output_cols)
        values = ["\t".join(str(row.get(col, "")) for col in self.output_cols) for row in rows]
        result_string = f"{header}{os.linesep}{os.linesep.join(values)}"

        return result_string, len(rows)
