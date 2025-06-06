import os
from typing import Tuple, List, Dict
from dataclasses import dataclass

from sqlalchemy import Select, distinct, func, select

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


@dataclass
class DemographicsRow:
    """Represents a single row in the demographics output."""

    code: str
    description: str
    count: int
    alternatives: str
    category: str = "DEMOGRAPHICS"
    biobank: str = ""
    dataset: str = "person"
    min_val: str = ""
    q1: str = ""
    median: str = ""
    mean: str = ""
    q3: str = ""
    max_val: str = ""
    omop: str = ""
    omop_descr: str = ""

    def to_dict(self) -> Dict[str, str]:
        """
        Convert the row to a dictionary format.

        Returns:
            Dict[str, str]: The row as a dictionary.
        """
        return {
            "BIOBANK": self.biobank,
            "CODE": self.code,
            "DESCRIPTION": self.description,
            "COUNT": str(self.count),
            "MIN": self.min_val,
            "Q1": self.q1,
            "MEDIAN": self.median,
            "MEAN": self.mean,
            "Q3": self.q3,
            "MAX": self.max_val,
            "ALTERNATIVES": self.alternatives,
            "DATASET": self.dataset,
            "OMOP": self.omop,
            "OMOP_DESCR": self.omop_descr,
            "CATEGORY": self.category,
        }


class DemographicsDistributionQuerySolver:
    """
    Solve distribution queries for demographics queries.

    Args:
        db_manager (SyncDBManager): The database manager.
        query (DistributionQuery): The distribution query to solve.

    Attributes:
        output_cols (list): A list of column names for the output table.
    """

    # Constants
    GENDER_CONCEPT_IDS = [8507, 8532]  # MALE, FEMALE
    DEFAULT_LOW_NUMBER = 10
    DEFAULT_ROUNDING = 10

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

    def _get_modifier_values(
        self, results_modifier: List[ResultModifier]
    ) -> Tuple[int, int]:
        """
        Extract modifier values from the results modifier list.

        Args:
            results_modifier: List[ResultModifier]
                A list of modifiers to be applied to the results of the query

        Returns:
            Tuple[int, int]: The low number and rounding values.
        """
        low_number = next(
            (
                item["threshold"]
                if item["threshold"] is not None
                else self.DEFAULT_LOW_NUMBER
                for item in results_modifier
                if item["id"] == "Low Number Suppression"
            ),
            self.DEFAULT_LOW_NUMBER,
        )
        rounding = next(
            (
                item["nearest"]
                if item["nearest"] is not None
                else self.DEFAULT_ROUNDING
                for item in results_modifier
                if item["id"] == "Rounding"
            ),
            self.DEFAULT_ROUNDING,
        )
        return low_number, rounding

    def _build_gender_query(
        self, rounding: int, low_number: int
    ) -> Select[Tuple[int, int]]:
        """Build the query for gender distribution.

        Args:
            rounding: int
                The rounding value to be used in the query
            low_number: int
                The low number value to be used in the query

        Returns:
            select: The query for gender distribution.
        """
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

        return stmnt

    def _get_concept_data(self) -> Dict[int, str]:
        """
        Get concept descriptions for gender concepts.

        Returns:
            Dict[int, str]: A dictionary of concept IDs and their corresponding names.
        """
        concept_query = select(Concept.concept_id, Concept.concept_name).where(
            Concept.concept_id.in_(self.GENDER_CONCEPT_IDS)
        )
        with self.db_manager.engine.connect() as con:
            concept_result = con.execute(concept_query)
            return {concept_id: name for concept_id, name in concept_result}

    def _build_alternatives_string(
        self,
        counts_by_gender: Dict[int, int],
        concept_names: Dict[int, str],
        results_modifier: List[ResultModifier],
    ) -> str:
        """
        Build the alternatives string for gender distribution.

        Args:
            counts_by_gender: Dict[int, int]
                A dictionary of concept IDs and their corresponding counts
            concept_names: Dict[int, str]
                A dictionary of concept IDs and their corresponding names
            results_modifier: List[ResultModifier]
                A list of modifiers to be applied to the results of the query

        Returns:
            str: The alternatives string for gender distribution.
        """
        alternatives = "^"
        for concept_id in self.GENDER_CONCEPT_IDS:
            if concept_id in counts_by_gender:
                count = apply_filters(counts_by_gender[concept_id], results_modifier)
                name = concept_names.get(concept_id, "Unknown")
                alternatives += f"{name}|{count}^"
        return alternatives

    def _create_demographics_rows(
        self, total_count: int, alternatives: str
    ) -> List[DemographicsRow]:
        """
        Create the demographics rows for the output.

        Args:
            total_count: int
                The total count of the query
            alternatives: str
                The alternatives string for gender distribution.
        """
        return [
            DemographicsRow(
                code="SEX",
                description="Sex",
                count=total_count,
                alternatives=alternatives,
                biobank=self.query.collection,
            ),
            DemographicsRow(
                code="GENOMICS",
                description="Genomics",
                count=total_count,
                alternatives=f"^No|{total_count}^",
                biobank=self.query.collection,
            ),
        ]

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(60),
        before_sleep=before_sleep_log(logger, INFO),
        after=after_log(logger, INFO),
    )
    def solve_query(self, results_modifier: List[ResultModifier]) -> Tuple[str, int]:
        """Build table of demographics query and return as a TAB separated string
        along with the number of rows.

        Args:
            results_modifier: List[ResultModifier]
                A list of modifiers to be applied to the results of the query

        Returns:
            Tuple[str, int]: The table as a string and the number of rows.
        """
        low_number, rounding = self._get_modifier_values(results_modifier)

        # Get the data
        with self.db_manager.engine.connect() as con:
            stmnt = self._build_gender_query(rounding, low_number)
            result = con.execute(stmnt)
            counts_by_gender = {gender_id: count for count, gender_id in result}

            concept_names = self._get_concept_data()

        # Calculate total count with suppression
        total_count = apply_filters(sum(counts_by_gender.values()), results_modifier)

        alternatives = self._build_alternatives_string(
            counts_by_gender, concept_names, results_modifier
        )

        rows = self._create_demographics_rows(total_count, alternatives)

        # Format as tsv
        header = "\t".join(self.output_cols)
        values = [
            "\t".join(str(row.to_dict().get(col, "")) for col in self.output_cols)
            for row in rows
        ]
        result_string = f"{header}{os.linesep}{os.linesep.join(values)}"

        return result_string, len(rows)
