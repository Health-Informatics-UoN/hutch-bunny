import os
from typing import Tuple
import pandas as pd

from sqlalchemy import distinct, func

from hutch_bunny.core.obfuscation import apply_filters
from hutch_bunny.core.db_manager import SyncDBManager
from hutch_bunny.core.entities import (
    Concept,
    Person,
)
from hutch_bunny.core.rquest_dto.query import DistributionQuery
from sqlalchemy import select
from hutch_bunny.core.solvers.availability_solver import ResultModifier


class DemographicsDistributionQuerySolver:
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

    def solve_query(self, results_modifier: list[ResultModifier]) -> Tuple[str, int]:
        """Build table of demographics query and return as a TAB separated string
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
        datasets: list[str] = []
        codes: list[str] = []
        descriptions: list[str] = []
        alternatives: list[str] = []

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

        concepts.append(8507)
        concepts.append(8532)

        # Concept description statement
        concept_query = select(Concept.concept_id, Concept.concept_name).where(
            Concept.concept_id.in_(concepts)
        )

        # Get the data
        with self.db_manager.engine.connect() as con:
            res = pd.read_sql(stmnt, con)
            concepts_df = pd.read_sql_query(concept_query, con=con)

        combined = res.merge(
            concepts_df,
            left_on="gender_concept_id",
            right_on="concept_id",
            how="left",
        )

        suppressed_count: int = apply_filters(res.iloc[:, 0].sum(), results_modifier)

        # Compile the data
        counts.append(suppressed_count)
        concepts.extend(res.iloc[:, 1])
        categories.append("DEMOGRAPHICS")
        biobanks.append(self.query.collection)
        datasets.append("person")
        descriptions.append("Sex")
        codes.append("SEX")

        alternative = "^"
        for _, row in combined.iterrows():
            alternative += f"{row[Concept.concept_name.name]}|{apply_filters(row.iloc[0], results_modifier)}^"
        alternatives.append(alternative)

        # Fill out the results table
        df["COUNT"] = counts
        df["CATEGORY"] = categories
        df["CODE"] = codes
        df["BIOBANK"] = biobanks
        df["DATASET"] = datasets
        df["DESCRIPTION"] = descriptions
        df["ALTERNATIVES"] = alternatives

        df = df.fillna("")

        # Convert df to tab separated string
        results = list(["\t".join(df.columns)])
        for _, row in df.iterrows():
            results.append("\t".join([str(r) for r in row.values]))
        return os.linesep.join(results), len(df)
