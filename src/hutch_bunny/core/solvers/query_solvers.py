import base64
import os
from hutch_bunny.core.logger import logger
from typing import Tuple
import pandas as pd

from sqlalchemy import func

from hutch_bunny.core.obfuscation import apply_filters
from hutch_bunny.core.solvers.availability_solver import AvailabilitySolver
from hutch_bunny.core.db_manager import SyncDBManager
from hutch_bunny.core.entities import (
    Concept,
    ConditionOccurrence,
    Measurement,
    Observation,
    Person,
    DrugExposure,
    ProcedureOccurrence,
)
from hutch_bunny.core.rquest_dto.query import AvailabilityQuery, DistributionQuery
from hutch_bunny.core.rquest_dto.file import File
from sqlalchemy import select

from hutch_bunny.core.rquest_dto.result import RquestResult
from hutch_bunny.core.enums import DistributionQueryType
from hutch_bunny.core.settings import get_settings
from hutch_bunny.core.constants import DISTRIBUTION_TYPE_FILE_NAMES_MAP


settings = get_settings()


class BaseDistributionQuerySolver:
    def solve_query(self, results_modifier: list) -> Tuple[str, int]:
        raise NotImplementedError


# class for distribution queries
class CodeDistributionQuerySolver(BaseDistributionQuerySolver):
    allowed_domains_map = {
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

    def __init__(self, db_manager: SyncDBManager, query: DistributionQuery) -> None:
        self.db_manager = db_manager
        self.query = query

    def solve_query(self, results_modifier: list) -> Tuple[str, int]:
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

        low_number = next(
            (
                item["threshold"]
                for item in results_modifier
                if item["id"] == "Low Number Suppression"
            ),
            10,
        )
        rounding = next(
            (item["nearest"] for item in results_modifier if item["id"] == "Rounding"),
            10,
        )

        # Get the counts for each concept ID
        counts: list = []
        concepts: list = []
        categories: list = []
        biobanks: list = []
        omop_desc: list = []

        with self.db_manager.engine.connect() as con:
            for domain_id in self.allowed_domains_map:
                logger.debug(domain_id)
                # get the right table and column based on the domain
                table = self.allowed_domains_map[domain_id]
                concept_col = self.domain_concept_id_map[domain_id]

                # gets a list of all concepts within this given table and their respective counts

                if rounding > 0:
                    stmnt = (
                        select(
                            func.round((func.count(table.person_id) / rounding))
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
                            func.count(table.person_id),
                            Concept.concept_id,
                            Concept.concept_name,
                        )
                        .join(Concept, concept_col == Concept.concept_id)
                        .group_by(Concept.concept_id, Concept.concept_name)
                    )

                if low_number > 0:
                    stmnt = stmnt.having(func.count() > low_number)

                res = pd.read_sql(stmnt, con)

                counts.extend(res.iloc[:, 0])
                concepts.extend(res.iloc[:, 1])
                omop_desc.extend(res.iloc[:, 2])
                # add the same category and collection if, for the number of results received
                categories.extend([domain_id] * len(res))
                biobanks.extend([self.query.collection] * len(res))

        for i in range(len(counts)):
            counts[i] = apply_filters(counts[i], results_modifier)

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


class DemographicsDistributionQuerySolver(BaseDistributionQuerySolver):
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

    def solve_query(self, results_modifier: list[dict]) -> Tuple[str, int]:
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

        low_number = next(
            (
                item["threshold"]
                for item in results_modifier
                if item["id"] == "Low Number Suppression"
            ),
            10,
        )
        rounding = next(
            (item["nearest"] for item in results_modifier if item["id"] == "Rounding"),
            10,
        )

        # Get the counts for each concept ID
        counts: list = []
        concepts: list = []
        categories: list = []
        biobanks: list = []
        datasets: list = []
        codes: list = []
        descriptions: list = []
        alternatives: list = []

        # People count statement
        if rounding > 0:
            stmnt = select(
                func.round((func.count() / rounding)) * rounding,
                Person.gender_concept_id,
            ).group_by(Person.gender_concept_id)
        else:
            stmnt = select(
                func.count(Person.person_id), Person.gender_concept_id
            ).group_by(Person.gender_concept_id)

        if low_number > 0:
            stmnt = stmnt.having(func.count() > low_number)

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


def solve_availability(
    results_modifier: list[dict], db_manager: SyncDBManager, query: AvailabilityQuery
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
) -> BaseDistributionQuerySolver:
    """Return a distribution query solver depending on the query.
    If `query.code` is "GENERIC", return a `CodeDistributionQuerySolver`.
    If `query.code` is "DEMOGRAPHICS", return a `DemographicsDistributionQuerySolver`.

    Args:
        db_manager (SyncDBManager): The database manager.
        query (DistributionQuery): The distribution query to solve.

    Returns:
        BaseDistributionQuerySolver: The solver for the distribution query type.
    """

    if query.code == DistributionQueryType.GENERIC:
        return CodeDistributionQuerySolver(db_manager, query)
    if query.code == DistributionQueryType.DEMOGRAPHICS:
        return DemographicsDistributionQuerySolver(db_manager, query)


def solve_distribution(
    results_modifier: list[dict], db_manager: SyncDBManager, query: DistributionQuery
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
            name=DISTRIBUTION_TYPE_FILE_NAMES_MAP.get(query.code, ""),
            sensitive=True,
            reference="",
            size=size,
            type_="BCOS",
        )
        result = RquestResult(
            uuid=query.uuid,
            status="ok",
            count=count,
            datasets_count=1,
            files=[result_file],
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
