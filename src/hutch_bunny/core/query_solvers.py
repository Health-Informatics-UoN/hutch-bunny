import base64
import os
import logging
from typing import Tuple
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

from sqlalchemy import or_, func, extract, BinaryExpression, ColumnElement, Select

from hutch_bunny.core.AvailabilityQuerySolver import AvailabilityQuerySolver
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
from sqlalchemy.dialects import postgresql
from hutch_bunny.core.rquest_dto.query import AvailabilityQuery, DistributionQuery
from hutch_bunny.core.rquest_dto.file import File
from sqlalchemy import select, Select

from hutch_bunny.core.rquest_dto.result import RquestResult
from hutch_bunny.core.enums import DistributionQueryType
import hutch_bunny.core.settings as settings
from hutch_bunny.core.constants import DISTRIBUTION_TYPE_FILE_NAMES_MAP
from hutch_bunny.core.rquest_dto.rule import Rule


class BaseDistributionQuerySolver:
    def solve_query(self) -> Tuple[str, int]:
        raise NotImplementedError


# class for distribution queries
class CodeDistributionQuerySolver(BaseDistributionQuerySolver):
    # todo - can the following be placed somewhere once as its repeated for all classes handling queries
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

    def solve_query(self) -> Tuple[str, int]:
        """Build table of distribution query and return as a TAB separated string
        along with the number of rows.

        Returns:
            Tuple[str, int]: The table as a string and the number of rows.
        """
        # Prepare the empty results data frame
        df = pd.DataFrame(columns=self.output_cols)

        # Get the counts for each concept ID
        counts = list()
        concepts = list()
        categories = list()
        biobanks = list()
        omop_desc = list()

        with self.db_manager.engine.connect() as con:
            for domain_id in self.allowed_domains_map:
                # get the right table and column based on the domain
                table = self.allowed_domains_map[domain_id]
                concept_col = self.domain_concept_id_map[domain_id]

                # gets a list of all concepts within this given table and their respective counts
                stmnt = (
                    select(
                        func.count(table.person_id),
                        Concept.concept_id,
                        Concept.concept_name,
                    )
                    .join(Concept, concept_col == Concept.concept_id)
                    .group_by(Concept.concept_id, Concept.concept_name)
                )
                res = pd.read_sql(stmnt, con)
                counts.extend(res.iloc[:, 0])
                concepts.extend(res.iloc[:, 1])
                omop_desc.extend(res.iloc[:, 2])
                # add the same category and collection if, for the number of results received
                categories.extend([domain_id] * len(res))
                biobanks.extend([self.query.collection] * len(res))

        df["COUNT"] = counts
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


# todo - i *think* the only diferrence between this one and generic is that the allowed_domain list is different. Could we not just have the one class and functions that have this passed in?
class DemographicsDistributionQuerySolver(BaseDistributionQuerySolver):
    allowed_domains_map = {
        "Gender": Person,
    }
    domain_concept_id_map = {
        "Gender": Person.gender_concept_id,
    }
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

    def solve_query(self) -> Tuple[str, int]:
        """Build table of distribution query and return as a TAB separated string
        along with the number of rows.

        Returns:
            Tuple[str, int]: The table as a string and the number of rows.
        """
        # Prepare the empty results data frame
        df = pd.DataFrame(columns=self.output_cols)

        # Get the counts for each concept ID
        counts = list()
        concepts = list()
        categories = list()
        biobanks = list()
        datasets = list()
        codes = list()
        descriptions = list()
        alternatives = list()
        for k in self.allowed_domains_map:
            table = self.allowed_domains_map[k]
            concept_col = self.domain_concept_id_map[k]

            # People count statement
            stmnt = select(func.count(table.person_id), concept_col).group_by(
                concept_col
            )

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
                left_on=concept_col.name,
                right_on=Concept.concept_id.name,
                how="left",
            )

            # Compile the data
            counts.append(res.iloc[:, 0].sum())
            concepts.extend(res.iloc[:, 1])
            categories.append("DEMOGRAPHICS")
            biobanks.append(self.query.collection)
            datasets.append(table.__tablename__)
            descriptions.append(k)
            codes.append(k.upper())

            alternative = "^"
            for _, row in combined.iterrows():
                alternative += f"{row[Concept.concept_name.name]}|{row.iloc[0]}^"
            alternatives.append(alternative)

        # Fill out the results table
        df["COUNT"] = counts
        df["CATEGORY"] = categories
        df["CODE"] = codes
        df["BIOBANK"] = biobanks
        df["DATASET"] = datasets
        df["DESCRIPTION"] = descriptions
        df["ALTERNATIVES"] = alternatives

        # Convert df to tab separated string
        results = list(["\t".join(df.columns)])
        for _, row in df.iterrows():
            results.append("\t".join([str(r) for r in row.values]))

        return os.linesep.join(results), len(df)


def solve_availability(
    db_manager: SyncDBManager, query: AvailabilityQuery
) -> RquestResult:
    """Solve RQuest availability queries.

    Args:
        db_manager (SyncDBManager): The database manager
        query (AvailabilityQuery): The availability query object

    Returns:
        RquestResult: Result object for the query
    """
    logger = logging.getLogger(settings.LOGGER_NAME)
    solver = AvailabilityQuerySolver(db_manager, query)
    try:
        count_ = solver.solve_query()
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
    db_manager: SyncDBManager, query: DistributionQuery
) -> RquestResult:
    """Solve RQuest distribution queries.

    Args:
        db_manager (SyncDBManager): The database manager
        query (DistributionQuery): The distribution query object

    Returns:
        DistributionResult: Result object for the query
    """
    logger = logging.getLogger(settings.LOGGER_NAME)
    solver = _get_distribution_solver(db_manager, query)
    try:
        res, count = solver.solve_query()
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
