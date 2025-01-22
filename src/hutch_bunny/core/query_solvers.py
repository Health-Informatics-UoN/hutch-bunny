import base64
import os
import logging
from typing import Tuple
import pandas as pd
from sqlalchemy import (
    and_,
    select,
    func,
)
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
from hutch_bunny.core.rquest_dto.result import RquestResult
from hutch_bunny.core.enums import DistributionQueryType
import hutch_bunny.core.settings as settings
from hutch_bunny.core.constants import DISTRIBUTION_TYPE_FILE_NAMES_MAP

# Class for availability queries
class AvailibilityQuerySolver:
    subqueries = list()
    concept_table_map = {
        "Condition": ConditionOccurrence,
        "Ethnicity": Person,
        "Drug": DrugExposure,
        "Gender": Person,
        "Race": Person,
        "Measurement": Measurement,
        "Observation": Observation,
        "Procedure": ProcedureOccurrence,
    }
    concept_time_column_map = {
        "Condition": ConditionOccurrence.condition_start_date,
        "Ethnicity": Person.birth_datetime,
        "Drug": DrugExposure.drug_exposure_start_date,
        "Gender": Person.birth_datetime,
        "Race": Person.birth_datetime,
        "Measurement": Measurement.measurement_date,
        "Observation": Observation.observation_date,
        "Procedure": ProcedureOccurrence.procedure_date,
    }
    numeric_rule_map = {
        "Measurement": Measurement.value_as_number,
        "Observation": Observation.value_as_number,
    }
    boolean_rule_map = {
        "Condition": ConditionOccurrence.condition_concept_id,
        "Ethnicity": Person.ethnicity_concept_id,
        "Drug": DrugExposure.drug_concept_id,
        "Gender": Person.gender_concept_id,
        "Race": Person.race_concept_id,
        "Measurement": Measurement.measurement_concept_id,
        "Observation": Observation.observation_concept_id,
        "Procedure": ProcedureOccurrence.procedure_concept_id,
    }

    def __init__(self, db_manager: SyncDBManager, query: AvailabilityQuery) -> None:
        self.db_manager = db_manager
        self.query = query

    """ Function that takes all the concept IDs in the cohort defintion, looks them up in the OMOP database 
    to extract the concept_id and domain and place this within a dictionary for lookup during other query building 
    
    Although the query payload will tell you where the OMOP concept is from (based on the RQUEST OMOP version, this is
    a safer method as we know concepts can move between tables based on a vocab. 
    
    Therefore this helps to account for a difference between the Bunny vocab version and the RQUEST OMOP version.
    

    #TODO: this does not cover the scenario that is possible to occur where the local vocab model may say the concept 
    should be based in one table but it is actually present in another

    """
    def _find_concepts(self) -> dict:
        concept_ids = set()
        for group in self.query.cohort.groups:
            for rule in group.rules:
                concept_ids.add(int(rule.value))

        concept_query = (
            # order must be .concept_id, .domain_id
            select(Concept.concept_id, Concept.domain_id)
            .where(Concept.concept_id.in_(concept_ids))
            .distinct()
        )
        concepts_df = pd.read_sql_query(
            concept_query, con=self.db_manager.engine.connect()
        )
        concept_dict = {
            str(concept_id): domain_id for concept_id, domain_id in concepts_df.values
        }
        return concept_dict

    """ Function for taking the JSON query from RQUEST and creating the required query to run against the OMOP database.
    
        RQUEST API spec can have multiple groups in each query, and then a condition between the groups. 
        
        Each group can have conditional logic AND/OR within the group
        
        Each concept can either be an inclusion or exclusion criteria. 
        
        Each concept can have an age set, so it is that this event with concept X occurred when 
        the person was between a certain age. - #TODO - not sure this is implemented here

        """
    def _solve_rules(self) -> None:

        #get the list of concepts to build the query constraints
        concepts = self._find_concepts()

        # This is related to the logic within a group. This is used in the subsequent for loop to determine how
        # the merge should be applied.
        merge_method = lambda x: "inner" if x == "AND" else "outer"

        # iterate through all the groups specified in the query
        for group in self.query.cohort.groups:

            # todo - refactor variable name concept as this is misleading. It is not the concept but actually the domain of the concept
            # this passes in the conceptID of but gets back the domain related to that concept.
            concept = concepts.get(group.rules[0].value)

            concept_table = self.concept_table_map.get(concept)
            boolean_rule_col = self.boolean_rule_map.get(concept)
            numeric_rule_col = self.numeric_rule_map.get(concept)

            #within the query, if a range was specified, which is currently
            if (
                group.rules[0].min_value is not None
                and group.rules[0].max_value is not None
            ):
                stmnt = (
                    select(concept_table.person_id)
                    .where(
                        and_(
                            boolean_rule_col == int(group.rules[0].value),
                            numeric_rule_col.between(
                                group.rules[0].min_value, group.rules[0].max_value
                            ),
                        )
                    )
                    .distinct()
                )
                main_df = pd.read_sql_query(
                    sql=stmnt, con=self.db_manager.engine.connect()
                )

            # the next two ifs are basically switching between equals and not equals. These could be merged with a simple
            # switch for the operator.

            elif group.rules[0].operator == "=":
                stmnt = (
                    select(concept_table.person_id)
                    .where(boolean_rule_col == int(group.rules[0].value))
                    .distinct()
                )
                main_df = pd.read_sql_query(
                    sql=stmnt, con=self.db_manager.engine.connect()
                )
            elif group.rules[0].operator == "!=":
                stmnt = (
                    select(concept_table.person_id)
                    .where(boolean_rule_col != int(group.rules[0].value))
                    .distinct()
                )
                main_df = pd.read_sql_query(
                    sql=stmnt, con=self.db_manager.engine.connect()
                )

            """
            Now that the main_df dataframe has been populated, the subsequent queries are created and merged into 
            main_df dataframe. That is why above the first concept is hard coded as accessing index 0 and why the for 
            loop below if start at index 1. The queries are almost identical to the above, exact same logic but 
            in order to facilitate the merging, a label is created on person id, so that the newly created data frame 
            can be merged with main_df via unique keys. 
            """

            for i, rule in enumerate(group.rules[1:], start=1):

                # todo - refactor variable name concept as this is misleading. It is not the concept but actually the domain of the concept
                # this passes in the conceptID of but gets back the domain related to that concept.
                concept = concepts.get(rule.value)

                concept_table = self.concept_table_map.get(concept)
                boolean_rule_col = self.boolean_rule_map.get(concept)
                numeric_rule_col = self.numeric_rule_map.get(concept)

                if rule.min_value is not None and rule.max_value is not None:
                    # numeric rule
                    stmnt = (
                        select(concept_table.person_id.label(f"person_id_{i}"))
                        .where(
                            and_(
                                boolean_rule_col == int(rule.value),
                                numeric_rule_col.between(
                                    rule.min_value, rule.max_value
                                ),
                            )
                        )
                        .distinct()
                    )
                    rule_df = pd.read_sql_query(
                        sql=stmnt, con=self.db_manager.engine.connect()
                    )
                    main_df = main_df.merge(
                        right=rule_df,
                        how=merge_method(group.rules_operator),
                        left_on="person_id",
                        right_on=f"person_id_{i}",
                    )
                # Text rules testing for inclusion
                elif rule.operator == "=":
                    stmnt = (
                        select(concept_table.person_id.label(f"person_id_{i}"))
                        .where(boolean_rule_col == int(rule.value))
                        .distinct()
                    )
                    rule_df = pd.read_sql_query(
                        sql=stmnt, con=self.db_manager.engine.connect()
                    )
                    main_df = main_df.merge(
                        right=rule_df,
                        how=merge_method(group.rules_operator),
                        left_on="person_id",
                        right_on=f"person_id_{i}",
                    )
                # Text rules testing for exclusion
                elif rule.operator == "!=":
                    stmnt = (
                        select(concept_table.person_id.label(f"person_id_{i}"))
                        .where(boolean_rule_col != int(rule.value))
                        .distinct()
                    )
                    rule_df = pd.read_sql_query(
                        sql=stmnt, con=self.db_manager.engine.connect()
                    )
                    main_df = main_df.merge(
                        right=rule_df,
                        how=merge_method(group.rules_operator),
                        left_on="person_id",
                        right_on=f"person_id_{i}",
                    )
            # subqueries therefore contain the results for each group within the cohort definition.
            self.subqueries.append(main_df)

    """ 
    This is the start of the process that begins to run the queries. 
    (1) call solve_rules that takes each group and adds those results to the sub_queries list 
    (2) this function then iterates through the list of groups to resolve the logic (AND/OR) between groups
    """
    def solve_query(self) -> int:
        #resolve within the group
        self._solve_rules()

        merge_method = lambda x: "inner" if x == "AND" else "outer"

        #seed the dataframe with the first
        group0_df = self.subqueries[0]
        group0_df.rename({"person_id": "person_id_0"}, inplace=True, axis=1)

        #for the next, rename columns to give a unique key, then merge based on the merge_method value
        for i, df in enumerate(self.subqueries[1:], start=1):
            df.rename({"person_id": f"person_id_{i}"}, inplace=True, axis=1)
            group0_df = group0_df.merge(
                right=df,
                how=merge_method(self.query.cohort.groups_operator),
                left_on="person_id_0",
                right_on=f"person_id_{i}",
            )
        self.subqueries.clear()
        return group0_df.shape[0]  # the number of rows


class BaseDistributionQuerySolver:
    def solve_query(self) -> Tuple[str, int]:
        raise NotImplementedError

# class for distriubtion queries
class CodeDistributionQuerySolver(BaseDistributionQuerySolver):
    #todo - can the following be placed somewhere once as its repeated for all classes handling queries
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

        #todo - rename k, as this is a domain id that is being used
        for k in self.allowed_domains_map:

            # get the right table and column based on the domain
            table = self.allowed_domains_map[k]
            concept_col = self.domain_concept_id_map[k]

            # gets a list of all concepts within this given table and their respective counts
            stmnt = select(func.count(table.person_id), concept_col).group_by(
                concept_col
            )
            res = pd.read_sql(stmnt, self.db_manager.engine.connect())
            counts.extend(res.iloc[:, 0])
            concepts.extend(res.iloc[:, 1])

            # add the same category and collection if, for the number of results received
            categories.extend([k] * len(res))
            biobanks.extend([self.query.collection] * len(res))

        df["COUNT"] = counts
        df["OMOP"] = concepts
        df["CATEGORY"] = categories
        df["CODE"] = df["OMOP"].apply(lambda x: f"OMOP:{x}")
        df["BIOBANK"] = biobanks

        # Get descriptions
        #todo - not sure why this can be included in the SQL output above, it would need a join to the concept table
        concept_query = select(Concept.concept_id, Concept.concept_name).where(
            Concept.concept_id.in_(concepts)
        )
        concepts_df = pd.read_sql_query(
            concept_query, con=self.db_manager.engine.connect()
        )
        for _, row in concepts_df.iterrows():
            df.loc[df["OMOP"] == row["concept_id"], "OMOP_DESCR"] = row["concept_name"]

        # Convert df to tab separated string
        results = list(["\t".join(df.columns)])
        for _, row in df.iterrows():
            results.append("\t".join([str(r) for r in row.values]))

        return os.linesep.join(results), len(df)

#todo - i *think* the only diference between this one and generic is that the allowed_domain list is different. Could we not just have the one class and functions that have this passed in?
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
            res = pd.read_sql(stmnt, self.db_manager.engine.connect())
            concepts_df = pd.read_sql_query(
                concept_query, con=self.db_manager.engine.connect()
            )
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
    solver = AvailibilityQuerySolver(db_manager, query)
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
            description="Result of code.distribution anaylsis",
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
