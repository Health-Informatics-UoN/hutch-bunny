import base64
import os
import logging
from typing import Tuple
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

import sqlalchemy
from mypy.checker import conditional_types
from sqlalchemy import (
    or_,
    select,
    func,
    extract,
    where,
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
from sqlalchemy.dialects import postgresql
from hutch_bunny.core.rquest_dto.query import AvailabilityQuery, DistributionQuery
from hutch_bunny.core.rquest_dto.file import File
from hutch_bunny.core.rquest_dto.rule import Rule
from hutch_bunny.core.rquest_dto.result import RquestResult
from hutch_bunny.core.enums import DistributionQueryType
import hutch_bunny.core.settings as settings
from hutch_bunny.core.constants import DISTRIBUTION_TYPE_FILE_NAMES_MAP


# Class for availability queries
class AvailibilityQuerySolver:
    subqueries = list()
    omop_domain_to_omop_table_map = {
        "Condition": ConditionOccurrence,
        "Ethnicity": Person,
        "Drug": DrugExposure,
        "Gender": Person,
        "Race": Person,
        "Measurement": Measurement,
        "Observation": Observation,
        "Procedure": ProcedureOccurrence,
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
        with self.db_manager.engine.connect() as con:
            concepts_df = pd.read_sql_query(
                concept_query, con=con
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
        the person was between a certain age. 
        
        This builds an SQL query to run as one for the whole query (was previous multiple) and it 
        returns an int for the result. Therefore, all dataframes have been removed. 

        """

    def _solve_rules(self) -> int:

        # get the list of concepts to build the query constraints
        concepts = self._find_concepts()

        logger = logging.getLogger(settings.LOGGER_NAME)

        with (self.db_manager.engine.connect() as con):

            # this is used to store the query for each group, one entry per group
            group_statement = list()

            # iterate through all the groups specified in the query
            for current_group in self.query.cohort.groups:

                # this is used to store all constraints for all rules in the group, one entry per rule
                list_for_rules = list()

                #captures all the person constraints for the group
                person_constraints = list()

                # for each rule in a group
                for rule_index, all_constraints_for_rule in enumerate(current_group.rules, start=0):

                    # a list for all the conditions for the rule, each rule generates searches
                    # in four tables, this field captures that
                    ruleConstraints = list()

                    # variables used to capture the relevent detail.
                    # "time" : "|1:TIME:M" in the payload means that
                    # if the | is on the left of the value it was less than 1 month
                    # if it was "1|:TIME:M" it would mean greater than one month
                    left_value_time = None
                    right_value_time = None

                    # if a time is supplied split the string out to component parts
                    if all_constraints_for_rule.time != "" and all_constraints_for_rule.time is not None:
                        time_value, time_category, time_unit = all_constraints_for_rule.time.split(":")
                        left_value_time, right_value_time = time_value.split("|")

                    # if a number was supplied, it is in the format "value" : "0.0|200.0"
                    # therefore split to capture min as 0 and max as 200
                    if all_constraints_for_rule.raw_range != "":
                        all_constraints_for_rule.min_value, all_constraints_for_rule.max_value = all_constraints_for_rule.raw_range.split("|")

                    # if the rule was not linked to a person variable
                    if all_constraints_for_rule.varcat != "Person": #i.e condition, observation, measurement or drug

                        # NOTE: Although the table is specified in the query, to cover for changes in vocabulary
                        # and for differencs in RQuest OMOP and local OMOP, we now search all four main tables
                        # for the presence of the concept. This is computationally more expensive, but is more r
                        # reliable longer term.

                        # initial setting for the four tables
                        condition: select = select(ConditionOccurrence.person_id)
                        drug: select = select(DrugExposure.person_id)
                        meas: select = select(Measurement.person_id)
                        obs: select = select(Observation.person_id)

                        """"
                        RELATIVE AGE SEARCH
                        """
                        # if there is an "Age" query added, this will require a join to the person table, to compare
                        # DOB with the data of event

                        if left_value_time is not None and (left_value_time != "" or right_value_time != "") and time_category == "AGE":
                            condition = condition.join(Person, Person.person_id == ConditionOccurrence.person_id)
                            drug = drug.join(Person, Person.person_id == DrugExposure.person_id)
                            meas = meas.join(Person, Person.person_id == Measurement.person_id)
                            obs = obs.join(Person, Person.person_id == Observation.person_id)

                            # due to the way the query is expressed and how split above, if the left value is empty
                            # it indicates a less than search
                            if left_value_time == "":
                                condition = condition.where(extract('year', ConditionOccurrence.condition_start_date) - extract('year',Person.birth_datetime) < int(right_value_time))
                                drug = drug.where(extract('year', DrugExposure.drug_exposure_start_date) - extract('year',Person.birth_datetime) < int(right_value_time))
                                meas = meas.where(extract('year', Measurement.measurement_date) - extract('year',Person.birth_datetime) < int(right_value_time))
                                obs = obs.where(extract('year', Observation.observation_date) - extract('year',Person.birth_datetime) < int(right_value_time))
                            else:
                                condition = condition.where(extract('year', ConditionOccurrence.condition_start_date) - extract('year',Person.birth_datetime) > int(left_value_time))
                                drug = drug.where(extract('year', DrugExposure.drug_exposure_start_date) - extract('year',Person.birth_datetime) > int(left_value_time))
                                meas = meas.where(extract('year', Measurement.measurement_date) - extract('year',Person.birth_datetime) > int(left_value_time))
                                obs = obs.where(extract('year', Observation.observation_date) - extract('year',Person.birth_datetime) > int(left_value_time))

                        """"
                        STANDARD CONCEPT ID SEARCH
                        """
                        if all_constraints_for_rule.operator == "=":
                            condition = condition.where(ConditionOccurrence.condition_concept_id == int(all_constraints_for_rule.value))
                            drug = drug.where(DrugExposure.drug_concept_id == int(all_constraints_for_rule.value))
                            meas = meas.where(Measurement.measurement_concept_id == int(all_constraints_for_rule.value))
                            obs = obs.where(Observation.observation_concept_id == int(all_constraints_for_rule.value))
                        else:
                            condition = condition.where(ConditionOccurrence.condition_concept_id != int(all_constraints_for_rule.value))
                            drug = drug.where(DrugExposure.drug_concept_id != int(all_constraints_for_rule.value))
                            meas = meas.where(Measurement.measurement_concept_id != int(all_constraints_for_rule.value))
                            obs = obs.where(Observation.observation_concept_id != int(all_constraints_for_rule.value))

                        """"
                        SECONDARY MODIFIER
                        """
                        # secondary modifier hits another field and only on the conditiion_occurrence
                        # on the RQuest GUI this is a list that can be created. Assuming this is also an
                        # AND condition for at least one of the selected values to be present
                        secondary_modifier_list = list()

                        # Not sure where, but even when a secondary modifier is not supplied, an array
                        # with a single entry is provided.
                        # todo: need to confirm if this is in the JSON from the API or our implementation
                        for type_index, typeAdd in enumerate(all_constraints_for_rule.secondary_modifier, start=0):
                            if (typeAdd!=""):
                                secondary_modifier_list.append(ConditionOccurrence.condition_type_concept_id == int(typeAdd))

                        if (len(secondary_modifier_list)>0):
                            condition = condition.where(or_(*secondary_modifier_list))

                        """"
                        VALUES AS NUMBER
                        """
                        if all_constraints_for_rule.min_value is not None and all_constraints_for_rule.max_value is not None:
                            meas = meas.where(
                                Measurement.value_as_number.between(float(all_constraints_for_rule.min_value), float(all_constraints_for_rule.max_value)))
                            obs = obs.where(
                                Observation.value_as_number.between(float(all_constraints_for_rule.min_value), float(all_constraints_for_rule.max_value)))
                        """"
                        RELATIVE TIME SEARCH SECTION
                        """
                        # this section deals with a relative time constraint, such as "time" : "|1:TIME:M"
                        if left_value_time is not None and (
                            left_value_time != "" or right_value_time != "") and time_category == "TIME":

                            time_value_supplied:str

                            # have to toggle between left and right, given |1 means less than 1 and
                            # 1| means greater than 1
                            if left_value_time == "":
                                time_value_supplied = right_value_time
                            else:
                                time_value_supplied = left_value_time

                            today_date = datetime.now()

                            # converting supplied time (stored as string) to int, and negating.
                            time_to_use: int = int(time_value_supplied)
                            time_to_use = time_to_use * -1

                            # the relative date to search on, is the current date minus
                            # the number of months supplied
                            relative_date = today_date + relativedelta(months=time_to_use)

                            # if the left value is blank, it means the original was |1 meaning
                            # "i want to find this event that occurred less than a month ago"
                            # therefore the logic is to search for a date that is after the date
                            # that was a month ago.
                            if left_value_time == "":
                                meas = meas.where(Measurement.measurement_date >= relative_date)
                                obs = obs.where(Observation.observation_date >= relative_date)
                                condition = condition.where(ConditionOccurrence.condition_start_date >= relative_date)
                                drug = drug.where(DrugExposure.drug_exposure_start_date >= relative_date)
                            else:
                                meas = meas.where(Measurement.measurement_date <= relative_date)
                                obs = obs.where(Observation.observation_date <= relative_date)
                                condition = condition.where(ConditionOccurrence.condition_start_date <= relative_date)
                                drug = drug.where(DrugExposure.drug_exposure_start_date <= relative_date)

                        """"
                        PREPARING THE LISTS FOR LATER USE
                        """
                        # improve
                        ruleConstraints.append(Person.person_id.in_(meas))
                        ruleConstraints.append(Person.person_id.in_(obs))
                        ruleConstraints.append(Person.person_id.in_(condition))
                        ruleConstraints.append(Person.person_id.in_(drug))

                        # all the constraints for this rule are added as a single list
                        # to the list which captures all rules for the group
                        list_for_rules.append(ruleConstraints)

                    else:
                        """
                        PERSON TABLE RELATED RULES
                        """

                        # this is unsupported currently
                        if all_constraints_for_rule.varname=="AGE":
                           logger.info("An unsupported rule for AGE was detected and ignored")
                           #nothing is done yet, but stops this causing problems
                        else:
                            concept_domain: str = concepts.get(all_constraints_for_rule.value)

                            if concept_domain == "Gender":
                                if all_constraints_for_rule.operator == "=":
                                    person_constraints.append(Person.gender_concept_id == int(all_constraints_for_rule.value))
                                else:
                                    person_constraints.append(Person.gender_concept_id != int(all_constraints_for_rule.value))

                            elif concept_domain == "Race":
                                if all_constraints_for_rule.operator == "=":
                                    person_constraints.append(Person.race_concept_id == int(all_constraints_for_rule.value))
                                else:
                                    person_constraints.append(Person.race_concept_id != int(all_constraints_for_rule.value))

                            elif concept_domain == "Ethnicity":
                                if all_constraints_for_rule.operator == "=":
                                    person_constraints.append(Person.ethnicity_concept_id == int(all_constraints_for_rule.value))
                                else:
                                    person_constraints.append(Person.ethnicity_concept_id != int(all_constraints_for_rule.value))


                """
                NOTE: all rules done for a group. Now to apply logic between the rules
                """

                ## if the logic between the rules for each group is AND
                if current_group.rules_operator == "AND":
                    # all person rules are added first
                    root_statement: select = select(Person.person_id).where(*person_constraints)

                    # although this is an AND, we include the top level as AND, but the
                    # sub-query is OR to account for searching in the four tables

                    for rule_index, all_constraints_for_rule in enumerate(list_for_rules, start=0):
                        root_statement: select = root_statement.where(or_(*all_constraints_for_rule))
                else:
                    # this might seem odd, but to add the rules as OR, we have to add them
                    # all at once, therefore listAllParameters is to create one list with
                    # everything added. So we can then add as one operation as OR
                    all_parameters = list()

                    #firstly add the person constrains
                    for rule_index, all_constraints_for_person in enumerate(person_constraints, start=0):
                        all_parameters.append(all_constraints_for_person)

                    # to get all the constraints in one list, we have to unpack the top-level grouping
                    # list_for_rules contains all the group of constraints for each rule
                    # therefore, we get each group, then for each group, we get each constraint
                    for rule_index, all_constraints_for_rule in enumerate(list_for_rules, start=0):
                        for constraint_index, current_constraint in enumerate(all_constraints_for_rule, start=0):
                            all_parameters.append(current_constraint)

                    # all added as an OR
                    root_statement = select(Person.person_id).where(or_(*all_parameters))

                #store the query for the given group in the list for assembly later across all groups
                group_statement.append(Person.person_id.in_(root_statement))

            """
            ALL GROUPS COMPLETED, NOW APPLY LOGIC BETWEEN GROUPS
            """

            # construct the query based on the OR or AND logic specified between groups
            if self.query.cohort.groups_operator == "OR":
                new_statement = select(func.count()).where(or_(*group_statement))
            else:
                new_statement = select(func.count()).where(*group_statement)

            # here for debug, prints the SQL statement created
            print(str(new_statement.compile(
                dialect=postgresql.dialect(),
                compile_kwargs={"literal_binds": True})))

            output = con.execute(new_statement).fetchone()

        return int(output[0])

    """ 
    This is the start of the process that begins to run the queries. 
    (1) call solve_rules that takes each group and adds those results to the sub_queries list 
    (2) this function then iterates through the list of groups to resolve the logic (AND/OR) between groups
    """

    def solve_query(self) -> int:
        # resolve within the group
        return self._solve_rules()


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


# todo - i *think* the only diference between this one and generic is that the allowed_domain list is different. Could we not just have the one class and functions that have this passed in?
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
                concepts_df = pd.read_sql_query(
                    concept_query, con=con
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
