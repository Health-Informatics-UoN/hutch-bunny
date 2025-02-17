import logging
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

from sqlalchemy import or_, func, BinaryExpression, ColumnElement
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

from hutch_bunny.core.obfuscation import apply_filters
from hutch_bunny.core.rquest_dto.query import AvailabilityQuery
from sqlalchemy import select, Select
from sqlalchemy.engine import Engine

import hutch_bunny.core.settings as settings
from hutch_bunny.core.rquest_dto.rule import Rule


# Class for availability queries
class AvailabilitySolver:
    measurement: Select
    drug: Select
    condition: Select
    observation: Select

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

    def solve_query(self, results_modifier: list[dict]) -> int:
        """
        This is the start of the process that begins to run the queries.
        (1) call solve_rules that takes each group and adds those results to the sub_queries list
        (2) this function then iterates through the list of groups to resolve the logic (AND/OR) between groups
        """
        # resolve within the group
        return self._solve_rules(results_modifier)

    def _find_concepts(self) -> dict:
        """Function that takes all the concept IDs in the cohort definition, looks them up in the OMOP database
        to extract the concept_id and domain and place this within a dictionary for lookup during other query building

        Although the query payload will tell you where the OMOP concept is from (based on the RQUEST OMOP version, this is
        a safer method as we know concepts can move between tables based on a vocab.

        Therefore, this helps to account for a difference between the Bunny vocab version and the RQUEST OMOP version.

        """
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
            concepts_df = pd.read_sql_query(concept_query, con=con)
        concept_dict = {
            str(concept_id): domain_id for concept_id, domain_id in concepts_df.values
        }
        return concept_dict

    def _solve_rules(self, results_modifier: list[dict]) -> int:
        """Function for taking the JSON query from RQUEST and creating the required query to run against the OMOP database.

        RQUEST API spec can have multiple groups in each query, and then a condition between the groups.

        Each group can have conditional logic AND/OR within the group

        Each concept can either be an inclusion or exclusion criteria.

        Each concept can have an age set, so it is that this event with concept X occurred when
        the person was between a certain age.

        This builds an SQL query to run as one for the whole query (was previous multiple) and it
        returns an int for the result. Therefore, all dataframes have been removed.

        """
        # get the list of concepts to build the query constraints
        concepts: dict = self._find_concepts()

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

        logger = logging.getLogger(settings.LOGGER_NAME)

        with self.db_manager.engine.connect() as con:
            # this is used to store the query for each group, one entry per group
            all_groups_queries: list[BinaryExpression[bool]] = []

            # iterate through all the groups specified in the query
            for current_group in self.query.cohort.groups:
                # this is used to store all constraints for all rules in the group, one entry per rule
                list_for_rules: list[list[BinaryExpression[bool]]] = []

                # captures all the person constraints for the group
                person_constraints_for_group: list[ColumnElement[bool]] = []

                # for each rule in a group
                for current_rule in current_group.rules:
                    # a list for all the conditions for the rule, each rule generates searches
                    # in four tables, this field captures that
                    rule_constraints: list[BinaryExpression[bool]] = []

                    # variables used to capture the relevant detail.
                    # "time" : "|1:TIME:M" in the payload means that
                    # if the | is on the left of the value it was less than 1 month
                    # if it was "1|:TIME:M" it would mean greater than one month
                    left_value_time: str | None = None
                    right_value_time: str | None = None

                    # if a time is supplied split the string out to component parts
                    if current_rule.time:
                        time_value, time_category, _ = current_rule.time.split(":")
                        left_value_time, right_value_time = time_value.split("|")

                    # if a number was supplied, it is in the format "value" : "0.0|200.0"
                    # therefore split to capture min as 0 and max as 200
                    if current_rule.raw_range != "":
                        current_rule.min_value, current_rule.max_value = (
                            current_rule.raw_range.split("|")
                        )

                    # if the rule was not linked to a person variable
                    if current_rule.varcat != "Person":
                        # i.e. condition, observation, measurement or drug
                        # NOTE: Although the table is specified in the query, to cover for changes in vocabulary
                        # and for differences in RQuest OMOP and local OMOP, we now search all four main tables
                        # for the presence of the concept. This is computationally more expensive, but is more r
                        # reliable longer term.

                        # initial setting for the four tables
                        self.condition: Select = select(ConditionOccurrence.person_id)
                        self.drug: Select = select(DrugExposure.person_id)
                        self.measurement: Select = select(Measurement.person_id)
                        self.observation: Select = select(Observation.person_id)

                        """"
                        RELATIVE AGE SEARCH
                        """
                        # if there is an "Age" query added, this will require a join to the person table, to compare
                        # DOB with the data of event

                        if (
                            left_value_time is not None or right_value_time is not None
                        ) and time_category == "AGE":
                            self._add_age_constraints(left_value_time, right_value_time)

                        """"
                        STANDARD CONCEPT ID SEARCH
                        """

                        self._add_standard_concept(current_rule)

                        """"
                        SECONDARY MODIFIER
                        """
                        # secondary modifier hits another field and only on the condition_occurrence
                        # on the RQuest GUI this is a list that can be created. Assuming this is also an
                        # AND condition for at least one of the selected values to be present
                        self._add_secondary_modifiers(current_rule)

                        """"
                        VALUES AS NUMBER
                        """
                        self._add_range_as_number(current_rule)

                        """"
                        RELATIVE TIME SEARCH SECTION
                        """
                        # this section deals with a relative time constraint, such as "time" : "|1:TIME:M"
                        if (
                            left_value_time is not None
                            and (left_value_time != "" or right_value_time != "")
                            and time_category == "TIME"
                        ):
                            self._add_relative_date(left_value_time, right_value_time)

                        """"
                        PREPARING THE LISTS FOR LATER USE
                        """
                        rule_constraints.append(Person.person_id.in_(self.measurement))
                        rule_constraints.append(Person.person_id.in_(self.observation))
                        rule_constraints.append(Person.person_id.in_(self.condition))
                        rule_constraints.append(Person.person_id.in_(self.drug))

                        # all the constraints for this rule are added as a single list
                        # to the list which captures all rules for the group
                        list_for_rules.append(rule_constraints)

                    else:
                        """
                        PERSON TABLE RELATED RULES
                        """
                        # this is unsupported currently
                        if current_rule.varname == "AGE":
                            logger.info(
                                "An unsupported rule for AGE was detected and ignored"
                            )
                            # nothing is done yet, but stops this causing problems
                        else:
                            person_constraints_for_group = self._add_person_constraints(
                                person_constraints_for_group, current_rule, concepts
                            )

                """
                NOTE: all rules done for a single group. Now to apply logic between the rules
                """

                ## if the logic between the rules for each group is AND
                if current_group.rules_operator == "AND":
                    # all person rules are added first
                    group_query: Select = select(Person.person_id).where(
                        *person_constraints_for_group
                    )

                    # although this is an AND, we include the top level as AND, but the
                    # sub-query is OR to account for searching in the four tables

                    for current_constraint in list_for_rules:
                        group_query = group_query.where(or_(*current_constraint))

                else:
                    # this might seem odd, but to add the rules as OR, we have to add them
                    # all at once, therefore listAllParameters is to create one list with
                    # everything added. So we can then add as one operation as OR
                    all_parameters = []

                    # firstly add the person constrains
                    for all_constraints_for_person in person_constraints_for_group:
                        all_parameters.append(all_constraints_for_person)

                    # to get all the constraints in one list, we have to unpack the top-level grouping
                    # list_for_rules contains all the group of constraints for each rule
                    # therefore, we get each group, then for each group, we get each constraint
                    for current_expression in list_for_rules:
                        for current_constraint_from in current_expression:
                            all_parameters.append(current_constraint_from)

                    # all added as an OR
                    group_query = select(Person.person_id).where(or_(*all_parameters))

                # store the query for the given group in the list for assembly later across all groups
                all_groups_queries.append(Person.person_id.in_(group_query))

            """
            ALL GROUPS COMPLETED, NOW APPLY LOGIC BETWEEN GROUPS
            """

            # construct the query based on the OR/AND logic specified between groups
            if self.query.cohort.groups_operator == "OR":
                if rounding > 0:
                    full_query_all_groups = select(
                        func.round((func.count() / rounding)) * rounding
                    ).where(or_(*all_groups_queries))
                else:
                    full_query_all_groups = select(func.count()).where(
                        or_(*all_groups_queries)
                    )
            else:
                if rounding > 0:
                    full_query_all_groups = select(
                        func.round((func.count() / rounding)) * rounding
                    ).where(*all_groups_queries)
                else:
                    full_query_all_groups = select(func.count()).where(
                        *all_groups_queries
                    )

            if low_number > 0:
                full_query_all_groups = full_query_all_groups.having(
                    func.count() > low_number
                )

            # here for debug, prints the SQL statement created
            logger.debug(
                str(
                    full_query_all_groups.compile(
                        dialect=postgresql.dialect(),
                        compile_kwargs={"literal_binds": True},
                    )
                )
            )

            output = con.execute(full_query_all_groups).fetchone()

        return apply_filters(int(output[0]), results_modifier)

    def _add_range_as_number(self, current_rule: Rule):
        if current_rule.min_value is not None and current_rule.max_value is not None:
            self.measurement = self.measurement.where(
                Measurement.value_as_number.between(
                    float(current_rule.min_value), float(current_rule.max_value)
                )
            )
            self.observation = self.observation.where(
                Observation.value_as_number.between(
                    float(current_rule.min_value), float(current_rule.max_value)
                )
            )

    def _add_age_constraints(self, left_value_time: str, right_value_time: str):
        self.condition = self.condition.join(
            Person, Person.person_id == ConditionOccurrence.person_id
        )
        self.drug = self.drug.join(Person, Person.person_id == DrugExposure.person_id)
        self.measurement = self.measurement.join(
            Person, Person.person_id == Measurement.person_id
        )
        self.observation = self.observation.join(
            Person, Person.person_id == Observation.person_id
        )

        # due to the way the query is expressed and how split above, if the left value is empty
        # it indicates a less than search

        if left_value_time == "":
            self.condition = self.condition.where(
                self._get_year_difference(
                    self.db_manager.engine,
                    ConditionOccurrence.condition_start_date,
                    Person.birth_datetime,
                )
                < int(right_value_time)
            )
            self.drug = self.drug.where(
                self._get_year_difference(
                    self.db_manager.engine,
                    DrugExposure.drug_exposure_start_date,
                    Person.birth_datetime,
                )
                < int(right_value_time)
            )
            self.measurement = self.measurement.where(
                self._get_year_difference(
                    self.db_manager.engine,
                    Measurement.measurement_date,
                    Person.birth_datetime,
                )
                < int(right_value_time)
            )
            self.observation = self.observation.where(
                self._get_year_difference(
                    self.db_manager.engine,
                    Observation.observation_date,
                    Person.birth_datetime,
                )
                < int(right_value_time)
            )
        else:
            self.condition = self.condition.where(
                self._get_year_difference(
                    self.db_manager.engine,
                    ConditionOccurrence.condition_start_date,
                    Person.birth_datetime,
                )
                > int(left_value_time)
            )
            self.drug = self.drug.where(
                self._get_year_difference(
                    self.db_manager.engine,
                    DrugExposure.drug_exposure_start_date,
                    Person.birth_datetime,
                )
                > int(left_value_time)
            )
            self.measurement = self.measurement.where(
                self._get_year_difference(
                    self.db_manager.engine,
                    Measurement.measurement_date,
                    Person.birth_datetime,
                )
                > int(left_value_time)
            )
            self.observation = self.observation.where(
                self._get_year_difference(
                    self.db_manager.engine,
                    Observation.observation_date,
                    Person.birth_datetime,
                )
                > int(left_value_time)
            )

    def _get_year_difference(self, engine: Engine, start_date, birth_date):
        if engine.dialect.name == "postgresql":
            return func.date_part("year", start_date) - func.date_part(
                "year", birth_date
            )
        elif engine.dialect.name == "mssql":
            return func.DATEPART("year", start_date) - func.DATEPART("year", birth_date)
        else:
            raise NotImplementedError("Unsupported database dialect")

    def _add_relative_date(self, left_value_time: str, right_value_time: str):
        time_value_supplied: str

        # have to toggle between left and right, given |1 means less than 1 and
        # 1| means greater than 1
        if left_value_time == "":
            time_value_supplied = right_value_time
        else:
            time_value_supplied = left_value_time
        # converting supplied time (in months) (stored as string) to int, and negating.
        time_to_use: int = int(time_value_supplied)
        time_to_use = time_to_use * -1

        # the relative date to search on, is the current date minus
        # the number of months supplied
        today_date: datetime = datetime.now()
        relative_date = today_date + relativedelta(months=time_to_use)

        # if the left value is blank, it means the original was |1 meaning
        # "I want to find this event that occurred less than a month ago"
        # therefore the logic is to search for a date that is after the date
        # that was a month ago.
        if left_value_time == "":
            self.measurement = self.measurement.where(
                Measurement.measurement_date >= relative_date
            )
            self.observation = self.observation.where(
                Observation.observation_date >= relative_date
            )
            self.condition = self.condition.where(
                ConditionOccurrence.condition_start_date >= relative_date
            )
            self.drug = self.drug.where(
                DrugExposure.drug_exposure_start_date >= relative_date
            )
        else:
            self.measurement = self.measurement.where(
                Measurement.measurement_date <= relative_date
            )
            self.observation = self.observation.where(
                Observation.observation_date <= relative_date
            )
            self.condition = self.condition.where(
                ConditionOccurrence.condition_start_date <= relative_date
            )
            self.drug = self.drug.where(
                DrugExposure.drug_exposure_start_date <= relative_date
            )

    def _add_person_constraints(
        self, person_constraints_for_group, current_rule: Rule, concepts
    ):
        concept_domain: str = concepts.get(current_rule.value)

        if concept_domain == "Gender":
            if current_rule.operator == "=":
                person_constraints_for_group.append(
                    Person.gender_concept_id == int(current_rule.value)
                )
            else:
                person_constraints_for_group.append(
                    Person.gender_concept_id != int(current_rule.value)
                )

        elif concept_domain == "Race":
            if current_rule.operator == "=":
                person_constraints_for_group.append(
                    Person.race_concept_id == int(current_rule.value)
                )
            else:
                person_constraints_for_group.append(
                    Person.race_concept_id != int(current_rule.value)
                )

        elif concept_domain == "Ethnicity":
            if current_rule.operator == "=":
                person_constraints_for_group.append(
                    Person.ethnicity_concept_id == int(current_rule.value)
                )
            else:
                person_constraints_for_group.append(
                    Person.ethnicity_concept_id != int(current_rule.value)
                )

        return person_constraints_for_group

    def _add_secondary_modifiers(self, current_rule: Rule):
        # Not sure where, but even when a secondary modifier is not supplied, an array
        # with a single entry is provided.
        # todo: need to confirm if this is in the JSON from the API or our implementation

        secondary_modifier_list = []

        for type_index, typeAdd in enumerate(current_rule.secondary_modifier, start=0):
            if typeAdd != "":
                secondary_modifier_list.append(
                    ConditionOccurrence.condition_type_concept_id == int(typeAdd)
                )

        if len(secondary_modifier_list) > 0:
            self.condition = self.condition.where(or_(*secondary_modifier_list))

    def _add_standard_concept(self, current_rule: Rule):
        if current_rule.operator == "=":
            self.condition = self.condition.where(
                ConditionOccurrence.condition_concept_id == int(current_rule.value)
            )
            self.drug = self.drug.where(
                DrugExposure.drug_concept_id == int(current_rule.value)
            )
            self.measurement = self.measurement.where(
                Measurement.measurement_concept_id == int(current_rule.value)
            )
            self.observation = self.observation.where(
                Observation.observation_concept_id == int(current_rule.value)
            )
        else:
            self.condition = self.condition.where(
                ConditionOccurrence.condition_concept_id != int(current_rule.value)
            )
            self.drug = self.drug.where(
                DrugExposure.drug_concept_id != int(current_rule.value)
            )
            self.measurement = self.measurement.where(
                Measurement.measurement_concept_id != int(current_rule.value)
            )
            self.observation = self.observation.where(
                Observation.observation_concept_id != int(current_rule.value)
            )
