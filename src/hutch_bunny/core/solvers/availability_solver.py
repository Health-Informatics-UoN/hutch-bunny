import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Any, Callable, TypedDict
from sqlalchemy.sql.expression import ClauseElement
from sqlalchemy import (
    or_,
    and_,
    func,
    BinaryExpression,
    ColumnElement,
    select,
    Select,
    text,
    Exists,
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
from tenacity import (
    retry,
    stop_after_attempt,
    wait_fixed,
    before_sleep_log,
    after_log,
)

from typing import Tuple
from sqlalchemy import exists

from hutch_bunny.core.obfuscation import apply_filters
from hutch_bunny.core.rquest_models.group import Group
from hutch_bunny.core.rquest_models.availability import AvailabilityQuery
from sqlalchemy.engine import Engine
from hutch_bunny.core.logger import logger, INFO

from hutch_bunny.core.settings import Settings
from hutch_bunny.core.rquest_models.rule import Rule
import operator as op


class ResultModifier(TypedDict):
    id: str
    threshold: int | None
    nearest: int | None


settings = Settings()


# Class for availability queries
class AvailabilitySolver:
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

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(60),
        before_sleep=before_sleep_log(logger, INFO),
        after=after_log(logger, INFO),
    )
    def solve_query(self, results_modifier: list[ResultModifier]) -> int:
        """
        This is the start of the process that begins to run the queries.
        (1) call solve_rules that takes each group and adds those results to the sub_queries list
        (2) this function then iterates through the list of groups to resolve the logic (AND/OR) between groups
        """
        # resolve within the group
        return self._solve_rules(results_modifier)

    def _find_concepts(self, groups: list[Group]) -> dict[str, str]:
        """Function that takes all the concept IDs in the cohort definition, looks them up in the OMOP database
        to extract the concept_id and domain and place this within a dictionary for lookup during other query building

        Although the query payload will tell you where the OMOP concept is from (based on the RQUEST OMOP version, this is
        a safer method as we know concepts can move between tables based on a vocab.

        Therefore, this helps to account for a difference between the Bunny vocab version and the RQUEST OMOP version.

        """
        concept_ids = set()
        for group in groups:
            for rule in group.rules:
                # Guard for None values (e.g. Age)
                if rule.value:
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

    def _solve_rules(self, results_modifier: list[ResultModifier]) -> int:
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
        concepts: dict[str, str] = self._find_concepts(self.query.cohort.groups)

        low_number: int = next(
            (
                item["threshold"] if item["threshold"] is not None else 0
                for item in results_modifier
                if item["id"] == "Low Number Suppression"
            ),
            0,
        )

        rounding: int = next(
            (
                item["nearest"] if item["nearest"] is not None else 0
                for item in results_modifier
                if item["id"] == "Rounding"
            ),
            0,
        )

        with self.db_manager.engine.connect() as con:
            # this is used to store the query for each group, one entry per group
            all_groups_queries: list[BinaryExpression[bool]] = []

            # iterate through all the groups specified in the query
            for current_group in self.query.cohort.groups:
                # this is used to store all constraints for all rules in the group, one entry per rule
                list_for_rules: list[ColumnElement[bool]] = []

                # captures all the person constraints for the group
                person_constraints_for_group: list[ColumnElement[bool]] = []

                # for each rule in a group
                for current_rule in current_group.rules:
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
                    if current_rule.raw_range and current_rule.raw_range != "":
                        min_str, max_str = current_rule.raw_range.split("|")
                        current_rule.min_value = float(min_str) if min_str else None
                        current_rule.max_value = float(max_str) if max_str else None

                    # if the rule was not linked to a person variable
                    if current_rule.varcat != "Person":
                        # i.e. condition, observation, measurement or drug
                        # NOTE: Although the table is specified in the query, to cover for changes in vocabulary
                        # and for differences in RQuest OMOP and local OMOP, we now search all four main tables
                        # for the presence of the concept. This is computationally more expensive, but is more r
                        # reliable longer term.

                        # initial setting for the four tables
                        self.condition: Select[Tuple[int]] = select(
                            ConditionOccurrence.person_id
                        )
                        self.drug: Select[Tuple[int]] = select(DrugExposure.person_id)
                        self.measurement: Select[Tuple[int]] = select(
                            Measurement.person_id
                        )
                        self.observation: Select[Tuple[int]] = select(
                            Observation.person_id
                        )

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
                            and right_value_time is not None
                            and (left_value_time != "" or right_value_time != "")
                            and time_category == "TIME"
                        ):
                            self._add_relative_date(left_value_time, right_value_time)

                        """"
                        PREPARING THE LISTS FOR LATER USE
                        """

                        # List of tables and their corresponding foreign keys
                        table_constraints = [
                            (self.measurement, Measurement.person_id),
                            (self.observation, Observation.person_id),
                            (self.condition, ConditionOccurrence.person_id),
                            (self.drug, DrugExposure.person_id),
                        ]

                        # a switch between whether the criteria are inclusion or exclusion
                        inclusion_criteria: bool = current_rule.operator == "="

                        # a list for all the conditions for the rule, each rule generates searches
                        # in four tables, this field captures that
                        table_rule_constraints: list[ColumnElement[bool]] = []

                        for table, fk in table_constraints:
                            constraint: Exists = exists(
                                table.where(fk == Person.person_id)
                            )
                            table_rule_constraints.append(
                                constraint if inclusion_criteria else ~constraint
                            )

                        if inclusion_criteria:
                            list_for_rules.append(or_(*table_rule_constraints))
                        else:
                            list_for_rules.append(and_(*table_rule_constraints))

                    else:
                        """
                        PERSON TABLE RELATED RULES
                        """
                        person_constraints_for_group = self._add_person_constraints(
                            person_constraints_for_group, current_rule, concepts
                        )

                """
                NOTE: all rules done for a single group. Now to apply logic between the rules
                """

                ## if the logic between the rules for each group is AND
                if current_group.rules_operator == "AND":
                    # all person rules are added first
                    group_query: Select[Tuple[int]] = select(Person.person_id).where(
                        *person_constraints_for_group
                    )

                    for current_constraint in list_for_rules:
                        group_query = group_query.where(current_constraint)

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
                        all_parameters.append(current_expression)

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
                        func.round((func.count() / rounding), 0) * rounding
                    ).where(or_(*all_groups_queries))
                else:
                    full_query_all_groups = select(func.count()).where(
                        or_(*all_groups_queries)
                    )
            else:
                if rounding > 0:
                    full_query_all_groups = select(
                        func.round((func.count() / rounding), 0) * rounding
                    ).where(*all_groups_queries)
                else:
                    full_query_all_groups = select(func.count()).where(
                        *all_groups_queries
                    )

            if low_number > 0:
                full_query_all_groups = full_query_all_groups.having(
                    func.count() >= low_number
                )

            # here for debug, prints the SQL statement created
            logger.debug(
                str(
                    full_query_all_groups.compile(
                        dialect=self.db_manager.engine.dialect,
                        compile_kwargs={"literal_binds": True},
                    )
                )
            )

            output = con.execute(full_query_all_groups).fetchone()
            count = int(output[0]) if output is not None else 0

        return apply_filters(count, results_modifier)

    def _add_range_as_number(self, current_rule: Rule) -> None:
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

    def _add_age_constraints(
        self, left_value_time: str | None, right_value_time: str | None
    ) -> None:
        """
        This function adds age constraints to the query.
        If the left value is empty it indicates a less than search.

        Args:
            left_value_time: The left value of the time constraint.
            right_value_time: The right value of the time constraint.

        Returns:
            None
        """
        if left_value_time is None or right_value_time is None:
            return

        if left_value_time == "":
            comparator = op.lt
            age_value = int(right_value_time)
        else:
            comparator = op.gt
            age_value = int(left_value_time)

        self.condition = self._apply_age_constraint_to_table(
            self.condition,
            ConditionOccurrence.person_id,
            ConditionOccurrence.condition_start_datetime,
            comparator,
            age_value,
        )
        self.drug = self._apply_age_constraint_to_table(
            self.drug,
            DrugExposure.person_id,
            DrugExposure.drug_exposure_start_date,
            comparator,
            age_value,
        )
        self.measurement = self._apply_age_constraint_to_table(
            self.measurement,
            Measurement.person_id,
            Measurement.measurement_date,
            comparator,
            age_value,
        )
        self.observation = self._apply_age_constraint_to_table(
            self.observation,
            Observation.person_id,
            Observation.observation_date,
            comparator,
            age_value,
        )

    def _apply_age_constraint_to_table(
        self,
        table_query: Select[Tuple[int]],
        table_person_id: ClauseElement,
        table_date_column: ClauseElement,
        operator_func: Callable[[Any, Any], BinaryExpression[bool]],
        age_value: int,
    ) -> Select[Tuple[int]]:
        """
        Helper method to apply age constraints to a table query.

        Args:
            table_query: The table query to apply the age constraint to.
            table_person_id: The person_id column in the table.
            table_date_column: The date column in the table.
            operator_func: The operator function to use in the constraint.
            age_value: The age value to use in the constraint.

        Returns:
            The table query with the age constraint applied.
        """
        age_difference = self._get_year_difference(
            self.db_manager.engine, table_date_column, Person.birth_datetime
        )

        constraint = operator_func(age_difference, age_value)

        return table_query.where(
            exists(
                select(1).where(
                    and_(
                        Person.person_id == table_person_id,
                        constraint,
                    )
                )
            )
        )

    def _get_year_difference(
        self, engine: Engine, start_date: ClauseElement, birth_date: ClauseElement
    ) -> ColumnElement[int]:
        if engine.dialect.name == "postgresql":
            return func.date_part("year", start_date) - func.date_part(
                "year", birth_date
            )
        elif engine.dialect.name == "mssql":
            return func.DATEPART(text("year"), start_date) - func.DATEPART(
                text("year"), birth_date
            )
        else:
            raise NotImplementedError("Unsupported database dialect")

    def _add_relative_date(self, left_value_time: str, right_value_time: str) -> None:
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
        self,
        person_constraints_for_group: list[ColumnElement[bool]],
        current_rule: Rule,
        concepts: dict[str, str],
    ) -> list[ColumnElement[bool]]:
        concept_domain: str | None = concepts.get(current_rule.value)

        if current_rule.varname == "AGE":
            # AGE is a special case, as it is not a concept_id but a range.
            min_value = current_rule.min_value
            max_value = current_rule.max_value

            if min_value is None or max_value is None:
                return person_constraints_for_group

            age = self._get_year_difference(
                self.db_manager.engine, func.current_timestamp(), Person.birth_datetime
            )
            person_constraints_for_group.append(age >= min_value)
            person_constraints_for_group.append(age <= max_value)

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

    def _add_secondary_modifiers(self, current_rule: Rule) -> None:
        # Not sure where, but even when a secondary modifier is not supplied, an array
        # with a single entry is provided.
        secondary_modifier_list = []

        for typeAdd in current_rule.secondary_modifier or []:
            if typeAdd:
                secondary_modifier_list.append(
                    ConditionOccurrence.condition_type_concept_id == int(typeAdd)
                )

        if len(secondary_modifier_list) > 0:
            self.condition = self.condition.where(or_(*secondary_modifier_list))

    def _add_standard_concept(self, current_rule: Rule) -> None:
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
