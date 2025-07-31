import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Any, Callable, TypedDict, Union
from sqlalchemy.sql.expression import ClauseElement
from sqlalchemy import (
    CompoundSelect,
    or_,
    func,
    BinaryExpression,
    ColumnElement,
    select,
    Select,
    text,
    intersect,
    union,
    literal,
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


class RuleTableQuery(TypedDict):
    union_query: CompoundSelect
    inclusion: bool


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

        with self.db_manager.engine.connect() as con:
            # this is used to store the query for each group, one entry per group
            all_groups_queries: list[Union[Select[Tuple[int]], CompoundSelect]] = []

            # iterate through all the groups specified in the query
            for current_group in self.query.cohort.groups:

                # captures all the person constraints for the group
                person_constraints_for_group: list[ColumnElement[bool]] = []
                # captures all the rule table queries for the group
                rule_table_queries: list[RuleTableQuery] = []

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
                        condition_query: Select[Tuple[int]] = select(
                            ConditionOccurrence.person_id
                        )
                        drug_query: Select[Tuple[int]] = select(DrugExposure.person_id)
                        measurement_query: Select[Tuple[int]] = select(
                            Measurement.person_id
                        )
                        observation_query: Select[Tuple[int]] = select(
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
                            condition_query, drug_query, measurement_query, observation_query = self._add_age_constraints(
                                left_value_time, right_value_time, condition_query, drug_query, measurement_query, observation_query
                            )

                        """"
                        STANDARD CONCEPT ID SEARCH
                        """
                        condition_query, drug_query, measurement_query, observation_query = self._add_standard_concept(
                            current_rule, condition_query, drug_query, measurement_query, observation_query
                        )

                        """"
                        SECONDARY MODIFIER
                        """
                        # secondary modifier hits another field and only on the condition_occurrence
                        # on the RQuest GUI this is a list that can be created. Assuming this is also an
                        # AND condition for at least one of the selected values to be present
                        condition_query = self._add_secondary_modifiers(current_rule, condition_query)

                        """"
                        VALUES AS NUMBER
                        """
                        measurement_query, observation_query = self._add_range_as_number(
                            current_rule, measurement_query, observation_query
                        )

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
                            condition_query, drug_query, measurement_query, observation_query = self._add_relative_date(
                                left_value_time, right_value_time, condition_query, drug_query, measurement_query, observation_query
                            )

                        """"
                        PREPARING THE LISTS FOR LATER USE
                        """
                        # a switch between whether the criteria are inclusion or exclusion
                        inclusion_criteria: bool = current_rule.operator == "="

                        # Store the table queries for this rule to be used later in UNION
                        # Union all table queries for this rule
                        rule_union = union(measurement_query, observation_query, condition_query, drug_query)
                        rule_table_queries.append({
                            'union_query': rule_union,
                            'inclusion': inclusion_criteria
                        })

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
                # Build the group query using UNION approach
                group_query = self._construct_group_query(
                    current_group, 
                    person_constraints_for_group,
                    rule_table_queries
                )

                # Store the group query for later assembly
                all_groups_queries.append(group_query)
                logger.debug(f"Total groups stored: {len(all_groups_queries)}")

            """
            ALL GROUPS COMPLETED, NOW APPLY LOGIC BETWEEN GROUPS
            """
            # construct the query based on the OR/AND logic specified between groups using CTEs
            final_query = self._construct_final_query(all_groups_queries, rounding)

            if low_number > 0:
                final_query = final_query.having(
                    func.count() >= low_number
                )

            # here for debug, prints the SQL statement created
            logger.debug(
                str(
                    final_query.compile(
                        dialect=self.db_manager.engine.dialect,
                        compile_kwargs={"literal_binds": True},
                    )
                )
            )

            output = con.execute(final_query).fetchone()
            count = int(output[0]) if output is not None else 0

        return apply_filters(count, results_modifier)

    def _construct_final_query(
        self, 
        all_groups_queries: list[Union[Select[Tuple[int]], CompoundSelect]], 
        rounding: int
    ) -> Select[Tuple[int]]:
        """
        Construct the final query by applying OR/AND logic between groups using CTEs.
        
        Args:
            all_groups_queries: List of queries for each group
            rounding: Rounding factor for the final count
            
        Returns:
            The final query that counts the results with appropriate rounding
        """
        if self.query.cohort.groups_operator == "OR":
            # For OR logic between groups, use UNION with CTEs
            if all_groups_queries:
                # Create CTEs for all group queries
                group_ctes = []
                for i, query in enumerate(all_groups_queries):
                    cte_name = f"final_group_{i}"
                    cte = query.cte(name=cte_name)
                    group_ctes.append(cte)

                # Union all group CTEs by selecting from them
                group_union_queries = [select(cte) for cte in group_ctes]
                final_union = union(*group_union_queries)

                if rounding > 0:
                    full_query_all_groups = select(
                        func.round((func.count() / rounding), 0) * rounding
                    ).select_from(final_union.subquery())
                else:
                    full_query_all_groups = select(func.count()).select_from(final_union.subquery())
            else:
                # Fallback to empty query
                full_query_all_groups = select(func.count()).where(literal(False))
        else:
            # For AND logic between groups, use INTERSECT with CTEs
            if all_groups_queries:
                # Create CTEs for all group queries
                group_ctes = []
                for i, query in enumerate(all_groups_queries):
                    cte_name = f"final_group_{i}"
                    cte = query.cte(name=cte_name)
                    group_ctes.append(cte)

                # Use INTERSECT for AND logic between groups
                group_intersect_queries = [select(cte) for cte in group_ctes]
                final_intersect = intersect(*group_intersect_queries)

                if rounding > 0:
                    full_query_all_groups = select(
                        func.round((func.count() / rounding), 0) * rounding
                    ).select_from(final_intersect.subquery())
                else:
                    full_query_all_groups = select(func.count()).select_from(final_intersect.subquery())

            else:
                # Fallback to empty query
                full_query_all_groups = select(func.count()).where(literal(False))
                
        return full_query_all_groups

    def _construct_group_query(
        self, 
        current_group: Group, 
        person_constraints_for_group: list[ColumnElement[bool]],
        rule_table_queries: list[RuleTableQuery]
    ) -> Union[Select[Tuple[int]], CompoundSelect]:
        """
        Construct the query for a single group by processing inclusion/exclusion rules.
        
        Args:
            current_group: The group to construct a query for
            person_constraints_for_group: Person-level constraints for this group
            rule_table_queries: List of rule table queries for this group
            
        Returns:
            The constructed group query
        """
        # Build the group query using UNION approach
        group_queries: list[Union[Select[Tuple[int]], CompoundSelect]] = []
        exclusion_queries: list[Union[Select[Tuple[int]], CompoundSelect]] = []

        # Add person constraints as a separate query
        if person_constraints_for_group:
            person_query = select(Person.person_id).where(*person_constraints_for_group)
            group_queries.append(person_query)

        # Add table queries for each rule
        if rule_table_queries:
            logger.debug(f"Processing {len(rule_table_queries)} rule table queries")
            for i, rule_data in enumerate(rule_table_queries):
                union_query = rule_data['union_query']
                inclusion = rule_data['inclusion']
                logger.debug(f"Rule {i}: inclusion={inclusion}")

                if inclusion:
                    # For inclusion: add the union directly
                    group_queries.append(union_query)
                    logger.debug(f"Added inclusion query for rule {i}")
                else:
                    # For exclusion: store the union query to exclude people who match
                    exclusion_queries.append(union_query)
                    logger.debug(f"Added exclusion query for rule {i}")
        else:
            logger.debug("No rule table queries found")

        # Create the final group query (without CTEs at this level)
        if group_queries:
            if current_group.rules_operator == "AND":
                # For AND logic, use INTERSECT which is more efficient than joins
                group_query: Union[Select[Tuple[int]], CompoundSelect] = group_queries[0]
                for query in group_queries[1:]:
                    group_query = intersect(group_query, query)
            else:
                # For OR logic, use UNION
                group_query = union(*group_queries)
        else:
            # Start with all people if no inclusion queries
            group_query = select(Person.person_id)

        # Handle exclusion queries - remove people who match exclusion criteria
        if exclusion_queries:
            logger.debug(f"Processing {len(exclusion_queries)} exclusion queries")
            try:
                # Union all exclusion queries
                exclusion_union = union(*exclusion_queries)
                logger.debug("Exclusion union created successfully")

                # Exclude people who match any exclusion criteria
                group_query = select(Person.person_id).where(
                    ~Person.person_id.in_(select(exclusion_union.subquery()))
                )
                logger.debug("Exclusion queries processed successfully")
            except Exception as e:
                logger.error(f"Error processing exclusion queries: {e}")
                raise

        return group_query

    def _add_range_as_number(
        self, 
        current_rule: Rule, 
        measurement_query: Select[Tuple[int]], 
        observation_query: Select[Tuple[int]]
    ) -> tuple[Select[Tuple[int]], Select[Tuple[int]]]:
        if current_rule.min_value is not None and current_rule.max_value is not None:
            measurement_query = measurement_query.where(
                Measurement.value_as_number.between(
                    float(current_rule.min_value), float(current_rule.max_value)
                )
            )
            observation_query = observation_query.where(
                Observation.value_as_number.between(
                    float(current_rule.min_value), float(current_rule.max_value)
                )
            )

        return measurement_query, observation_query

    def _add_age_constraints(
        self, left_value_time: str | None, right_value_time: str | None,
        condition_query: Select[Tuple[int]], drug_query: Select[Tuple[int]],
        measurement_query: Select[Tuple[int]], observation_query: Select[Tuple[int]]
    ) -> tuple[Select[Tuple[int]], Select[Tuple[int]], Select[Tuple[int]], Select[Tuple[int]]]:
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
            return condition_query, drug_query, measurement_query, observation_query

        if left_value_time == "":
            comparator = op.lt
            age_value = int(right_value_time)
        else:
            comparator = op.gt
            age_value = int(left_value_time)

        condition_query = self._apply_age_constraint_to_table(
            condition_query,
            ConditionOccurrence.person_id,
            ConditionOccurrence.condition_start_date,
            comparator,
            age_value,
        )
        drug_query = self._apply_age_constraint_to_table(
            drug_query,
            DrugExposure.person_id,
            DrugExposure.drug_exposure_start_date,
            comparator,
            age_value,
        )
        measurement_query = self._apply_age_constraint_to_table(
            measurement_query,
            Measurement.person_id,
            Measurement.measurement_date,
            comparator,
            age_value,
        )
        observation_query = self._apply_age_constraint_to_table(
            observation_query,
            Observation.person_id,
            Observation.observation_date,
            comparator,
            age_value,
        )

        return condition_query, drug_query, measurement_query, observation_query

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
            self.db_manager.engine, table_date_column, Person.year_of_birth
        )

        constraint = operator_func(age_difference, age_value)

        # Use proper join instead of cross-join
        return table_query.join(Person, Person.person_id == table_person_id).where(constraint)

    def _get_year_difference(
    self, engine: Engine, start_date: ClauseElement, year_of_birth: ClauseElement
) -> ColumnElement[int]:
        if engine.dialect.name in ("postgresql", "postgres"):
            result = func.date_part("year", start_date) - year_of_birth
            return result
        elif engine.dialect.name == "mssql":
            result = func.DATEPART(text("year"), start_date) - year_of_birth
            return result
        else:
            logger.error(f"Unsupported database dialect: {engine.dialect.name}")
            raise NotImplementedError(f"Unsupported database dialect: {engine.dialect.name}")

    def _add_relative_date(self, left_value_time: str, right_value_time: str,
        condition_query: Select[Tuple[int]], drug_query: Select[Tuple[int]],
        measurement_query: Select[Tuple[int]], observation_query: Select[Tuple[int]]
    ) -> tuple[Select[Tuple[int]], Select[Tuple[int]], Select[Tuple[int]], Select[Tuple[int]]]:
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
            measurement_query = measurement_query.where(
                Measurement.measurement_date >= relative_date
            )
            observation_query = observation_query.where(
                Observation.observation_date >= relative_date
            )
            condition_query = condition_query.where(
                ConditionOccurrence.condition_start_date >= relative_date
            )
            drug_query = drug_query.where(
                DrugExposure.drug_exposure_start_date >= relative_date
            )
        else:
            measurement_query = measurement_query.where(
                Measurement.measurement_date <= relative_date
            )
            observation_query = observation_query.where(
                Observation.observation_date <= relative_date
            )
            condition_query = condition_query.where(
                ConditionOccurrence.condition_start_date <= relative_date
            )
            drug_query = drug_query.where(
                DrugExposure.drug_exposure_start_date <= relative_date
            )

        return condition_query, drug_query, measurement_query, observation_query

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
                self.db_manager.engine, func.current_timestamp(), Person.year_of_birth
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

    def _add_secondary_modifiers(self, current_rule: Rule, condition_query: Select[Tuple[int]]) -> Select[Tuple[int]]:
        # Not sure where, but even when a secondary modifier is not supplied, an array
        # with a single entry is provided.
        secondary_modifier_list = []

        for typeAdd in current_rule.secondary_modifier or []:
            if typeAdd:
                secondary_modifier_list.append(
                    ConditionOccurrence.condition_type_concept_id == int(typeAdd)
                )

        if len(secondary_modifier_list) > 0:
            condition_query = condition_query.where(or_(*secondary_modifier_list))

        return condition_query

    def _add_standard_concept(self, current_rule: Rule, condition_query: Select[Tuple[int]], drug_query: Select[Tuple[int]], measurement_query: Select[Tuple[int]], observation_query: Select[Tuple[int]]) -> tuple[Select[Tuple[int]], Select[Tuple[int]], Select[Tuple[int]], Select[Tuple[int]]]:
        condition_query = condition_query.where(
            ConditionOccurrence.condition_concept_id == int(current_rule.value)
        )
        drug_query = drug_query.where(
            DrugExposure.drug_concept_id == int(current_rule.value)
        )
        measurement_query = measurement_query.where(
            Measurement.measurement_concept_id == int(current_rule.value)
        )
        observation_query = observation_query.where(
            Observation.observation_concept_id == int(current_rule.value)
        )

        return condition_query, drug_query, measurement_query, observation_query
