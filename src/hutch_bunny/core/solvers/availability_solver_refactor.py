from dataclasses import dataclass
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta
from typing import Any, Callable, TypedDict, Union
from sqlalchemy.sql.expression import ClauseElement
from sqlalchemy import (
    CompoundSelect,
    or_,
    and_,
    func,
    BinaryExpression,
    ColumnElement,
    select,
    Select,
    text,
    intersect,
    union,
    Exists
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

from typing import Tuple, Literal
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


class RuleTableQuery(TypedDict):
    union_query: CompoundSelect
    inclusion: bool


class SQLDialectHandler:
    @staticmethod
    def get_year_difference(
        engine: Engine,
        start_date: ClauseElement,
        birth_date: ClauseElement
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


class OMOPRuleQueryBuilder:
    """Builder for constructing OMOP queries from availability rules."""

    def __init__(self, db_manager: SyncDBManager):
        self.db_manager = db_manager
        self.condition_query: Select[Tuple[int]] = select(ConditionOccurrence.person_id)
        self.drug_query: Select[Tuple[int]] = select(DrugExposure.person_id)
        self.measurement_query: Select[Tuple[int]] = select(Measurement.person_id)
        self.observation_query: Select[Tuple[int]] = select(Observation.person_id)

    def add_concept_constraint(self, concept_id: int) -> 'OMOPRuleQueryBuilder':
        """Add standard concept ID constraints to all relevant tables."""
        self.condition_query = self.condition_query.where(
            ConditionOccurrence.condition_concept_id == concept_id
        )
        self.drug_query = self.drug_query.where(
            DrugExposure.drug_concept_id == concept_id
        )
        self.measurement_query = self.measurement_query.where(
            Measurement.measurement_concept_id == concept_id
        )
        self.observation_query = self.observation_query.where(
            Observation.observation_concept_id == concept_id
        )
        return self

    def add_age_constraint(
        self,
        left_value_time: str | None,
        right_value_time: str | None
    ) -> 'OMOPRuleQueryBuilder':
        """Add age-at-event constraints."""
        if left_value_time is None or right_value_time is None:
            return self
        if left_value_time == "":
            comparator = op.lt
            age_value = int(right_value_time)
        else:
            comparator = op.gt
            age_value = int(left_value_time)

        self.condition_query = self._apply_age_constraint_to_table(
            self.condition_query,
            ConditionOccurrence.person_id,
            ConditionOccurrence.condition_start_date,
            comparator,
            age_value,
        )
        self.drug_query = self._apply_age_constraint_to_table(
            self.drug_query,
            DrugExposure.person_id,
            DrugExposure.drug_exposure_start_date,
            comparator,
            age_value,
        )
        self.measurement_query = self._apply_age_constraint_to_table(
            self.measurement_query,
            Measurement.person_id,
            Measurement.measurement_date,
            comparator,
            age_value,
        )
        self.observation_query = self._apply_age_constraint_to_table(
            self.observation_query,
            Observation.person_id,
            Observation.observation_date,
            comparator,
            age_value,
        )
        return self

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
        age_difference = SQLDialectHandler.get_year_difference(
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

    def add_temporal_constraint(
        self,
        left_value_time: str,
        right_value_time: str
    ) -> 'OMOPRuleQueryBuilder':
        """
        Add temporal constraints relative to current date.

        Args:

        """
        if left_value_time == "":
            time_value_supplied = right_value_time
        else:
            time_value_supplied = left_value_time

        time_to_use = int(time_value_supplied)
        time_to_use = time_to_use * -1

        today_date = datetime.now()
        relative_date = today_date + relativedelta(months=time_to_use)

        if left_value_time == "":
            self.measurement_query = self.measurement_query.where(
                Measurement.measurement_date >= relative_date
            )
            self.observation_query = self.observation_query.where(
                Observation.observation_date >= relative_date
            )
            self.condition_query = self.condition_query.where(
                ConditionOccurrence.condition_start_date >= relative_date
            )
            self.drug_query = self.drug_query.where(
                DrugExposure.drug_exposure_start_date >= relative_date
            )
        else:
            self.measurement_query = self.measurement_query.where(
                Measurement.measurement_date <= relative_date
            )
            self.observation_query = self.observation_query.where(
                Observation.observation_date <= relative_date
            )
            self.condition_query = self.condition_query.where(
                ConditionOccurrence.condition_start_date <= relative_date
            )
            self.drug_query = self.drug_query.where(
                DrugExposure.drug_exposure_start_date <= relative_date
            )
        return self

    def add_numeric_range(
        self,
        min_value: float = None,
        max_value: float = None
    ) -> 'OMOPRuleQueryBuilder':
        self.measurement_query = self.measurement_query.where(
            Measurement.value_as_number.between(
                float(min_value), float(max_value)
            )
        )
        self.observation_query = self.observation_query.where(
            Observation.value_as_number.between(
                float(min_value), float(max_value)
            )
        )
        return self

    def add_secondary_modifiers(self, secondary_modifiers: list[int]) -> 'QueryBuilder':
        """Add secondary modifier constraints (only applies to conditions)."""
        if not secondary_modifiers:
            return self

        modifier_constraints = [
            ConditionOccurrence.condition_type_concept_id == modifier_id
            for modifier_id in secondary_modifiers if modifier_id
        ]

        if modifier_constraints:
            self.condition_query = self.condition_query.where(or_(*modifier_constraints))

        return self

    def build(self) -> ColumnElement[bool]:
        """
        Build UNION query across all tables for this rule.

        Returns:
            CompoundSelect: UNION of all table queries
        """
        return union(
            self.measurement_query,
            self.observation_query,
            self.condition_query,
            self.drug_query
        )


class PersonConstraintBuilder:
    """Builds constraints for Person table queries."""

    def __init__(self, db_manager: SyncDBManager):
        self.db_manager = db_manager

    def build_constraints(self, rule: Rule, concepts: dict[str, str]) -> list[ColumnElement[bool]]:
        """Build all constraints for a person-related rule."""
        if rule.varname == "AGE":
            return self._build_age_constraints(rule)

        concept_domain = concepts.get(rule.value)
        if concept_domain == "Gender":
            return self._build_gender_constraint(rule)
        elif concept_domain == "Race":
            return self._build_race_constraint(rule)
        elif concept_domain == "Ethnicity":
            return self._build_ethnicity_constraint(rule)

        return []

    def _build_age_constraints(self, rule: Rule) -> list[ColumnElement[bool]]:
        """Build age range constraints."""
        if rule.min_value is None or rule.max_value is None:
            return []

        age = SQLDialectHandler.get_year_difference(
            self.db_manager,
            func.current_timestamp(),
            Person.birth_datetime
        )
        return [
            age >= rule.min_value,
            age <= rule.max_value
        ]

    def _build_gender_constraint(self, rule: Rule) -> list[ColumnElement[bool]]:
        """Build gender constraint."""
        constraint = Person.gender_concept_id == int(rule.value)
        return [constraint if rule.operator == "=" else ~constraint]

    def _build_race_constraint(self, rule: Rule) -> list[ColumnElement[bool]]:
        """Build race constraint."""
        constraint = Person.race_concept_id == int(rule.value)
        return [constraint if rule.operator == "=" else ~constraint]

    def _build_ethnicity_constraint(self, rule: Rule) -> list[ColumnElement[bool]]:
        """Build ethnicity constraint."""
        constraint = Person.ethnicity_concept_id == int(rule.value)
        return [constraint if rule.operator == "=" else ~constraint]


class AvailabilitySolver():

    def __init__(self, db_manager: SyncDBManager, query: AvailabilityQuery) -> None:
        self.db_manager = db_manager
        self.query = query
        self.person_constraint_builder = PersonConstraintBuilder(db_manager)

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
        return self.solve_rules(results_modifier)

    def solve_rules(self, results_modifiers: list[ResultModifier]) -> int:
        """Main query resolution."""
        concepts = self._find_concepts(self.query.cohort.groups)
        low_number = self._extract_modifier(results_modifiers, "Low Number Suppression")
        rounding = self._extract_modifier(results_modifiers, "Rounding")

        with self.db_manager.engine.connect() as con:
            group_queries = []

            for group in self.query.cohort.groups:
                group_query = self._build_group_query(group, concepts)
                group_queries.append(group_query)

            final_query = self._construct_final_query(
                group_queries,
                low_number,
                rounding
            )

            output = con.execute(final_query).fetchone()
            count = int(output[0]) if output is not None else 0

        return apply_filters(count, results_modifiers)

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

    def _extract_modifier(
        self,
        results_modifiers: list[ResultModifier],
        result_id: str,
        default_value: int = 10
    ) -> int:
        return next(
            (
                item["threshold"] if item["threshold"] is not None else 10
                for item in results_modifiers
                if item["id"] == result_id
            ),
            default_value,
        )

    def _build_group_query(
        self,
        group: Group,
        concepts: dict[str, str]
    ) -> Union[Select[Tuple[int]], CompoundSelect]:
        """Build query for a single group - a nested SQL expression."""
        rule_table_queries = []
        person_constraints = []

        for rule in group.rules:
            inclusion_criteria = rule.operator == "="
            if rule.varcat == "Person":
                constraints = self.person_constraint_builder.build_constraints(rule, concepts)
                person_constraints.extend(constraints)
            else:
                rule_union = self._build_rule_query(rule)
                rule_table_queries.append({
                    'union_query': rule_union,
                    'inclusion': inclusion_criteria
                })

        return self._construct_group_query(group, person_constraints, rule_table_queries)

    def _build_rule_query(self, rule: Rule) -> ColumnElement[bool]:
        """Build query for a single non-Person rule."""
        builder = OMOPRuleQueryBuilder(self.db_manager)

        if rule.value:
            builder.add_concept_constraint(int(rule.value))

        if rule.left_value_time and rule.right_value_time and rule.time_category == "AGE":
            builder.add_age_constraint(
                left_value_time=rule.left_value_time,
                right_value_time=rule.right_value_time
            )
        elif rule.left_value_time and rule.right_value_time and rule.time_category == "TIME":
            builder.add_temporal_constraint(
                left_value_time=rule.left_value_time,
                right_value_time=rule.right_value_time
            )

        if rule.min_value is not None and rule.max_value is not None:
            builder.add_numeric_range(rule.min_value, rule.max_value)

        if rule.secondary_modifier:
            builder.add_secondary_modifiers(rule.secondary_modifier)

        return builder.build()

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
        inclusion_queries: list[Union[Select[Tuple[int]], CompoundSelect]] = []
        exclusion_queries: list[Union[Select[Tuple[int]], CompoundSelect]] = []

        # Add person constraints as a separate query
        if person_constraints_for_group:
            person_query = select(Person.person_id).where(*person_constraints_for_group)
            inclusion_queries.append(person_query)

        # Add table queries for each rule
        if rule_table_queries:
            logger.debug(f"Processing {len(rule_table_queries)} rule table queries")
            for i, rule_data in enumerate(rule_table_queries):
                union_query = rule_data['union_query']
                inclusion = rule_data['inclusion']
                logger.debug(f"Rule {i}: inclusion={inclusion}")

                if inclusion:
                    # For inclusion: add the union directly
                    inclusion_queries.append(union_query)
                    logger.debug(f"Added inclusion query for rule {i}")
                else:
                    # For exclusion: store the union query to exclude people who match
                    exclusion_queries.append(union_query)
                    logger.debug(f"Added exclusion query for rule {i}")
        else:
            logger.debug("No rule table queries found")

        # Create the final group query (without CTEs at this level)
        if inclusion_queries:
            if current_group.rules_operator == "AND":
                # For AND logic, use INTERSECT which is more efficient than joins
                group_query: Union[Select[Tuple[int]], CompoundSelect] = inclusion_queries[0]
                for query in inclusion_queries[1:]:
                    group_query = intersect(group_query, query)
            else:
                # For OR logic, use UNION
                group_query = union(*inclusion_queries)
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
                exclusion_query = select(Person.person_id).where(
                    ~Person.person_id.in_(select(exclusion_union.subquery()))
                )
                group_query = intersect(group_query, exclusion_query)

                logger.debug("Exclusion queries processed successfully")
            except Exception as e:
                logger.error(f"Error processing exclusion queries: {e}")
                raise

        return group_query

    def _construct_final_query(
        self,
        all_groups_queries: list[Union[Select[Tuple[int]], CompoundSelect]],
        rounding: int,
        low_number: int
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

        if low_number > 0:
            full_query_all_groups = full_query_all_groups.having(
                func.count() >= low_number
            )

        logger.debug(
            str(
                full_query_all_groups.compile(
                    dialect=self.db_manager.engine.dialect,
                    compile_kwargs={"literal_binds": True},
                )
            )
        )

        return full_query_all_groups
