import pandas as pd
from typing import TypedDict, Union
from sqlalchemy import (
    CompoundSelect,
    func,
    ColumnElement,
    select,
    Select,
    intersect,
    union,
    literal
)
from hutch_bunny.core.db_manager import SyncDBManager
from hutch_bunny.core.entities import (
    Concept,
    Person
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
from hutch_bunny.core.logger import logger, INFO
from hutch_bunny.core.rquest_models.rule import Rule
from hutch_bunny.core.solvers.rule_query_builders import OMOPRuleQueryBuilder, PersonConstraintBuilder


class ResultModifier(TypedDict):
    id: str
    threshold: int | None
    nearest: int | None


class RuleTableQuery(TypedDict):
    union_query: CompoundSelect
    inclusion: bool


class AvailabilitySolver():

    def __init__(self, db_manager: SyncDBManager, query: AvailabilityQuery) -> None:
        self.db_manager = db_manager
        self.query = query
        self.person_constraint_builder = PersonConstraintBuilder(db_manager)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(60),
        before_sleep=before_sleep_log(logger, INFO),
        after=after_log(logger, INFO)
    )
    def solve_query(self, results_modifiers: list[ResultModifier]) -> int:
        """
        Solve the availability query by:
        1. Finding concepts and extracting modifiers
        2. Building queries for each group
        3. Combining groups with AND/OR logic
        4. Executing the final query and applying filters
        """
        concepts = self._find_concepts(self.query.cohort.groups)
        low_number = self._extract_modifier(results_modifiers, "Low Number Suppression", "threshold", 10)
        rounding = self._extract_modifier(results_modifiers, "Rounding", "nearest", 10)

        with self.db_manager.engine.connect() as con:
            group_queries = []

            for group in self.query.cohort.groups:
                group_query = self._build_group_query(group, concepts)
                group_queries.append(group_query)

            final_query = self._construct_final_query(
                group_queries,
                rounding, 
                low_number
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
        key: str,                  # new parameter for the key to extract
        default_value: int = 10
    ) -> int:
        return next(
            (
                item[key] if item.get(key) is not None else default_value
                for item in results_modifiers
                if item.get("id") == result_id
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

    def _build_rule_query(self, rule: Rule) -> CompoundSelect:
        """Build query for a single non-Person rule."""
        builder = OMOPRuleQueryBuilder(self.db_manager)

        if rule.value:
            builder.add_concept_constraint(int(rule.value))

        valid_time_constraint = rule.left_value_time or rule.right_value_time
        if valid_time_constraint and rule.time_category == "AGE":
            builder.add_age_constraint(
                left_value_time=rule.left_value_time,
                right_value_time=rule.right_value_time
            )
        elif valid_time_constraint and rule.time_category == "TIME":
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
