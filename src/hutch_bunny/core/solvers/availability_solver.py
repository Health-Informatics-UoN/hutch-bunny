from datetime import date
from logging import DEBUG
from typing import TypedDict, Union
from sqlalchemy import (
    CompoundSelect,
    Connection,
    Date,
    Integer,
    func,
    ColumnElement,
    insert,
    select,
    Select,
    intersect,
    union,
    literal, 
    or_
)
from hutch_bunny.core.db import BaseDBClient
from hutch_bunny.core.db.entities import (
    Cohort,
    CohortDefinition,
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
from hutch_bunny.core.logger import logger
from hutch_bunny.core.rquest_models.rule import Rule
from hutch_bunny.core.omop import Varcat
from hutch_bunny.core.results_modifiers import ResultModifier, get_modifier_value
from hutch_bunny.core.solvers.rule_query_builders import OMOPRuleQueryBuilder, PersonConstraintBuilder
from hutch_bunny.core.db.utils import log_query
from hutch_bunny.core.settings import Settings


settings = Settings()

# Values written to cohort_definition when LOG_RESULTS_ENABLED is set
COHORT_DEFINITION_DESCRIPTION = "auto generated from cohort discovery search"
COHORT_DEFINITION_TYPE_CONCEPT_ID = 0
COHORT_SUBJECT_CONCEPT_ID = 1147314 #this is to indicate the cohort is based on the person id



class RuleTableQuery(TypedDict):
    union_query: CompoundSelect[Tuple[int]]
    inclusion: bool


class AvailabilitySolver():

    def __init__(self, db_client: BaseDBClient, query: AvailabilityQuery) -> None:
        self.db_client = db_client
        self.query = query
        self.person_constraint_builder = PersonConstraintBuilder(db_client)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(60),
        before_sleep=before_sleep_log(logger, DEBUG),
        after=after_log(logger, DEBUG)
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
        low_number = get_modifier_value(results_modifiers, "Low Number Suppression", "threshold")
        rounding = get_modifier_value(results_modifiers, "Rounding", "nearest")

        with self.db_client.engine.connect() as con:
            group_queries = []

            for group in self.query.cohort.groups:
                group_query = self._build_group_query(group, concepts)
                group_queries.append(group_query)

            person_ids_query = self._construct_person_ids_query(group_queries)
            log_results = settings.LOG_RESULTS_ENABLED and self.db_client.supports_cohort_logging

            try:
                if log_results:
                    # Store the matching person IDs, then count what was stored so the
                    # returned count always matches the logged cohort.
                    cohort_definition_id = self._write_cohort(con, person_ids_query)
                    count_source: Union[Select[Tuple[int]], CompoundSelect[Tuple[int]]] = select(
                        Cohort.subject_id
                    ).where(Cohort.cohort_definition_id == cohort_definition_id)
                else:
                    count_source = person_ids_query

                final_query = self._construct_count_query(count_source, rounding, low_number)
                output = con.execute(final_query).fetchone()
                count = int(output[0]) if output is not None else 0

                if log_results:
                    con.commit()
            except Exception as e:
                logger.error(str(e))

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
        with self.db_client.engine.connect() as con:
            result = con.execute(concept_query)
            concept_dict = {
                str(concept_id): domain_id for concept_id, domain_id in result
            }
        return concept_dict

    def _build_group_query(
        self,
        group: Group,
        concepts: dict[str, str]
    ) -> Union[Select[Tuple[int]], CompoundSelect[Tuple[int]]]:
        """
        Build query for a single group - a nested SQL expression.

        Args:
            group: The group that contains the rules to be assembled
            concepts: a dictionary that maps the concepts IDs to the domains they belong

        Returns:
            Either a single Select statement or multiple joined by UNION or INTERSECT
            depending on the logic specified for the group
        """

        rule_table_queries: list[RuleTableQuery] = []
        person_constraints: list[ColumnElement[bool]] = []

        for rule in group.rules:
            inclusion_criteria = rule.operator == "="
            if rule.varcat == Varcat.PERSON:
                constraints = self.person_constraint_builder.build_constraints(rule, concepts)
                person_constraints.extend(constraints)
            else:
                rule_union = self._build_rule_query(rule)
                rule_table_queries.append({
                    'union_query': rule_union,
                    'inclusion': inclusion_criteria
                })

        return self._construct_group_query(group, person_constraints, rule_table_queries)

    def _build_rule_query(self, rule: Rule) -> CompoundSelect[Tuple[int]]:
        """Build query for a single non-Person rule."""
        builder = OMOPRuleQueryBuilder(
            self.db_client,
            include_specimen=settings.OMOP_SPECIMEN_ENABLED,
            include_location=settings.OMOP_LOCATION_ENABLED,
            include_death=settings.OMOP_DEATH_ENABLED,
            varcat=rule.varcat,
        )

        if rule.value:
            builder.add_concept_constraint(int(rule.value))

        valid_time_constraint = rule.greater_than_value or rule.less_than_value

        if valid_time_constraint and rule.time_category == "AGE":
            builder.add_age_constraint(
                greater_than_value=rule.greater_than_value,
                less_than_value=rule.less_than_value
            )
        elif valid_time_constraint and rule.time_category == "TIME":
            builder.add_temporal_constraint(
                greater_than_time=rule.greater_than_value or "",
                less_than_time=rule.less_than_value or ""
            )

        if rule.min_value is not None and rule.max_value is not None:
            builder.add_numeric_range(rule.min_value, rule.max_value)

        if rule.secondary_modifier:
            builder.add_secondary_modifiers(rule.secondary_modifier)

        if (
            rule.center_lat is not None
            and rule.center_lon is not None
            and rule.geo_radius_meters is not None
        ):
            builder.add_haversine_radius_constraint(
                rule.center_lat, rule.center_lon, rule.geo_radius_meters
            )

        return builder.build()

    def _construct_group_query(
        self,
        current_group: Group,
        person_constraints_for_group: list[ColumnElement[bool]],
        rule_table_queries: list[RuleTableQuery]
    ) -> Union[Select[Tuple[int]], CompoundSelect[Tuple[int]]]:
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
        inclusion_queries: list[Union[Select[Tuple[int]], CompoundSelect[Tuple[int]]]] = []
        exclusion_queries: list[Union[Select[Tuple[int]], CompoundSelect[Tuple[int]]]] = []

        # Add person constraints as a separate query
        if person_constraints_for_group:
            if current_group.rules_operator == "OR" and len(person_constraints_for_group) > 1:
                # For OR logic, combine Person constraints with OR
                person_query = select(Person.person_id).where(or_(*person_constraints_for_group))
            else:
                # For AND logic or single constraint, use AND (default)
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
                group_query: Union[Select[Tuple[int]], CompoundSelect[Tuple[int]]] = inclusion_queries[0]
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

    def _construct_person_ids_query(
        self,
        all_groups_queries: list[Union[Select[Tuple[int]], CompoundSelect[Tuple[int]]]],
    ) -> Union[Select[Tuple[int]], CompoundSelect[Tuple[int]]]:
        """
        Construct the query returning the distinct person IDs matching the cohort, by
        applying OR/AND logic between groups using CTEs.

        Args:
            all_groups_queries: List of queries for each group

        Returns:
            The query that selects the matching person IDs
        """
        if not all_groups_queries:
            return select(Person.person_id).where(literal(False))

        group_queries = [
            select(query.cte(name=f"final_group_{i}"))
            for i, query in enumerate(all_groups_queries)
        ]

        if self.query.cohort.groups_operator == "OR":
            # For OR logic between groups, use UNION
            return union(*group_queries)
        # For AND logic between groups, use INTERSECT
        return intersect(*group_queries)

    def _construct_count_query(
        self,
        person_ids_query: Union[Select[Tuple[int]], CompoundSelect[Tuple[int]]],
        rounding: int,
        low_number: int
    ) -> Select[Tuple[int]]:
        """
        Construct the query that counts the person IDs with rounding and low number suppression.

        Args:
            person_ids_query: The query selecting the person IDs to count
            rounding: Rounding factor for the final count
            low_number: Counts below this threshold return no rows

        Returns:
            The final query that counts the results with appropriate rounding
        """
        person_ids = person_ids_query.subquery()

        if rounding > 0:
            count_query = select(
                func.round((func.count() / rounding), 0) * rounding
            ).select_from(person_ids)
        else:
            count_query = select(func.count()).select_from(person_ids)

        if low_number > 0:
            count_query = count_query.having(func.count() >= low_number)

        log_query(count_query, self.db_client.engine)

        return count_query

    def _write_cohort(
        self,
        con: Connection,
        person_ids_query: Union[Select[Tuple[int]], CompoundSelect[Tuple[int]]],
    ) -> int:
        """
        Write a cohort_definition row for this query, and a cohort row for each matching
        person, without the person IDs leaving the database. The caller commits.

        Args:
            con: The connection holding the transaction
            person_ids_query: The query selecting the matching person IDs

        Returns:
            The cohort_definition_id written
        """
        today = date.today()

        # cohort_definition_id has no sequence in the CDM, so take the next free ID
        cohort_definition_id = con.execute(
            select(func.coalesce(func.max(CohortDefinition.cohort_definition_id), 0) + 1)
        ).scalar_one()

        con.execute(
            insert(CohortDefinition).values(
                cohort_definition_id=cohort_definition_id,
                cohort_definition_name=self.query.uuid,
                cohort_definition_description=(
                    f"{COHORT_DEFINITION_DESCRIPTION}: {self.query.cohort.model_dump_json()}"
                ),
                definition_type_concept_id=COHORT_DEFINITION_TYPE_CONCEPT_ID,
                cohort_definition_syntax=None,
                subject_concept_id=COHORT_SUBJECT_CONCEPT_ID,
                cohort_initiation_date=today,
            )
        )

        person_ids = person_ids_query.subquery()
        person_id = list(person_ids.c)[0]
        insert_cohort = insert(Cohort).from_select(
            ["cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date"],
            select(
                literal(cohort_definition_id, Integer),
                person_id,
                literal(today, Date),
                literal(today, Date),
            ),
        )
        log_query(insert_cohort, self.db_client.engine)
        con.execute(insert_cohort)

        logger.info(f"Logged cohort as cohort_definition_id {cohort_definition_id}")
        return int(cohort_definition_id)
