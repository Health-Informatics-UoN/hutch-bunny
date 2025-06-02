from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Tuple, TypedDict
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
from hutch_bunny.core.entities import (
    ConditionOccurrence,
    Measurement,
    Observation,
    Person,
    DrugExposure,
    ProcedureOccurrence,
)
from hutch_bunny.core.rquest_dto.group import Group
from hutch_bunny.core.rquest_dto.query import AvailabilityQuery
from hutch_bunny.core.rquest_dto.rule import Rule
from hutch_bunny.core.services.concept_service import ConceptService
from sqlalchemy.engine import Engine
from sqlalchemy import exists
from dataclasses import dataclass


class ResultModifier(TypedDict):
    id: str
    threshold: int | None
    nearest: int | None


@dataclass
class QueryState:
    condition: Select[Tuple[int]]
    drug: Select[Tuple[int]]
    measurement: Select[Tuple[int]]
    observation: Select[Tuple[int]]

    @classmethod
    def create_initial_state(cls) -> "QueryState":
        return cls(
            condition=select(ConditionOccurrence.person_id),
            drug=select(DrugExposure.person_id),
            measurement=select(Measurement.person_id),
            observation=select(Observation.person_id),
        )


class AvailabilityQueryBuilder:
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

    def __init__(self, engine: Engine, query: AvailabilityQuery) -> None:
        self.engine = engine
        self.query = query
        self.concepts = ConceptService(engine).map_concepts_to_domains(
            query.cohort.groups
        )

    def build_query(self, results_modifier: list[ResultModifier]) -> Select[Tuple[int]]:
        """Function for taking the JSON query from RQUEST and creating the required query to run against the OMOP database.

        RQUEST API spec can have multiple groups in each query, and then a condition between the groups.

        Each group can have conditional logic AND/OR within the groups.

        Each concept can either be an inclusion or exclusion criteria.

        Each concept can have an age set, so it is that this event with concept X occurred when
        the person was between a certain age.

        This builds an SQL query to run as one for the whole query (was previous multiple) and it
        returns an int for the result. Therefore, all dataframes have been removed.
        """
        all_groups_queries = self._build_group_queries()
        return self._build_final_query(all_groups_queries, results_modifier)

    def _build_group_queries(self) -> list[BinaryExpression[bool]]:
        """Build queries for each group in the cohort."""
        all_groups_queries: list[BinaryExpression[bool]] = []

        for current_group in self.query.cohort.groups:
            group_query = self._build_single_group_query(current_group)
            all_groups_queries.append(Person.person_id.in_(group_query))

        return all_groups_queries

    def _build_single_group_query(self, current_group: Group) -> Select[Tuple[int]]:
        """Build a query for a single group."""
        list_for_rules: list[ColumnElement[bool]] = []
        person_constraints_for_group: list[ColumnElement[bool]] = []

        for current_rule in current_group.rules:
            if current_rule.varcat != "Person":
                rule_constraints = self._build_non_person_rule_constraints(current_rule)
                list_for_rules.extend(rule_constraints)
            else:
                person_constraints_for_group = self._add_person_constraints(
                    person_constraints_for_group, current_rule
                )

        return self._combine_group_constraints(
            current_group.rules_operator, person_constraints_for_group, list_for_rules
        )

    def _build_non_person_rule_constraints(
        self, current_rule: Rule
    ) -> list[ColumnElement[bool]]:
        """Build constraints for a non-person rule."""
        query_state = QueryState.create_initial_state()

        if current_rule.time_category == "AGE":
            query_state = self._add_age_constraints(
                query_state,
                current_rule.left_value_time,
                current_rule.right_value_time,
            )

        query_state = self._add_standard_concept(query_state, current_rule)
        query_state = self._add_secondary_modifiers(query_state, current_rule)
        query_state = self._add_range_as_number(query_state, current_rule)

        if current_rule.time_category == "TIME":
            query_state = self._add_relative_date(
                query_state,
                current_rule.left_value_time,
                current_rule.right_value_time,
            )

        return self._build_table_constraints(query_state, current_rule)

    def _build_table_constraints(
        self, query_state: QueryState, current_rule: Rule
    ) -> list[ColumnElement[bool]]:
        """Build constraints for each table in the query state."""
        table_constraints = [
            (query_state.measurement, Measurement.person_id),
            (query_state.observation, Observation.person_id),
            (query_state.condition, ConditionOccurrence.person_id),
            (query_state.drug, DrugExposure.person_id),
        ]

        table_rule_constraints: list[ColumnElement[bool]] = []
        inclusion_criteria: bool = current_rule.operator == "="

        for table, fk in table_constraints:
            constraint: Exists = exists(table.where(fk == Person.person_id))
            table_rule_constraints.append(
                constraint if inclusion_criteria else ~constraint
            )

        if inclusion_criteria:
            return [or_(*table_rule_constraints)]
        else:
            return [and_(*table_rule_constraints)]

    def _combine_group_constraints(
        self,
        rules_operator: str,
        person_constraints: list[ColumnElement[bool]],
        rule_constraints: list[ColumnElement[bool]],
    ) -> Select[Tuple[int]]:
        """Combine person constraints and rule constraints based on the operator."""
        if rules_operator == "AND":
            group_query = select(Person.person_id).where(*person_constraints)
            for constraint in rule_constraints:
                group_query = group_query.where(constraint)
            return group_query
        else:
            all_parameters = person_constraints + rule_constraints
            return select(Person.person_id).where(or_(*all_parameters))

    def _build_final_query(
        self,
        all_groups_queries: list[BinaryExpression[bool]],
        results_modifier: list[ResultModifier],
    ) -> Select[Tuple[int]]:
        """Build the final query with all groups and modifiers."""
        low_number = self._get_low_number_threshold(results_modifier)
        rounding = self._get_rounding_value(results_modifier)

        if self.query.cohort.groups_operator == "OR":
            base_query = select(func.count()).where(or_(*all_groups_queries))
        else:
            base_query = select(func.count()).where(*all_groups_queries)

        if rounding > 0:
            base_query = select(
                func.round((func.count() / rounding), 0) * rounding
            ).where(base_query.whereclause)

        if low_number > 0:
            base_query = base_query.having(func.count() >= low_number)

        return base_query

    def _add_range_as_number(
        self, query_state: QueryState, current_rule: Rule
    ) -> QueryState:
        if current_rule.min_value is not None and current_rule.max_value is not None:
            return QueryState(
                condition=query_state.condition,
                drug=query_state.drug,
                measurement=query_state.measurement.where(
                    Measurement.value_as_number.between(
                        float(current_rule.min_value), float(current_rule.max_value)
                    )
                ),
                observation=query_state.observation.where(
                    Observation.value_as_number.between(
                        float(current_rule.min_value), float(current_rule.max_value)
                    )
                ),
            )
        return query_state

    def _add_age_constraints(
        self,
        query_state: QueryState,
        left_value_time: str | None,
        right_value_time: str | None,
    ) -> QueryState:
        if left_value_time is None or right_value_time is None:
            return query_state

        new_condition = query_state.condition.join(
            Person, Person.person_id == ConditionOccurrence.person_id
        )
        new_drug = query_state.drug.join(
            Person, Person.person_id == DrugExposure.person_id
        )
        new_measurement = query_state.measurement.join(
            Person, Person.person_id == Measurement.person_id
        )
        new_observation = query_state.observation.join(
            Person, Person.person_id == Observation.person_id
        )

        # due to the way the query is expressed and how split above, if the left value is empty
        # it indicates a less than search

        if left_value_time == "":
            new_condition = new_condition.where(
                self._get_year_difference(
                    self.engine,
                    ConditionOccurrence.condition_start_datetime,
                    Person.birth_datetime,
                )
                < int(right_value_time)
            )
            new_drug = new_drug.where(
                self._get_year_difference(
                    self.engine,
                    DrugExposure.drug_exposure_start_date,
                    Person.birth_datetime,
                )
                < int(right_value_time)
            )
            new_measurement = new_measurement.where(
                self._get_year_difference(
                    self.engine,
                    Measurement.measurement_date,
                    Person.birth_datetime,
                )
                < int(right_value_time)
            )
            new_observation = new_observation.where(
                self._get_year_difference(
                    self.engine,
                    Observation.observation_date,
                    Person.birth_datetime,
                )
                < int(right_value_time)
            )
        else:
            new_condition = new_condition.where(
                self._get_year_difference(
                    self.engine,
                    ConditionOccurrence.condition_start_date,
                    Person.birth_datetime,
                )
                > int(left_value_time)
            )
            new_drug = new_drug.where(
                self._get_year_difference(
                    self.engine,
                    DrugExposure.drug_exposure_start_date,
                    Person.birth_datetime,
                )
                > int(left_value_time)
            )
            new_measurement = new_measurement.where(
                self._get_year_difference(
                    self.engine,
                    Measurement.measurement_date,
                    Person.birth_datetime,
                )
                > int(left_value_time)
            )
            new_observation = new_observation.where(
                self._get_year_difference(
                    self.engine,
                    Observation.observation_date,
                    Person.birth_datetime,
                )
                > int(left_value_time)
            )

        return QueryState(
            condition=new_condition,
            drug=new_drug,
            measurement=new_measurement,
            observation=new_observation,
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

    def _add_relative_date(
        self,
        query_state: QueryState,
        left_value_time: str | None,
        right_value_time: str | None,
    ) -> QueryState:
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
            return QueryState(
                condition=query_state.condition.where(
                    ConditionOccurrence.condition_start_date >= relative_date
                ),
                drug=query_state.drug.where(
                    DrugExposure.drug_exposure_start_date >= relative_date
                ),
                measurement=query_state.measurement.where(
                    Measurement.measurement_date >= relative_date
                ),
                observation=query_state.observation.where(
                    Observation.observation_date >= relative_date
                ),
            )
        else:
            return QueryState(
                condition=query_state.condition.where(
                    ConditionOccurrence.condition_start_date <= relative_date
                ),
                drug=query_state.drug.where(
                    DrugExposure.drug_exposure_start_date <= relative_date
                ),
                measurement=query_state.measurement.where(
                    Measurement.measurement_date <= relative_date
                ),
                observation=query_state.observation.where(
                    Observation.observation_date <= relative_date
                ),
            )

    def _add_person_constraints(
        self,
        person_constraints_for_group: list[ColumnElement[bool]],
        current_rule: Rule,
    ) -> list[ColumnElement[bool]]:
        concept_domain: str | None = self.concepts.get(current_rule.value)

        if current_rule.varname == "AGE":
            # AGE is a special case, as it is not a concept_id but a range.
            min_value = current_rule.min_value
            max_value = current_rule.max_value

            if min_value is None or max_value is None:
                return person_constraints_for_group

            age = self._get_year_difference(
                self.engine, func.current_timestamp(), Person.birth_datetime
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

    def _add_secondary_modifiers(
        self, query_state: QueryState, current_rule: Rule
    ) -> QueryState:
        # Not sure where, but even when a secondary modifier is not supplied, an array
        # with a single entry is provided.
        secondary_modifier_list = []

        for type_index, typeAdd in enumerate(current_rule.secondary_modifier, start=0):
            if typeAdd != "":
                secondary_modifier_list.append(
                    ConditionOccurrence.condition_type_concept_id == int(typeAdd)
                )

        if len(secondary_modifier_list) > 0:
            return QueryState(
                condition=query_state.condition.where(or_(*secondary_modifier_list)),
                drug=query_state.drug,
                measurement=query_state.measurement,
                observation=query_state.observation,
            )
        return query_state

    def _add_standard_concept(
        self, query_state: QueryState, current_rule: Rule
    ) -> QueryState:
        return QueryState(
            condition=query_state.condition.where(
                ConditionOccurrence.condition_concept_id == int(current_rule.value)
            ),
            drug=query_state.drug.where(
                DrugExposure.drug_concept_id == int(current_rule.value)
            ),
            measurement=query_state.measurement.where(
                Measurement.measurement_concept_id == int(current_rule.value)
            ),
            observation=query_state.observation.where(
                Observation.observation_concept_id == int(current_rule.value)
            ),
        )

    def _get_low_number_threshold(self, results_modifier: list[ResultModifier]) -> int:
        """Get the low number threshold from results modifier, defaulting to 10 if not found."""
        for item in results_modifier:
            if item["id"] == "Low Number Suppression":
                return item["threshold"] if item["threshold"] is not None else 10
        return 10

    def _get_rounding_value(self, results_modifier: list[ResultModifier]) -> int:
        """Get the rounding value from results modifier, defaulting to 10 if not found."""
        for item in results_modifier:
            if item["id"] == "Rounding":
                return item["nearest"] if item["nearest"] is not None else 10
        return 10
