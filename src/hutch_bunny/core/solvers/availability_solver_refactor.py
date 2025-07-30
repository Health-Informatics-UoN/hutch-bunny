from dataclasses import dataclass
from datetime import datetime

import pandas as pd
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


@dataclass
class TimeConstraint:
    value: str
    category: Literal["AGE", "TIME"]
    left_value: str | None = None
    right_value: str | None = None


@dataclass
class NumericRange:
    min: float = None
    max: float = None


class ResultModifier(TypedDict):
    id: str
    threshold: int | None
    nearest: int | None


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
        self,
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

    def build(self, operator: str) -> ColumnElement[bool]:
        """
        Build the final constraint list for this single rule.

        Returns:
            A single constraint that represents this rule's logic
        """
        table_constraints = [
            (self.measurement_query, Measurement.person_id),
            (self.observation_query, Observation.person_id),
            (self.condition_query, ConditionOccurrence.person_id),
            (self.drug_query, DrugExposure.person_id),
        ]
        table_rule_constraints = []
        inclusion_criteria = operator == "="

        for table, fk in table_constraints:
            constraint: Exists = exists(
                table.where(fk == Person.person_id)
            )
            table_rule_constraints.append(
                constraint if inclusion_criteria else ~constraint
            )

        if inclusion_criteria:
            return or_(*table_rule_constraints)
        else:
            return and_(*table_rule_constraints)


class AvailabilitySolver():

    def __init__(self, db_manager: SyncDBManager, query: AvailabilityQuery) -> None:
        self.db_manager = db_manager
        self.query = query

    def solve_rules(self):
        """Main query resolution."""
        concepts = self._find_concepts(self.query.cohort.groups)
        modifiers = self._extract_modifiers(results_modifier)

        with self.db_manager.engine.connect() as con:
            group_queries = []

            for group in self.query.cohort.groups:
                group_query = self._build_group_query(group, concepts)
                group_queries.append(group_query)

            final_query = self._combine_groups(group_queries, self.query.cohort.groups_operator)
            final_query = self._apply_modifiers(final_query, modifiers)

            return self._execute_query(con, final_query, modifiers)

    def _extract_modifier(
        self,
        results_modifiers: list[ResultsModifier],
        result_id: str,
        default_value: int = 10
    ):
        return next(
            (
                item["threshold"] if item["threshold"] is not None else 10
                for item in results_modifiers
                if item["id"] == result_id
            ),
            default_value,
        )

    def _build_group_query(self):
        pass

    def _build_rule_constraint(self, rule: Rule) -> ColumnElement[bool]:
        """Build constraint for a single non-Person rule."""
        builder = OMOPRuleQueryBuilder(self.db_manager)

        time_constraint = self._parse_time_constraint(rule.time) if rule.time else None
        numeric_range = self._parse_numeric_range(rule.raw_range) if rule.raw_range else None

        if rule.value:
            builder.add_concept_constraint(int(rule.value))

        if time_constraint and time_constraint.category == "AGE":
            builder.add_age_constraint(
                left_value_time=time_constraint.left_value,
                right_value_time=time_constraint.right_value
            )
        elif time_constraint and time_constraint.category == "TIME":
            builder.add_temporal_constraint(
                left_value_time=time_constraint.left_value,
                right_value_time=time_constraint.right_value
            )

        if numeric_range:
            builder.add_numeric_range(numeric_range.min, numeric_range.max)

        if rule.secondary_modifier:
            builder.add_secondary_modifiers(rule.secondary_modifier)

        return builder.build(operator=rule.operator)

    def _parse_time_constraint(self, time: str):
        time_value, time_category, _ = time.split(":")
        left_value_time, right_value_time = time_value.split("|")
        return TimeConstraint(
            value=time_value,
            category=time_category,
            left_value=left_value_time,
            right_value=right_value_time
        )

    def _parse_numeric_range(self, raw_range: str):
        min_str, max_str = raw_range.split("|")
        min_val = float(min_str) if min_str else None
        max_val = float(max_str) if max_str else None
        return NumericRange(
            min=min_val,
            max=max_val
        )

    def _combine_constraints(self):
        pass



