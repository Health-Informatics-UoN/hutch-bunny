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


class QueryBuilder:
    """Builder for constructing OMOP queries from availability rules."""

    def __init__(self, db_manager: SyncDBManager):
        self.db_manager = db_manager
        self.condition_query: Select[Tuple[int]] = select(ConditionOccurrence.person_id)
        self.drug_query: Select[Tuple[int]] = select(DrugExposure.person_id)
        self.measurement_query: Select[Tuple[int]] = select(Measurement.person_id)
        self.observation_query: Select[Tuple[int]] = select(Observation.person_id)

    def add_concept_constraint(self, concept_id: int) -> 'QueryBuilder':
        """Add standard concept ID constraints to all relevant tables."""
        pass

    def add_age_constraint(
        self,
        left_value_time: str | None,
        right_value_time: str | None
    ) -> 'QueryBuilder':
        """Add age-at-event constraints."""
        if left_value_time is None or right_value_time is None:
            return
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

    def add_temporal_constraint(self, before_date: datetime = None, after_date: datetime = None) -> 'QueryBuilder':
        pass

    def add_numeric_range(self, min_value: float = None, max_value: float = None) -> 'QueryBuilder':
        pass

    def build(self) -> list[ColumnElement[bool]]:
        """Build the final constraint list."""
        pass


class AvailabilitySolver():

    def solve_rules(self):
        """Main query resolution."""
        # Find concepts

        # Extract modifiers

        # Build the group query's and append to list

        # Combine the groups

        # Apply the modifiers

        # Execute the query

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

    def _build_rule_constraint(self, rule: Rule) -> ColumnElement[bool]:
        """Build constraint for a single non-Person rule."""
        builder = QueryBuilder(self.db_manager)

        time_constraint = self._parse_time_constraint(rule.time) if rule.time else None
        numeric_range = self._parse_numeric_range(rule.raw_range) if rule.raw_range else None

        if rule.value:
            builder.add_concept_constraint(int(rule.value))

        if time_constraint and time_constraint.type == "AGE":
            builder.add_age_constraint(time_constraint.operator, time_constraint.value)

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



