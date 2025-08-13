from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Any, Callable
from sqlalchemy.sql.expression import ClauseElement
from sqlalchemy import (
    CompoundSelect,
    Engine, 
    or_,
    func,
    BinaryExpression,
    ColumnElement,
    select,
    Select,
    text, 
    union
)
from hutch_bunny.core.db_manager import SyncDBManager
from hutch_bunny.core.entities import (
    ConditionOccurrence,
    Measurement,
    Observation,
    Person,
    DrugExposure
)
from typing import Tuple 
import operator as op

from hutch_bunny.core.rquest_models.rule import Rule


class SQLDialectHandler:
    @staticmethod
    def get_year_difference(
        engine: Engine,
        start_date: ClauseElement,
        year_of_birth: ClauseElement 
    ) -> ColumnElement[int]:
        if engine.dialect.name == "postgresql":
            return func.date_part("year", start_date) - year_of_birth
        elif engine.dialect.name == "mssql":
            return func.DATEPART(text("year"), start_date) - year_of_birth
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
        """
        Apply age-at-event constraints to condition, drug, measurement, and observation queries.

        Depending on which boundary is provided (left or right), this method applies a greater-than or less-than
        comparator to filter records where the person's age at the event date satisfies the constraint.

        Args:
            left_value_time (str | None): Lower age bound as a string, or None if not specified.
            right_value_time (str | None): Upper age bound as a string, or None if not specified.

        Returns:
            OMOPRuleQueryBuilder: The current instance with updated queries reflecting the age constraints.
        """
        if not left_value_time and not right_value_time: 
            return self
        
        if not left_value_time:
            comparator = op.lt
            age_value = int(right_value_time)
        elif not right_value_time:
            comparator = op.gt
            age_value = int(left_value_time)
        else:
            # Both values present - this would be a range
            # Currently we instead apply lower and upper constraints independently
            raise ValueError(f"Age constraint with both boundaries not implemented: {left_value_time}|{right_value_time}")

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
            self.db_manager.engine, 
            table_date_column, 
            Person.year_of_birth  
        )

        constraint = operator_func(age_difference, age_value)

        # Use JOIN instead of EXISTS for better performance
        return table_query.join(
            Person, 
            Person.person_id == table_person_id
        ).where(constraint)

    def add_temporal_constraint(
        self,
        left_value_time: str, 
        right_value_time: str 
    ) -> 'OMOPRuleQueryBuilder':
        """
        Adds a temporal constraint to OMOP queries relative to the current date,
        using pre-parsed time values representing months.

        Exactly one of `left_value_time` or `right_value_time` should be provided as
        a numeric string (e.g., "6"), representing months. The other should be an
        empty string.

        The method filters events to either before or after the computed relative
        date based on which time value is supplied:
        - If `left_value_time` is given, events before (<=) that relative date are included.
        - If `left_value_time` is empty, events after (>=) the `right_value_time` relative date are included.

        Args:
            left_value_time (str): Left-side time bound in months as a numeric string,
                or empty string if unused.
            right_value_time (str): Right-side time bound in months as a numeric string,
                or empty string if unused.

        Returns:
            OMOPRuleQueryBuilder: The current instance with updated query filters.

        Notes:
        - This method assumes the input strings have already been parsed and
          validated (e.g., "|6" converted to "6") before being passed in.
        - The time values represent months relative to the current date.
        """

        if not left_value_time and not right_value_time:
            raise ValueError(
                "Temporal constraint requires exactly one time value. "
                "Both left_value_time and right_value_time are empty."
            )
        
        if left_value_time and right_value_time:
            raise ValueError(
                "Temporal constraint requires exactly one time value. "
                f"Both values were provided: left='{left_value_time}', right='{right_value_time}'. "
                "One must be an empty string."
            )

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
        """
        Add numeric range constraints to measurement and observation queries.
        
        Args:
            min_value: Minimum value (inclusive)
            max_value: Maximum value (inclusive)
            
        Returns:
            Self for method chaining
            
        Raises:
            ValueError: If only one bound is provided or if min > max
        """
        if min_value is None and max_value is None:
            return self
        
        if min_value is None or max_value is None:
            raise ValueError(
                "Both min_value and max_value must be provided for numeric range. "
                f"Got min_value={min_value}, max_value={max_value}"
            )
        
        min_val = float(min_value)
        max_val = float(max_value)

        if min_val > max_val:
            raise ValueError(
                f"min_value must be less than or equal to max_value. "
                f"Got min_value={min_val}, max_value={max_val}"
            )

        self.measurement_query = self.measurement_query.where(
        Measurement.value_as_number.between(min_val, max_val)
        )
        self.observation_query = self.observation_query.where(
            Observation.value_as_number.between(min_val, max_val)
        )

        return self
    
    def add_secondary_modifiers(self, secondary_modifiers: list[int]) -> 'OMOPRuleQueryBuilder':
        """
        Filter the condition query by condition_type_concept_id values.

        Adds an OR-combined filter to `condition_query` so that only condition
        occurrences whose `condition_type_concept_id` matches one of the given
        secondary modifier IDs are included. Has no effect on other table queries.

        Args:
            secondary_modifiers (list[int]): List of `condition_type_concept_id` values
                to filter by. If empty or None, no filter is applied.

        Returns:
            OMOPRuleQueryBuilder: The current instance for method chaining.
        """   
        if not isinstance(secondary_modifiers, list):
            raise TypeError(f"Expected list[int], got {type(secondary_modifiers).__name__}")

        if any(not isinstance(mod, int) for mod in secondary_modifiers):
            raise TypeError("All secondary modifier IDs must be integers")
        
        if not secondary_modifiers:
            return self

        modifier_constraints = [
            ConditionOccurrence.condition_type_concept_id == modifier_id
            for modifier_id in secondary_modifiers if modifier_id
        ]

        if modifier_constraints:
            self.condition_query = self.condition_query.where(or_(*modifier_constraints))

        return self

    def build(self) -> CompoundSelect:
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
    """
    Constructs SQLAlchemy filter constraints for querying the Person table
    based on provided rules and concept mappings.
    """

    def __init__(self, db_manager: SyncDBManager):
        self.db_manager = db_manager

    def build_constraints(self, rule: Rule, concepts: dict[str, str]) -> list[ColumnElement[bool]]:
        """
        Generate SQL constraints for a given person-related rule.

        Args:
            rule (Rule): The rule defining the constraint parameters.
            concepts (dict[str, str]): Mapping of concept IDs to their domains.

        Returns:
            List of SQLAlchemy boolean expressions representing the constraints.
        """
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
            self.db_manager.engine,
            func.current_timestamp(),
            Person.year_of_birth 
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