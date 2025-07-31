import re
from typing import Any, Literal
from pydantic import BaseModel, ConfigDict, Field


class Rule(BaseModel):
    """
    A rule in a group of rules.

    Specifies the search criteria for a rule.
    """

    varname: str = ""
    """
    Variable name to search for.

    Either:
    - `OMOP`: For OMOP searches
    - `AGE`: For AGE searches
    - `OMOP=21490742`: For Measurement searches
    """

    varcat: Literal[
        "Person",
        "Condition",
        "Observation",
        "Drug",
        "Measurement",
        "Medication",
        "Procedure",
    ]
    """
    Table to search in.
    """

    type_: Literal["NUM", "TEXT"] = Field(default="TEXT", alias="type")
    """
    Type of value to search for.

    - `TEXT`: For OMOP concept_id searches
    - `NUM`: For AGE or Measurement searches

    RQUEST supports `ALT`, `SET`, `BOOLEAN` also - but Bunny does not.
    """

    operator: Literal["=", "!="] = Field(default="=", alias="oper")
    """
    Operator to use in the search

    = for inclusion

    != for exclusion
    """

    value: str = ""
    """
    Value to search for.

    TEXT searches have a OMOP concept_id (for example `8507`)

    NUM searches have a range value split by `|` (for example 1.0|3.0)
    """

    time: str | None = None
    """
    Time to search for.

    A time is a number followed by a colon and a unit.

    If the `|` is on the left of the value it was less than the number.

    If the `|` is on the right of the value it was greater than the number.

    For example:
    - 10|:AGE:Y (greater than 10 years)
    - 10|:TIME:M (greater than 10 months)
    - |10:TIME:M (less than 10 months)
    - |10:AGE:Y (less than 10 years)
    """

    secondary_modifier: list[int] | None = None
    """
    Secondary modifier to use in the search.

    This is used to on the provenance of the data on `ConditionOccurence`.

    A list of concept_ids, for example `[32020]`.
    """

    """
    Used to store parsed numeric values from range strings.
    """
    raw_range: str | None = None
    min_value: float | None = None
    max_value: float | None = None

    # Parsed time values
    time_value: str | None = None
    time_category: str | None = None
    time_unit: str | None = None
    left_value_time: str | None = None
    right_value_time: str | None = None

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
    )

    def model_post_init(self, __context: Any) -> None:
        """
        Initialize numeric values and parse time values after model creation

        Args:
            __context: The context of the model creation.

        Returns:
            None
        """
        # Parse numeric values for NUM type rules
        if self.type_ == "NUM":
            # For NUM type rules, the value might be in range format (1.0..3.0) 
            # or pipe-separated format (1.0|3.0)
            if ".." in self.value:
                self.min_value, self.max_value = self._parse_numeric(self.value)
            else:
                # Handle pipe-separated format (1.0|3.0)
                self.min_value, self.max_value = self._parse_pipe_separated(self.value)
            
            parts = self.varname.split("=")
            v = parts[1] if len(parts) > 1 else None
            self.raw_range = self.value
            self.value = v or ""
        else:
            # For non-NUM rules, parse range from raw_range if provided
            if self.raw_range and self.raw_range != "":
                self.min_value, self.max_value = self._parse_pipe_separated(self.raw_range)
            else:
                self.min_value, self.max_value = None, None

        # Parse time values if time is provided
        if self.time:
            self._parse_time()

    @staticmethod
    def _parse_numeric(value: str) -> tuple[float | None, float | None]:
        """
        Parse numeric values from range strings.

        Args:
            value (str): The value to parse.

        Returns:
            tuple[float | None, float | None]: The parsed numeric values.
        """
        pattern = re.compile(r"(-?\d*\.\d+|\d+|null)\.\.(-?\d*\.\d+|null)")
        if match := re.search(pattern, value):
            lower, upper = match.groups()
            try:
                min_value = float(lower)
            except ValueError:
                min_value = None
            try:
                max_value = float(upper)
            except ValueError:
                max_value = None
            return min_value, max_value
        return None, None

    @staticmethod
    def _parse_pipe_separated(value: str) -> tuple[float | None, float | None]:
        """
        Parse pipe-separated numeric values (e.g., "1.0|3.0").

        Args:
            value (str): The value to parse.

        Returns:
            tuple[float | None, float | None]: The parsed numeric values.
        """
        try:
            min_str, max_str = value.split("|")
            min_value = float(min_str) if min_str else None
            max_value = float(max_str) if max_str else None
            return min_value, max_value
        except (ValueError, AttributeError):
            return None, None

    def _parse_time(self) -> None:
        """
        Parse time string into components.
        
        Time format: "value|:CATEGORY:UNIT" or "|value:CATEGORY:UNIT"
        Examples:
        - "10|:AGE:Y" (greater than 10 years)
        - "|10:TIME:M" (less than 10 months)
        """
        if not self.time:
            return
            
        try:
            time_value, time_category, time_unit = self.time.split(":")
            self.time_value = time_value
            self.time_category = time_category
            self.time_unit = time_unit
            
            # Parse left and right values from time_value
            if "|" in time_value:
                left_value, right_value = time_value.split("|")
                self.left_value_time = left_value if left_value else ""
                self.right_value_time = right_value if right_value else ""
            else:
                self.left_value_time = time_value
                self.right_value_time = ""
        except (ValueError, AttributeError):
            # If parsing fails, set all values to None
            self.time_value = None
            self.time_category = None
            self.time_unit = None
            self.left_value_time = None
            self.right_value_time = None
