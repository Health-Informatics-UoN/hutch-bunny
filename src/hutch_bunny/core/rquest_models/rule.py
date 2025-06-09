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

    varcat: Literal["Person", "Condition", "Observation", "Drug", "Measurement"]
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

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
    )

    def model_post_init(self, __context: Any) -> None:
        """
        Initialize numeric values after model creation

        Args:
            __context: The context of the model creation.

        Returns:
            None
        """
        if self.type_ == "NUM":
            self.min_value, self.max_value = self._parse_numeric(self.value)
            parts = self.varname.split("=")
            v = parts[1] if len(parts) > 1 else None
            self.raw_range = self.value
            self.value = v or ""
        else:
            self.min_value, self.max_value = None, None

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
