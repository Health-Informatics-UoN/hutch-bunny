import re
from typing import Any, Literal
from pydantic import BaseModel, Field


class Rule(BaseModel):
    """Rule"""

    value: str = ""
    type_: Literal["NUM", "TEXT"] = Field(default="TEXT", alias="type")
    time: str | None = None
    varname: str = ""
    operator: Literal["=", "!="] = Field(default="=", alias="oper")
    raw_range: str | None = None
    varcat: Literal["Person", "Condition", "Observation", "Drug", "Measurement"]
    secondary_modifier: list[int] | None = None
    min_value: float | None = None
    max_value: float | None = None

    model_config = {
        "populate_by_name": True,
        "arbitrary_types_allowed": True,
    }

    def model_post_init(self, __context: Any) -> None:
        """Initialize numeric values after model creation"""
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
        """Parse numeric values from range strings"""
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
