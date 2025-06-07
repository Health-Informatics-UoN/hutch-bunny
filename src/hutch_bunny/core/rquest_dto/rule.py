import re
from typing import Any, Optional
from pydantic import BaseModel, Field, ValidationInfo, field_validator


class Rule(BaseModel):
    """Rule"""

    value: Any = None
    type_: str = Field(default="", alias="type")
    time: Optional[str] = None
    varname: str = ""
    operator: str = Field(default="", alias="oper")
    raw_range: str = ""
    varcat: Optional[str] = None
    secondary_modifier: Optional[list[Any]] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None

    model_config = {
        "populate_by_name": True,
        "arbitrary_types_allowed": True,
    }

    @field_validator("min_value", "max_value", mode="before")
    @classmethod
    def parse_numeric(cls, v: Optional[float], info: ValidationInfo) -> Optional[float]:
        """Parse numeric values from range strings for NUM type rules"""
        if info.data.get("type_") == "NUM":
            pattern = re.compile(r"(-?\d*\.\d+|\d+|null)\.\.(-?\d*\.\d+|null)")
            if match := re.search(pattern, info.data.get("value", "")):
                lower, upper = match.groups()
                try:
                    min_value = float(lower)
                except ValueError:
                    min_value = None
                try:
                    max_value = float(upper)
                except ValueError:
                    max_value = None
                return min_value if info.field_name == "min_value" else max_value
        return None
