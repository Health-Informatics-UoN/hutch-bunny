import re
from typing import Any, Tuple, List, Optional, Dict


class Rule:
    def __init__(
        self,
        value: Optional[Any] = None,
        type_: str = "",
        time: Optional[str] = None,
        varname: str = "",
        operator: str = "",
        raw_range: str = "",
        varcat: Optional[str] = None,
        secondary_modifier: Optional[List[str]] = None,
        **kwargs,
    ) -> None:
        self.raw_range = raw_range
        self.value = value
        self.type_ = type_
        self.time = time
        self.varname = varname
        self.operator = operator
        self.varcat = varcat
        self.secondary_modifier = secondary_modifier or []

        # Parse time if present
        # "time" : "|1:TIME:M" in the payload means that
        # if the | is on the left of the value it was less than 1 month
        # if it was "1|:TIME:M" it would mean greater than one month
        self.time_value: Optional[str] = None
        self.time_category: Optional[str] = None
        self.left_value_time: Optional[str] = None
        self.right_value_time: Optional[str] = None
        if self.time:
            time_value, time_category, _ = self.time.split(":")
            self.time_value = time_value
            self.time_category = time_category
            self.left_value_time, self.right_value_time = time_value.split("|")

        if self.type_ == "NUM":
            self.min_value, self.max_value = self._parse_numeric(self.value)
            parts = self.varname.split("=")
            v = parts[1] if len(parts) > 1 else None
            self.raw_range = self.value
            self.value = v
        else:
            self.min_value, self.max_value = self._parse_raw_range(raw_range)

    def _parse_raw_range(
        self, raw_range: str
    ) -> Tuple[Optional[float], Optional[float]]:
        """Parse the raw_range string into min and max values.

        Args:
            raw_range: String in format "min|max" where min and max are optional

        Returns:
            Tuple of (min_value, max_value) where either can be None
        """
        if not raw_range:
            return None, None

        try:
            min_str, max_str = raw_range.split("|")
            min_value = float(min_str) if min_str else None
            max_value = float(max_str) if max_str else None
            return min_value, max_value
        except ValueError:
            return None, None

    def to_dict(self) -> Dict[str, Any]:
        """Convert `Rule` to `dict`.

        Returns:
            dict: `Rule` as a `dict`.
        """
        varname = self.varname
        value = self.value
        if self.type_ == "NUM":
            varname = f"OMOP={value}"
            value = f"{self.min_value}..{self.max_value}"
        dict_ = {
            "varname": varname,
            "type": self.type_,
            "oper": self.operator,
            "value": value,
        }
        return dict_

    @classmethod
    def from_dict(cls, dict_: Dict[str, Any]) -> "Rule":
        """Create a `Rule` from RO-Crate JSON.

        Args:
            dict_ (dict): Mapping containing the `Rule`'s attributes.

        Returns:
            Self: `Rule` object.
        """
        type_ = dict_.get("type", "")
        value = dict_.get("value")
        time = dict_.get("time")
        varname = dict_.get("varname", "")
        operator = dict_.get("oper", "")
        varcat = dict_.get("varcat", "")
        secondary_modifier = dict_.get("secondary_modifier", [])
        raw_range = dict_.get("raw_range", "")

        return cls(
            type_=type_,
            value=value,
            time=time,
            varname=varname,
            operator=operator,
            varcat=varcat,
            secondary_modifier=secondary_modifier,
            raw_range=raw_range,
        )

    def _parse_numeric(self, value: str) -> Tuple[Optional[float], Optional[float]]:
        pattern = re.compile(r"(-?\d*\.\d+|\d+|null)\.\.(-?\d*\.\d+|null)")
        # Try and parse min and max values, then return them
        if match := re.search(pattern, value):
            lower, upper = match.groups()
            # parse lower bound
            try:
                min_value = float(lower)
            except ValueError:
                min_value = None
            # parse upper bound
            try:
                max_value = float(upper)
            except ValueError:
                max_value = None
            return min_value, max_value

        return None, None
