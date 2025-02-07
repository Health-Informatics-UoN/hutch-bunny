import re
from typing import Any, Tuple, Union


class Rule:
    def __init__(
        self,
        value: Any = None,
        type_: str = "",
        time: Union[str, None] = None,
        varname: str = "",
        operator: str = "",
        raw_range: str = "",
        varcat: str = None,
        secondary_modifier: [] = None,
        **kwargs,
    ) -> None:
        self.raw_range = raw_range
        self.value = value
        self.type_ = type_
        self.time = time
        self.varname = varname
        self.operator = operator
        self.varcat = varcat
        self.secondary_modifier = secondary_modifier

        if self.type_ == "NUM":
            self.min_value, self.max_value = self._parse_numeric(self.value)
            _, v = self.varname.split("=")
            self.raw_range = self.value
            self.value = v
        else:
            self.min_value, self.max_value = None, None

    def to_dict(self) -> dict:
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
    def from_dict(cls, dict_: dict):
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

        return cls(
            type_=type_,
            value=value,
            time=time,
            varname=varname,
            operator=operator,
            varcat=varcat,
            secondary_modifier=secondary_modifier,
        )

    def _parse_numeric(
        self, value: str
    ) -> Tuple[Union[float, None], Union[float, None]]:
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
