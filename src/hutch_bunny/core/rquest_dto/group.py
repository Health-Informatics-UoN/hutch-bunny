from typing import List
from hutch_bunny.core.rquest_dto.base_dto import BaseDto
from hutch_bunny.core.rquest_dto.rule import Rule


class Group(BaseDto):
    """Python representation of a group based on [ItemList](https://schema.org/ItemList)."""

    def __init__(self, rules: List[Rule], rules_operator: str, **kwargs) -> None:
        self.rules = rules
        self.rules_operator = rules_operator

    def to_dict(self) -> dict:
        """Convert `Group` to `dict`.

        Returns:
            dict: `Group` as a `dict`.
        """
        return {
            "rules": [r.to_dict() for r in self.rules],
            "rules_oper": self.rules_operator,
        }

    @classmethod
    def from_dict(cls, dict_: dict):
        """Create a `Group` from dict.

        Args:
            dict_ (dict): Mapping containing the `Group`'s attributes.

        Returns:
            Self: `Group` object.
        """
        rules = [Rule.from_dict(r) for r in dict_.get("rules", [])]
        rules_operator = dict_.get("rules_oper", "")
        return cls(rules=rules, rules_operator=rules_operator)
