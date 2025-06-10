from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, field_validator
from hutch_bunny.core.rquest_models.rule import Rule


class Group(BaseModel):
    """
    A group of rules with an operator to combine them.
    """

    rules: list[Rule]
    """
    Rules of the group.
    """

    rules_operator: Literal["AND", "OR"] = Field(alias="rules_oper")
    """
    Operator to combine the rules.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
    )

    @field_validator("rules", mode="before")
    @classmethod
    def validate_rules(cls, v: list[dict[str, str | float | bool]]) -> list[Rule]:
        """
        Validate and convert the list of rule dictionaries to `Rule` objects.

        Args:
            v (list[dict]): List of rule dictionaries to validate.

        Returns:
            list[Rule]: List of validated `Rule` objects.
        """
        if isinstance(v, list):
            return [Rule.model_validate(r) for r in v]
