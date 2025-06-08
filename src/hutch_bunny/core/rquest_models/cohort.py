from typing import Literal
from pydantic import BaseModel, Field, field_validator
from hutch_bunny.core.rquest_models.group import Group


class Cohort(BaseModel):
    """
    Represents a collection of groups with an operator to combine them.
    """

    groups: list[Group]
    groups_operator: Literal["AND", "OR"] = Field(alias="groups_oper")

    model_config = {
        "populate_by_name": True,
        "arbitrary_types_allowed": True,
    }

    @field_validator("groups", mode="before")
    @classmethod
    def validate_groups(cls, v: list[dict[str, str | float | bool]]) -> list[Group]:
        """
        Validate and convert the list of group dictionaries to `Group` objects.

        Args:
            v (list[dict]): List of group dictionaries to validate.

        Returns:
            list[Group]: List of validated `Group` objects.
        """
        if isinstance(v, list):
            return [Group.model_validate(g) for g in v]
        return v
