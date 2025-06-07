from pydantic import BaseModel, Field, field_validator
from hutch_bunny.core.rquest_dto.group import Group


class Cohort(BaseModel):
    """Cohort - represents a collection of groups with an operator to combine them"""

    groups: list[Group]
    groups_operator: str = Field(alias="groups_oper")

    model_config = {
        "populate_by_name": True,
        "arbitrary_types_allowed": True,
    }

    @field_validator("groups", mode="before")
    @classmethod
    def validate_groups(cls, v: list[dict]) -> list[Group]:
        """Validate and convert the list of group dictionaries to Group objects.
        This ensures proper nested validation of groups and their rules.

        Args:
            v (list[dict]): List of group dictionaries to validate

        Returns:
            list[Group]: List of validated Group objects
        """
        if isinstance(v, list):
            return [Group.model_validate(g) for g in v]
        return v

    def to_dict(self) -> dict[str, list[dict[str, str | bool | float]] | str]:
        """Convert `Cohort` to `dict`

        Returns:
            dict[str, list[dict[str, str | bool | float]] | str]: The `Cohort` as a `dict`
        """
        return self.model_dump(by_alias=True)
