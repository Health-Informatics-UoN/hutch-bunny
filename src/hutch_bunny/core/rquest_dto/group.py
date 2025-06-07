from pydantic import BaseModel, Field, field_validator
from hutch_bunny.core.rquest_dto.rule import Rule


class Group(BaseModel):
    """Group - represents a collection of rules with an operator to combine them"""

    rules: list[Rule]
    rules_operator: str = Field(alias="rules_oper")

    model_config = {
        "populate_by_name": True,
        "arbitrary_types_allowed": True,
    }

    @field_validator("rules", mode="before")
    @classmethod
    def validate_rules(cls, v: list[dict[str, str | float | bool]]) -> list[Rule]:
        """Validate and convert the list of rule dictionaries to Rule objects.
        This ensures proper validation of each rule's fields.

        Args:
            v (list[dict]): List of rule dictionaries to validate

        Returns:
            list[Rule]: List of validated Rule objects
        """
        if isinstance(v, list):
            return [Rule.model_validate(r) for r in v]
        return v
