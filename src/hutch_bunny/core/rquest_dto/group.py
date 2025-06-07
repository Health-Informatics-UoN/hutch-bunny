from typing import Dict, List
from pydantic import BaseModel, Field, field_validator
from hutch_bunny.core.rquest_dto.rule import Rule


class Group(BaseModel):
    """Group - represents a collection of rules with an operator to combine them"""

    rules: List[Rule]
    rules_operator: str = Field(alias="rules_oper")

    model_config = {
        "populate_by_name": True,
        "arbitrary_types_allowed": True,
    }

    @field_validator("rules", mode="before")
    @classmethod
    def validate_rules(cls, v: List[dict]) -> List[Rule]:
        """Validate and convert the list of rule dictionaries to Rule objects.
        This ensures proper validation of each rule's fields.

        Args:
            v (List[dict]): List of rule dictionaries to validate

        Returns:
            List[Rule]: List of validated Rule objects
        """
        if isinstance(v, list):
            return [Rule.model_validate(r) for r in v]
        return v

    def to_dict(self) -> Dict[str, List[Dict[str, str]] | str]:
        """Convert `Group` to `dict`.

        Returns:
            Dict[str, List[Dict[str, str]] | str]: `Group` as a `dict`.
        """
        return self.model_dump(by_alias=True)
