from enum import Enum
from typing import Literal
from pydantic import BaseModel, field_validator


class DistributionQueryType(str, Enum):
    """Types of distribution queries."""

    DEMOGRAPHICS = "DEMOGRAPHICS"
    GENERIC = "GENERIC"
    ICD_MAIN = "ICD-MAIN"

    @property
    def file_name(self) -> Literal["demographics.distribution", "code.distribution"]:
        """Get the corresponding file name for this distribution type."""
        mapping = {
            DistributionQueryType.DEMOGRAPHICS: "demographics.distribution",
            DistributionQueryType.GENERIC: "code.distribution",
        }
        if self not in mapping:
            raise ValueError(f"No file name mapping for query type: {self}")
        return mapping[self]  # type: ignore


class DistributionQuery(BaseModel):
    """
    The top-level structure of a distribution query request.
    """

    owner: str
    """
    Owner of the query. Not the user itself, but the ID of the connection - default is `user1`.
    """

    code: DistributionQueryType
    """
    Code of the query. This is the type of distribution query to run.
    """

    analysis: Literal["DISTRIBUTION"]
    """
    Analysis of the query. Currently only `DISTRIBUTION` is supported.
    """

    uuid: str
    """
    Unique identifier of the query.
    """

    collection: str
    """
    Collection of the query. This is the unique collection that the query is being run on.
    """

    @field_validator("code", mode="before")
    @classmethod
    def validate_code(cls, v: str) -> DistributionQueryType:
        """Validate that the code is a valid distribution query type.

        Args:
            v (str): The code value to validate

        Raises:
            ValueError: If the code is not a valid distribution query type

        Returns:
            DistributionQueryType: The validated enum value
        """
        try:
            return DistributionQueryType(v)
        except ValueError:
            valid_values = [t.value for t in DistributionQueryType]
            raise ValueError(
                f"'{v}' is not a valid distribution query type. Valid values are: {', '.join(repr(v) for v in valid_values)}"
            )
