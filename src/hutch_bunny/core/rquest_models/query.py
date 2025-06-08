from pydantic import BaseModel, field_validator
from hutch_bunny.core.enums import DistributionQueryType
from hutch_bunny.core.rquest_models.cohort import Cohort


class AvailabilityQuery(BaseModel):
    """
    The top-level structure of an availability query request.
    """

    cohort: Cohort
    """
    Cohort of the query, which contains the query groups and their rules.
    """

    uuid: str
    """
    UUID of the query.
    """

    owner: str
    """
    Owner of the query. 
    """

    collection: str
    """
    Collection of the query.
    """

    protocol_version: str
    """
    Protocol version of the query.
    """

    char_salt: str
    """
    Char salt of the query.
    """

    model_config = {
        "populate_by_name": True,
        "arbitrary_types_allowed": True,
    }

    @field_validator("cohort", mode="before")
    @classmethod
    def validate_cohort(cls, v: dict[str, str | float | bool]) -> Cohort:
        """Validate and convert the cohort dictionary to a Cohort object.
        This ensures proper nested validation of the entire query structure.

        Args:
            v (dict[str, str | float | bool]): The cohort dictionary to validate, containing groups and their rules

        Returns:
            Cohort: The validated Cohort object with all nested structures validated
        """
        if isinstance(v, dict):
            return Cohort.model_validate(v)
        return v


class DistributionQuery(BaseModel):
    """
    The top-level structure of a distribution query request.
    """

    owner: str
    """
    Owner of the query.
    """

    code: DistributionQueryType
    """
    Code of the query.
    """

    analysis: str
    """
    Analysis of the query.
    """

    uuid: str
    """
    UUID of the query.
    """

    collection: str

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
