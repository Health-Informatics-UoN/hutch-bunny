from pydantic import BaseModel, ConfigDict, field_validator
from hutch_bunny.core.rquest_models.cohort import Cohort


class AvailabilityQuery(BaseModel):
    """
    The top-level structure of an Availability Query request.

    Enables the user to query the availability of a cohort in a collection.
    """

    cohort: Cohort
    """
    Cohort of the query, which contains the query groups and their rules.
    """

    uuid: str
    """
    Unique identifier of the query.
    """

    owner: str
    """
    Owner of the query. Not the user itself, but the ID of the connection - default is `user1`.
    """

    collection: str
    """
    Collection of the query. This is the unqiue collection that the query is being run on.
    """

    protocol_version: str
    """
    Protocol version of the query, for example `v2`.
    """

    char_salt: str
    """
    Char salt of the query used for hashing.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
    )

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
