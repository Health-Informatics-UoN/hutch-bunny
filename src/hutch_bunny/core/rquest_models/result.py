from pydantic import BaseModel, Field
from hutch_bunny.core.rquest_models.file import File


class QueryResult(BaseModel):
    """Nested model for query result data"""

    count: int
    datasetCount: int = Field(alias="datasetCount")
    files: list[File] = Field(default_factory=list)


class RquestResult(BaseModel):
    """
    This class represents the result of an RQuest query.
    """

    uuid: str
    status: str
    collection_id: str
    count: int = 0
    datasets_count: int = Field(default=0, alias="datasetCount")
    files: list[File] = Field(default_factory=list)
    message: str = ""
    protocol_version: str = Field(default="v2", alias="protocolVersion")

    model_config = {
        "populate_by_name": True,
        "arbitrary_types_allowed": True,
    }

    def to_dict(self) -> dict[str, str | int | dict[str, str | int | float | bool]]:
        """Convert this `DistributionResult` object to a JSON serialisable `dict`.

        Returns:
            dict[str, str | int | dict[str, str | int | float | bool]]: The dict representing the result of a distribution query.
        """
        query_result = QueryResult(
            count=self.count, datasetCount=self.datasets_count, files=self.files
        )

        result = self.model_dump(
            by_alias=True, exclude={"count", "datasets_count", "files"}
        )
        result["queryResult"] = query_result.model_dump(mode="json", by_alias=True)

        return result
