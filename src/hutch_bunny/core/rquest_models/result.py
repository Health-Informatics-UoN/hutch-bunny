from typing import Literal
from pydantic import BaseModel, Field
from hutch_bunny.core.rquest_models.file import File


class QueryResult(BaseModel):
    """Nested model for query result data"""

    count: int
    datasetCount: int = Field(alias="datasetCount")
    files: list[File] = Field(default_factory=list)


class RquestResult(BaseModel):
    """
    RquestResult model.

    Specifies the result of a query.
    """

    uuid: str
    """
    UUID of the query. Supplied by the upstream API.
    """

    status: Literal["ok", "error"]
    """
    Status of the query.
    """

    collection_id: str
    """
    Collection ID of the query. Supplied by the upstream API.
    """

    count: int = 0
    """
    Result count of the query.
    """

    datasets_count: int = Field(default=0, alias="datasetCount")
    """
    Count of the datasets in the query.
    Bunny only returns 1 dataset.
    """

    files: list[File] = Field(default_factory=list)
    """
    Result files of the query.
    """

    message: str = ""
    """
    Message of the query. This is only used when the status is `error`.
    """

    protocol_version: str = Field(default="v2", alias="protocolVersion")
    """
    Protocol version of the query. Supplied by the upstream API.
    """

    model_config = {
        "populate_by_name": True,
        "arbitrary_types_allowed": True,
    }

    def to_dict(self) -> dict[str, str | int | dict[str, str | int | float | bool]]:
        """
        Convert `RquestResult` to a JSON serialisable `dict`.

        Returns:
            dict[str, str | int | dict[str, str | int | float | bool]]: The dict of result.
        """
        query_result = QueryResult(
            count=self.count, datasetCount=self.datasets_count, files=self.files
        )

        result = self.model_dump(
            by_alias=True, exclude={"count", "datasets_count", "files"}
        )
        result["queryResult"] = query_result.model_dump(mode="json", by_alias=True)

        return result
