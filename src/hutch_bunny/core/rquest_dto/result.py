from typing import List, TypedDict
from pydantic import BaseModel, Field
from hutch_bunny.core.rquest_dto.file import File


class QueryResult(TypedDict):
    """Type definition for the query result structure"""

    count: int
    datasetCount: int
    files: List[dict[str, str | bool | float]]


class RquestResult(BaseModel):
    """
    This class represents the result of an RQuest query.
    """

    uuid: str
    status: str
    collection_id: str
    count: int = 0
    datasets_count: int = Field(default=0, alias="datasetCount")
    files: List[File] = Field(default_factory=list)
    message: str = ""
    protocol_version: str = Field(default="v2", alias="protocolVersion")

    model_config = {
        "populate_by_name": True,
        "arbitrary_types_allowed": True,
    }

    def to_dict(self) -> dict[str, str | int | QueryResult]:
        """Convert this `DistributionResult` object to a JSON serialisable `dict`.

        Returns:
            dict[str, str | int | QueryResult]: The dict representing the result of a distribution query.
        """
        return {
            "status": self.status,
            "protocolVersion": self.protocol_version,
            "uuid": self.uuid,
            "queryResult": {
                "count": self.count,
                "datasetCount": self.datasets_count,
                "files": [f.model_dump(by_alias=True) for f in self.files],
            },
            "message": self.message,
            "collection_id": self.collection_id,
        }
