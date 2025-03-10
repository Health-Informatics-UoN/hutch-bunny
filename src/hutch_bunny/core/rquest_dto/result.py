from typing import List
from hutch_bunny.core.rquest_dto.base_dto import BaseDto
from hutch_bunny.core.rquest_dto.file import File


class RquestResult(BaseDto):
    """
    This class represents the result of an RQuest query.
    """

    def __init__(
        self,
        uuid: str,
        status: str,
        collection_id: str,
        count: int = 0,
        datasets_count: int = 0,
        files: List[File] = None,
        message: str = "",
        protocol_version: str = "v2",
    ) -> None:
        self.uuid = uuid
        self.status = status
        self.count = count
        self.datasets_count = datasets_count
        self.files = files if files is not None else list()
        self.message = message
        self.collection_id = collection_id
        self.protocol_version = protocol_version

    def to_dict(self) -> dict:
        """Convert this `DistributionResult` object to a JSON serialisable `dict`.

        Returns:
            dict:
                the `dict` representing the result of a distribution query.
        """
        return {
            "status": self.status,
            "protocolVersion": self.protocol_version,
            "uuid": self.uuid,
            "queryResult": {
                "count": self.count,
                "datasetCount": self.datasets_count,
                "files": [f.to_dict() for f in self.files],
            },
            "message": self.message,
            "collection_id": self.collection_id,
        }
