from pydantic import BaseModel, Field


class File(BaseModel):
    """Python representation of an RQuest File"""

    data: str = Field(alias="file_data")
    description: str = Field(alias="file_description")
    name: str = Field(alias="file_name")
    reference: str = Field(alias="file_reference")
    sensitive: bool = Field(alias="file_sensitive")
    size: float = Field(alias="file_size")
    type_: str = Field(alias="file_type")

    model_config = {
        "populate_by_name": True,
    }

    def to_dict(self) -> dict[str, str | bool | float]:
        """Convert to dictionary using field aliases.

        Returns:
            dict[str, str | bool | float]: Dictionary representation using field aliases
        """
        return self.model_dump(by_alias=True)
