from pydantic import BaseModel, Field


class File(BaseModel):
    """
    A file as part of a RquestResult.

    Specifies the file details of a query.
    """

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
