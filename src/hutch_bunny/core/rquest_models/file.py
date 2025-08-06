from typing import Literal
from pydantic import BaseModel, ConfigDict, Field


class File(BaseModel):
    """
    A file as part of a RquestResult.

    Specifies the file details of a query.
    """

    name: Literal["demographics.distribution", "code.distribution", "metadata.bcos"] = (
        Field(alias="file_name")
    )
    """
    Name of the file.

    `demographics.distribution` for demographics distribution.
    `code.distribution` for code distribution.
    """

    data: str = Field(alias="file_data")
    """
    Data of the file.

    Base64 encoded string containing the file data of the query, in a TSV format.

    See: https://hutch.health/concepts/distribution#response-schema for more details.
    """

    description: str = Field(alias="file_description")
    """
    User friendly description of the file.
    """

    reference: str = Field(alias="file_reference")
    """
    Reference of the file. This is not used by Bunny.
    """

    sensitive: bool = Field(alias="file_sensitive")
    """
    Sensitive flag of the file - whether the file contains sensitive data.
    """

    size: float = Field(alias="file_size")
    """
    Size of the file in KB.
    """

    type_: Literal["BCOS"] = Field(alias="file_type")
    """
    Type of the file. 

    `BCOS` for tab separated binary content.
    """

    model_config = ConfigDict(
        populate_by_name=True,
    )
