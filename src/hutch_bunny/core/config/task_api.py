from pydantic_settings import BaseSettings
from pydantic import Field, field_validator, ValidationInfo
from typing import Optional, Literal
# from hutch_bunny.core.logger import logger


class TaskApiSettings(BaseSettings):
    """
    Task API connection and configuration settings
    """

    TASK_API_ENFORCE_HTTPS: bool = Field(
        description="Whether to enforce HTTPS for the task API", default=True
    )
    TASK_API_BASE_URL: str = Field(description="The base URL of the task API")
    TASK_API_USERNAME: str = Field(description="The username for the task API")
    TASK_API_PASSWORD: str = Field(description="The password for the task API")
    TASK_API_TYPE: Optional[Literal["a", "b"]] = Field(
        description="The type of task API to use", default=None
    )
    COLLECTION_ID: str = Field(description="The collection ID")

    @field_validator("TASK_API_BASE_URL")
    def validate_https_enforcement(cls, v: str, info: ValidationInfo) -> str:
        """
        Validates that HTTPS is used when TASK_API_ENFORCE_HTTPS is True.
        """
        enforce_https = info.data.get("TASK_API_ENFORCE_HTTPS", True)

        if not v.startswith("https://"):
            if enforce_https:
                raise ValueError(
                    "HTTPS is required for the task API but not used. Set TASK_API_ENFORCE_HTTPS to false if you are using a non-HTTPS connection."
                )
            # else:
            #     # logger.warning(
            #     #     "HTTPS is not used for the task API. This is not recommended in production environments."
            #     # )
        return v

