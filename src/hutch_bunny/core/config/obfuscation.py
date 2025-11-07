from pydantic_settings import BaseSettings
from pydantic import Field


class ObfuscationSettings(BaseSettings):
    """
    Obfuscation configuration settings
    """

    LOW_NUMBER_SUPPRESSION_THRESHOLD: int = Field(
        description="The threshold for low numbers", default=10
    )
    ROUNDING_TARGET: int = Field(description="The target for rounding", default=10)

