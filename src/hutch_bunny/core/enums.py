from enum import Enum


class DistributionQueryType(str, Enum):
    """Enum representing the types of distribution query types."""

    DEMOGRAPHICS = "DEMOGRAPHICS"
    GENERIC = "GENERIC"
    ICD_MAIN = "ICD-MAIN"
