import pytest
from hutch_bunny.core.rquest_models.distribution import (
    DistributionQuery,
    DistributionQueryType,
)


def test_distribution_query_type_enum() -> None:
    """Test the DistributionQueryType enum values and properties."""
    # Test valid enum values
    assert DistributionQueryType.DEMOGRAPHICS.value == "DEMOGRAPHICS"
    assert DistributionQueryType.GENERIC.value == "GENERIC"
    assert DistributionQueryType.ICD_MAIN.value == "ICD-MAIN"

    # Test file_name property for valid types
    assert DistributionQueryType.DEMOGRAPHICS.file_name == "demographics.distribution"
    assert DistributionQueryType.GENERIC.file_name == "code.distribution"

    # Test file_name property for invalid type
    with pytest.raises(
        ValueError,
        match="No file name mapping for query type: DistributionQueryType.ICD_MAIN",
    ):
        _ = DistributionQueryType.ICD_MAIN.file_name


def test_distribution_query_creation() -> None:
    """Test creating valid DistributionQuery instances."""
    # Test valid query creation
    query = DistributionQuery(
        owner="user1",
        code=DistributionQueryType.DEMOGRAPHICS,
        analysis="DISTRIBUTION",
        uuid="test-uuid",
        collection="test-collection",
    )

    assert query.owner == "user1"
    assert query.code == DistributionQueryType.DEMOGRAPHICS
    assert query.analysis == "DISTRIBUTION"
    assert query.uuid == "test-uuid"
    assert query.collection == "test-collection"


def test_distribution_query_validation() -> None:
    """Test validation of DistributionQuery fields."""
    # Test invalid code
    with pytest.raises(
        ValueError, match="'INVALID' is not a valid distribution query type"
    ):
        DistributionQuery(
            owner="user1",
            code="INVALID",  # type: ignore
            analysis="DISTRIBUTION",
            uuid="test-uuid",
            collection="test-collection",
        )

    # Test invalid analysis
    with pytest.raises(ValueError):
        DistributionQuery(
            owner="user1",
            code=DistributionQueryType.DEMOGRAPHICS,
            analysis="INVALID",  # type: ignore
            uuid="test-uuid",
            collection="test-collection",
        )


def test_distribution_query_required_fields() -> None:
    """Test that all required fields are enforced."""
    # Test missing required field
    with pytest.raises(ValueError):
        DistributionQuery(
            owner="user1",
            code=DistributionQueryType.DEMOGRAPHICS,
            analysis="DISTRIBUTION",
            collection="test-collection",
        )

    with pytest.raises(ValueError):
        DistributionQuery(
            owner="user1",
            code=DistributionQueryType.DEMOGRAPHICS,
            analysis="DISTRIBUTION",
            uuid="test-uuid",
        )
