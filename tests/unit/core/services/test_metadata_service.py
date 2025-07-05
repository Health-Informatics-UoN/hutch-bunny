import pytest
import base64
from hutch_bunny.core.services.metadata_service import MetadataService
from hutch_bunny.core.settings import Settings


@pytest.fixture
def settings() -> Settings:
    """Create a test settings instance."""
    return Settings(
        ROUNDING_TARGET=10,
        LOW_NUMBER_SUPPRESSION_THRESHOLD=150,
    )


@pytest.fixture
def metadata_service(settings: Settings) -> MetadataService:
    """Create a test metadata service instance."""
    return MetadataService(settings)


def test_generate_metadata(metadata_service: MetadataService) -> None:
    """Test metadata file generation."""
    metadata_file = metadata_service.generate_metadata("test-uuid")

    # Check file properties
    assert metadata_file.name == "code.distribution"
    assert (
        metadata_file.description
        == "Metadata for the result of code.distribution analysis"
    )
    assert metadata_file.sensitive is False
    assert metadata_file.type_ == "BCOS"
    assert metadata_file.size > 0

    # Check that data is base64 encoded and contains expected content
    decoded_data = base64.b64decode(metadata_file.data).decode("utf-8")
    assert "BIOBANK PROTOCOL OS BCLINK DATAMODEL ROUNDING THRESHOLD" in decoded_data
    assert "test-uuid" in decoded_data
    assert "gened" in decoded_data
    assert "Rocky Linux" in decoded_data
    assert "6.3.4" in decoded_data
    assert "OMOP" in decoded_data
    assert "10" in decoded_data
    assert "150" in decoded_data


def test_metadata_service_initialization(settings: Settings) -> None:
    """Test metadata service initialization."""
    service = MetadataService(settings)
    assert service.settings == settings
