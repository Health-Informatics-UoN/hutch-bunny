
import pytest
import base64
from unittest.mock import patch, MagicMock
from hutch_bunny.core.services.metadata_service import MetadataService
from hutch_bunny.core.settings import DaemonSettings


@pytest.fixture
def mock_settings() -> MagicMock:
    """Create a mock settings instance."""
    mock = MagicMock(spec=DaemonSettings)
    mock.COLLECTION_ID = "test_collection"
    mock.ROUNDING_TARGET = 10
    mock.LOW_NUMBER_SUPPRESSION_THRESHOLD = 150
    return mock


@pytest.fixture
def metadata_service(mock_settings: MagicMock) -> MetadataService:
    """Create a test metadata service instance with mocked settings."""
    with patch(
        "hutch_bunny.core.services.metadata_service.DaemonSettings",
        return_value=mock_settings,
    ):
        return MetadataService()


def test_generate_metadata(metadata_service: MetadataService) -> None:
    """Test metadata file generation."""
    metadata_file = metadata_service.generate_metadata()


    # Check file properties
    assert metadata_file.name == "metadata.bcos"
    assert (
        metadata_file.description
        == "Metadata for the result of code.distribution analysis"
    )
    assert metadata_file.sensitive is False
    assert metadata_file.type_ == "BCOS"
    assert metadata_file.size > 0

    # Check that data is base64 encoded and contains expected content
    decoded_data = base64.b64decode(metadata_file.data).decode("utf-8")
    assert "BIOBANK\tPROTOCOL\tOS\tBCLINK\tDATAMODEL\tROUNDING\tTHRESHOLD" in decoded_data
    assert "test_collection" in decoded_data  # biobank (collection_id)
    assert "Bunny" in decoded_data
    assert "1.0.5" in decoded_data  # bclink (version)
    assert "OMOP" in decoded_data
    assert "10" in decoded_data  # rounding
    assert "150" in decoded_data  # threshold


def test_metadata_service_initialization(mock_settings: MagicMock) -> None:
    """Test metadata service initialization."""
    with patch(
        "hutch_bunny.core.services.metadata_service.DaemonSettings",
        return_value=mock_settings,
    ):
        service = MetadataService()
        assert service.settings == mock_settings
