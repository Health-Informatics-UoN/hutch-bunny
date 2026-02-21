import pytest
import base64
from hutch_bunny.core.services.metadata_service import MetadataService
from importlib.metadata import version


@pytest.fixture
def metadata_service() -> MetadataService:
    """Create a test metadata service instance."""
    return MetadataService(collection_id="test_collection")


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
    assert (
        "BIOBANK\tPROTOCOL\tOS\tBCLINK\tDATAMODEL\tROUNDING\tTHRESHOLD" in decoded_data
    )
    assert "test_collection" in decoded_data  # biobank (collection_id)
    assert "Bunny" in decoded_data
    assert version("hutch-bunny") in decoded_data  # bclink (version)
    assert "OMOP" in decoded_data
    assert "0" in decoded_data  # rounding
    assert "0" in decoded_data  # threshold


def test_metadata_service_initialization() -> None:
    """Test metadata service initialization."""
    service = MetadataService(collection_id="test_collection")
    assert service.collection_id == "test_collection"
