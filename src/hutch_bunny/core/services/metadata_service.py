import base64
from hutch_bunny.core.rquest_models.file import File
from hutch_bunny.core.settings import DaemonSettings
from importlib.metadata import version


class MetadataService:
    """Service for generating metadata for distribution query results."""

    def __init__(self) -> None:
        self.settings = DaemonSettings()

    def generate_metadata(self) -> File:
        """
        Generate metadata for a distribution query result.

        Returns:
            File object containing the metadata
        """
        biobank = self.settings.COLLECTION_ID
        protocol = "Bunny"
        os_info = ""
        # version number
        bclink = version("hutch-bunny")
        datamodel = "OMOP"

        # TODO: these should only be sent if enabled to be sent..
        rounding = str(self.settings.ROUNDING_TARGET)
        threshold = str(self.settings.LOW_NUMBER_SUPPRESSION_THRESHOLD)

        # Format metadata to the expected format
        header = "BIOBANK\tPROTOCOL\tOS\tBCLINK\tDATAMODEL\tROUNDING\tTHRESHOLD"
        data_line = f"{biobank}\t{protocol}\t{os_info}\t{bclink}\t{datamodel}\t{rounding}\t{threshold}"

        metadata = f"{header}\n{data_line}"

        # Encode to base64
        metadata_b64_bytes = base64.b64encode(metadata.encode("utf-8"))
        metadata_size = len(metadata_b64_bytes) / 1000
        metadata_b64 = metadata_b64_bytes.decode("utf-8")

        return File(
            data=metadata_b64,
            description="Metadata for the result of code.distribution analysis",
            name="metadata.bcos",
            sensitive=False,
            reference="",
            size=metadata_size,
            type_="BCOS",
        )
