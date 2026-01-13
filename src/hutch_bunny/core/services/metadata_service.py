import base64
from importlib.metadata import version

from hutch_bunny.core.rquest_models.file import File
from hutch_bunny.core.settings import DaemonSettings
from hutch_bunny.core.obfuscation import encode_output



class MetadataService:
    """Service for generating metadata for distribution query results."""

    def __init__(self) -> None:
        self.settings = DaemonSettings()

    def generate_metadata(self, encode_result: bool = True) -> File:
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
        # rounding = str(self.settings.ROUNDING_TARGET)
        # threshold = str(self.settings.LOW_NUMBER_SUPPRESSION_THRESHOLD)

        #hard coding these to 0 for now until wider conversations can be had
        rounding="0"
        threshold="0"


        # Format metadata to the expected format
        header = "BIOBANK\tPROTOCOL\tOS\tBCLINK\tDATAMODEL\tROUNDING\tTHRESHOLD"
        data_line = f"{biobank}\t{protocol}\t{os_info}\t{bclink}\t{datamodel}\t{rounding}\t{threshold}"

        metadata = f"{header}\n{data_line}"

        if encode_result: 
            metadata, metadata_size = encode_output(metadata)
        else: 
            metadata_size = len(metadata.encode("utf-8")) / 1000

        return File(
            data=metadata,
            description="Metadata for the result of code.distribution analysis",
            name="metadata.bcos",
            sensitive=False,
            reference="",
            size=metadata_size,
            type_="BCOS",
        )
