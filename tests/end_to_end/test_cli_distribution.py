import subprocess
import pytest
import os
import json
import sys
import base64

# TODO: Fix tests that are failing
test_cases = [
    ("tests/queries/distribution/distribution.json", "[]", 4),
]  # type: ignore


@pytest.mark.end_to_end
@pytest.mark.parametrize("json_file_path, modifiers, expected_count", test_cases)
def test_cli_distribution(
    json_file_path: str, modifiers: str, expected_count: int
) -> None:
    """
    Test the CLI distribution command.

    This test will run the CLI availability command with the given JSON file and modifiers,
    and assert the output is as expected.

    Args:
        json_file_path (str): The path to the JSON file containing the query.
        modifiers (str): The modifiers to apply to the query.
        expected_count (int): The expected count of results.

    Returns:
        None
    """
    # Arrange
    output_file_path = "tests/queries/distribution/output.json"

    # Distribution file output
    file_data = "BIOBANK	CODE	COUNT	DESCRIPTION	MIN	Q1	MEDIAN	MEAN	Q3	MAX	ALTERNATIVES	DATASET	OMOP	OMOP_DESCR	CATEGORY"
    file_data += "\ncollection_id	OMOP:38003564	40										38003564	Not Hispanic or Latino	Ethnicity"
    file_data += "\ncollection_id	OMOP:38003563	60										38003563	Hispanic or Latino	Ethnicity"
    file_data += "\ncollection_id	OMOP:8507	40										8507	MALE	Gender"
    file_data += "\ncollection_id	OMOP:8532	60										8532	FEMALE	Gender"
    file_data_b64 = base64.b64encode(file_data.encode("utf-8")).decode("utf-8")
    file_size = len(file_data_b64) / 1000

    # Assert output file content
    file = {
        "file_name": "code.distribution",
        "file_data": file_data_b64,
        "file_description": "Result of code.distribution analysis",
        "file_size": file_size,
        "file_type": "BCOS",
        "file_sensitive": True,
        "file_reference": "",
    }

    # Act
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "hutch_bunny.cli",
            "--body",
            json_file_path,
            "--modifiers",
            modifiers,
            "--output",
            output_file_path,
        ],
        capture_output=True,
        text=True,
    )

    # Assert
    assert result.returncode == 0, f"CLI failed with error: {result.stderr}"

    # Assert output file
    assert os.path.exists(output_file_path), "Output file was not created."

    with open(output_file_path, "r") as f:
        output_data = json.load(f)

        # Assert expected keys
        assert "status" in output_data
        assert "protocolVersion" in output_data
        assert "uuid" in output_data
        assert "queryResult" in output_data
        assert "count" in output_data["queryResult"]
        assert "datasetCount" in output_data["queryResult"]
        assert "files" in output_data["queryResult"]
        assert "message" in output_data
        assert "collection_id" in output_data

        # Assert expected values
        assert output_data["status"] == "ok"
        assert output_data["protocolVersion"] == "v2"
        assert output_data["uuid"] == "unique_id"
        assert output_data["queryResult"]["count"] == expected_count
        assert output_data["queryResult"]["datasetCount"] == 1
        assert output_data["queryResult"]["files"] == [file]
        assert output_data["message"] == ""
        assert output_data["collection_id"] == "collection_id"


    # Clean up
    os.remove(output_file_path)
