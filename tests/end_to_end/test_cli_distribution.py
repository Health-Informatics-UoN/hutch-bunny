import subprocess
import pytest
import os
import json
import sys
import base64
from dataclasses import dataclass
from typing import Dict

@dataclass
class DistributionTestCase:
    json_file_path: str
    modifiers: str
    expected_values: Dict[str, int]  # Map of OMOP codes to their expected counts

test_cases = [
    DistributionTestCase(
        json_file_path="tests/queries/distribution/distribution.json",
        modifiers="[]",
        expected_values={
            "8507": 40,  # MALE
            "8532": 60,  # FEMALE
            "38003564": 40,  # Not Hispanic
            "38003563": 60,  # Hispanic
        }
    ),
    DistributionTestCase(
        json_file_path="tests/queries/distribution/distribution.json",
        modifiers='[{"id": "Rounding", "nearest": 0}]',
        expected_values={
            "8507": 44,
            "8532": 55,
            "38003564": 41,
            "38003563": 58,
        }
    ),
]

@pytest.mark.end_to_end
@pytest.mark.parametrize("test_case", test_cases)
def test_cli_distribution(test_case: DistributionTestCase) -> None:
    """
    Test the CLI distribution command.

    This test will run the CLI availability command with the given JSON file and modifiers,
    and assert the output is as expected.

    Args:
        test_case (DistributionTestCase): The test case containing the JSON file path, modifiers, and expected counts.

    Returns:
        None
    """
    # Arrange
    output_file_path = "tests/queries/distribution/output.json"

    # Act
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "hutch_bunny.cli",
            "--body",
            test_case.json_file_path,
            "--modifiers",
            test_case.modifiers,
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
        assert output_data["queryResult"]["datasetCount"] == 1
        assert output_data["message"] == ""
        assert output_data["collection_id"] == "collection_id"

        file_data = base64.b64decode(output_data["queryResult"]["files"][0]["file_data"]).decode("utf-8")
        lines = file_data.split("\n")
        assert lines[0] == "BIOBANK	CODE	COUNT	DESCRIPTION	MIN	Q1	MEDIAN	MEAN	Q3	MAX	ALTERNATIVES	DATASET	OMOP	OMOP_DESCR	CATEGORY"
        assert lines[1] == f"collection_id	OMOP:38003564	{test_case.expected_values['38003564']}										38003564	Not Hispanic or Latino	Ethnicity"
        assert lines[2] == f"collection_id	OMOP:38003563	{test_case.expected_values['38003563']}										38003563	Hispanic or Latino	Ethnicity"
        assert lines[3] == f"collection_id	OMOP:8532	{test_case.expected_values['8532']}										8532	FEMALE	Gender"
        assert lines[4] == f"collection_id	OMOP:8507	{test_case.expected_values['8507']}										8507	MALE	Gender"

        # Verify counts
        for line in lines[1:]:  # Skip header
            fields = line.split("\t")
            omop_code = fields[12]  # OMOP column

            # Validate count is an integer
            count_str = fields[2]  # COUNT column as string
            assert count_str.isdigit(), f"Expected an integer count, but got: {count_str}"
            count = int(count_str)  # Convert to int after validation
            assert count == test_case.expected_values[omop_code]

    # Clean up
    os.remove(output_file_path)
