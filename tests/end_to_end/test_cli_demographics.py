import subprocess
import pytest
import os
import json
import sys
import base64
from dataclasses import dataclass


@dataclass
class DistributionTestCase:
    json_file_path: str
    modifiers: str
    expected_count: int  # Number of lines in the output file
    expected_gender_count: int
    expected_male: int
    expected_female: int


test_cases = [
    DistributionTestCase(
        json_file_path="tests/queries/distribution/demographics.json",
        modifiers="[]",
        expected_count=1,
        expected_gender_count=100,
        expected_male=40,
        expected_female=60,
    ),
    DistributionTestCase(
        json_file_path="tests/queries/distribution/demographics.json",
        modifiers='[{"id": "Rounding", "nearest": 0}]',
        expected_count=1,
        expected_gender_count=99,
        expected_male=44,
        expected_female=55,
    ),
    DistributionTestCase(
        json_file_path="tests/queries/distribution/demographics.json",
        modifiers='[{"id": "Rounding", "nearest": 100}]',
        expected_count=1,
        expected_gender_count=100,
        expected_male=0,
        expected_female=100,
    ),
    DistributionTestCase(
        json_file_path="tests/queries/distribution/demographics.json",
        modifiers='[{"id": "Rounding", "nearest": 10}, {"id": "Low Number Suppression", "threshold": 10}]',
        expected_count=1,
        expected_gender_count=100,
        expected_male=40,
        expected_female=60,
    ),
    DistributionTestCase(
        json_file_path="tests/queries/distribution/demographics.json",
        modifiers='[{"id": "Rounding", "nearest": 10}, {"id": "Low Number Suppression", "threshold": 50}]',
        expected_count=1,
        expected_gender_count=60,
        expected_male=0,
        expected_female=60,
    ),
]


@pytest.mark.end_to_end
@pytest.mark.parametrize("test_case", test_cases)
def test_cli_demographics(test_case: DistributionTestCase) -> None:
    """
    Test the CLI demographics command.

    This test will run the CLI demographics command with the given JSON file and modifiers,
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
        assert output_data["queryResult"]["count"] == test_case.expected_count
        assert output_data["queryResult"]["datasetCount"] == 1
        assert output_data["message"] == ""
        assert output_data["collection_id"] == "collection_id"

        # Assert expected values in output file
        file_data = base64.b64decode(
            output_data["queryResult"]["files"][0]["file_data"]
        ).decode("utf-8")
        lines = file_data.split("\n")
        assert (
            lines[0]
            == "BIOBANK	CODE	DESCRIPTION	COUNT	MIN	Q1	MEDIAN	MEAN	Q3	MAX	ALTERNATIVES	DATASET	OMOP	OMOP_DESCR	CATEGORY"
        )

        # Assert the gender count and gender distribution
        fields = lines[1].split("\t")
        assert int(fields[3]) == test_case.expected_gender_count
        assert (
            fields[10]
            == f"^MALE|{test_case.expected_male}^FEMALE|{test_case.expected_female}^"
        )

    # Clean up
    os.remove(output_file_path)
