import subprocess
from typing import Dict
import pytest
import os
import json
import sys
import base64
from dataclasses import dataclass


@dataclass
class DemographicsTestCase:
    json_file_path: str
    modifiers: str
    expected_count: int  # Number of lines in the output file
    expected_gender_count: int
    expected_values: Dict[str, Dict[str, int]]  # Maps category to its expected values


test_cases = [
    DemographicsTestCase(
        json_file_path="tests/queries/distribution/demographics.json",
        modifiers="[]",
        expected_count=2,
        expected_gender_count=100,
        expected_values={"SEX": {"MALE": 40, "FEMALE": 60}, "GENOMICS": {"No": 100}},
    ),
    DemographicsTestCase(
        json_file_path="tests/queries/distribution/demographics.json",
        modifiers='[{"id": "Rounding", "nearest": 0}]',
        expected_count=2,
        expected_gender_count=99,
        expected_values={"SEX": {"MALE": 44, "FEMALE": 55}, "GENOMICS": {"No": 99}},
    ),
    DemographicsTestCase(
        json_file_path="tests/queries/distribution/demographics.json",
        modifiers='[{"id": "Rounding", "nearest": 100}]',
        expected_count=2,
        expected_gender_count=100,
        expected_values={"SEX": {"MALE": 0, "FEMALE": 100}, "GENOMICS": {"No": 100}},
    ),
    DemographicsTestCase(
        json_file_path="tests/queries/distribution/demographics.json",
        modifiers='[{"id": "Rounding", "nearest": 10}, {"id": "Low Number Suppression", "threshold": 10}]',
        expected_count=2,
        expected_gender_count=100,
        expected_values={"SEX": {"MALE": 40, "FEMALE": 60}, "GENOMICS": {"No": 100}},
    ),
    DemographicsTestCase(
        json_file_path="tests/queries/distribution/demographics.json",
        modifiers='[{"id": "Rounding", "nearest": 10}, {"id": "Low Number Suppression", "threshold": 50}]',
        expected_count=2,
        expected_gender_count=60,
        expected_values={"SEX": {"FEMALE": 60}, "GENOMICS": {"No": 60}},
    ),
    DemographicsTestCase(
        json_file_path="tests/queries/distribution/demographics.json",
        modifiers='[{"id": "Rounding", "nearest": 0}, {"id": "Low Number Suppression", "threshold": 0}]',
        expected_count=2,
        expected_gender_count=99,
        expected_values={"SEX": {"MALE": 44, "FEMALE": 55}, "GENOMICS": {"No": 99}},
    ),
]


@pytest.mark.end_to_end
@pytest.mark.parametrize("test_case", test_cases)
def test_cli_demographics(test_case: DemographicsTestCase) -> None:
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

        # Assert file details
        assert (
            output_data["queryResult"]["files"][0]["file_name"]
            == "demographics.distribution"
        )
        assert output_data["queryResult"]["files"][0]["file_type"] == "BCOS"
        assert output_data["queryResult"]["files"][0]["file_sensitive"] is True
        assert (
            output_data["queryResult"]["files"][0]["file_description"]
            == "Result of code.distribution analysis"
        )
        assert output_data["queryResult"]["files"][0]["file_data"] is not None

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
        # Sample line:
        # collection_id	SEX	Sex	100							^MALE|40^FEMALE|60^	person			DEMOGRAPHICS
        # collection_id	GENOMICS	Genomics	100							^No|100^	person			DEMOGRAPHICS
        for i, line in enumerate(lines[1:], 1):  # Skip header line
            fields = line.split("\t")
            category = fields[1]  # e.g., "SEX" or "GENOMICS"

            # Assert the count for this category
            assert int(fields[3]) == test_case.expected_gender_count

            # Assert the distribution for this category
            values = fields[10].split("^")
            for value in values[1:-1]:  # Skip empty first and last elements
                if value:  # Skip empty values
                    gender, count = value.split("|")
                    assert int(count) == test_case.expected_values[category][gender]

    # Clean up
    os.remove(output_file_path)
