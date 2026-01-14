import subprocess
from typing import Dict, Any 
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
        expected_gender_count=1130,
        expected_values={"SEX": {"Male": 570, "Female": 560}, "GENOMICS": {"No": 1130}},
    ),
    DemographicsTestCase(
        json_file_path="tests/queries/distribution/demographics.json",
        modifiers='[{"id": "Rounding", "nearest": 0}]',
        expected_count=2,
        expected_gender_count=1130,
        expected_values={"SEX": {"Male": 572, "Female": 558}, "GENOMICS": {"No": 1130}},
    ),
    DemographicsTestCase(
        json_file_path="tests/queries/distribution/demographics.json",
        modifiers='[{"id": "Rounding", "nearest": 100}]',
        expected_count=2,
        expected_gender_count=1200,
        expected_values={"SEX": {"Male": 600, "Female": 600}, "GENOMICS": {"No": 1200}},
    ),
    DemographicsTestCase(
        json_file_path="tests/queries/distribution/demographics.json",
        modifiers='[{"id": "Rounding", "nearest": 10}, {"id": "Low Number Suppression", "threshold": 10}]',
        expected_count=2,
        expected_gender_count=1130,
        expected_values={"SEX": {"Male": 570, "Female": 560}, "GENOMICS": {"No": 1130}},
    ),
    DemographicsTestCase(
        json_file_path="tests/queries/distribution/demographics.json",
        modifiers='[{"id": "Rounding", "nearest": 10}, {"id": "Low Number Suppression", "threshold": 50}]',
        expected_count=2,
        expected_gender_count=1130,
        expected_values={"SEX": {"Male": 570, "Female": 560}, "GENOMICS": {"No": 1130}},
    ),
    DemographicsTestCase(
        json_file_path="tests/queries/distribution/demographics.json",
        modifiers='[{"id": "Rounding", "nearest": 0}, {"id": "Low Number Suppression", "threshold": 0}]',
        expected_count=2,
        expected_gender_count=1130,
        expected_values={"SEX": {"Male": 572, "Female": 558}, "GENOMICS": {"No": 1130}},
    ),
]


def assert_demographics_output(
    output_file_path: str, 
    expected_count: int, 
    expected_gender_count: int, 
    expected_values: dict[Any, Any], 
    encode_result: bool = True
) -> None: 
    """
    Validate the output file from the CLI distribution command.

    Args:
        output_file_path (str | Path): Path to the output JSON file.
        expected_count (int): Expected total count of records.
        expected_values (dict): Map of OMOP codes to expected counts.
    """
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
    file_data = output_data["queryResult"]["files"][0]["file_data"]
    if encode_result: 
        file_data = base64.b64decode(file_data).decode("utf-8")
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
        assert int(fields[3]) == expected_gender_count

        # Assert the distribution for this category
        values = fields[10].split("^")
        for value in values[1:-1]:  # Skip empty first and last elements
            if value:  # Skip empty values
                gender, count = value.split("|")
                assert int(count) == expected_values[category][gender]


@pytest.mark.end_to_end
@pytest.mark.parametrize("test_case", test_cases)
@pytest.mark.parametrize("input_method", ["file", "inline"])
def test_cli_demographics(test_case: DemographicsTestCase,input_method: str) -> None:
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
    if input_method == "file":
        cmd = [
            sys.executable,
            "-m",
            "hutch_bunny.cli",
            "--body",
            test_case.json_file_path,
            "--modifiers",
            test_case.modifiers,
            "--output",
            output_file_path,
        ]
    else:
        with open(test_case.json_file_path) as f:
            query_json = json.dumps(json.load(f))
        
        cmd = [
            sys.executable,
            "-m",
            "hutch_bunny.cli",
            "--body-json",
            query_json,
            "--modifiers",
            test_case.modifiers,
            "--output",
            output_file_path,
        ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    # Assert
    assert result.returncode == 0, f"CLI failed with error: {result.stderr}"

    # Assert output file
    assert os.path.exists(output_file_path), "Output file was not created."

    assert_demographics_output(
        output_file_path, 
        test_case.expected_count, 
        test_case.expected_gender_count, 
        test_case.expected_values, 
        encode_result=True
    ) 

    # Clean up
    os.remove(output_file_path)
 