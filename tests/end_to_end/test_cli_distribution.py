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
    expected_count: int  # Number of lines in the output file
    expected_values: Dict[str, int]  # Map of OMOP codes to their expected counts


test_cases = [
    DistributionTestCase(
        json_file_path="tests/queries/distribution/distribution.json",
        modifiers="[]",
        expected_count=374,
        expected_values={
            "8507": 570,  # MALE
            "8532": 560,  # FEMALE
            "260139": 440,  # Acute Bronchitis
            "4311629": 310,  # Impaired glucose tolerance
            "19115351": 10, # Diazepam (drug)
            "44783196": 20, # Surgical manipulation of joint of knee
            "255848": 10, # Pneumonia
            "432867": 110, # Hyperlipidemia
        },
    ),
    DistributionTestCase(
        json_file_path="tests/queries/distribution/distribution.json",
        modifiers='[{"id": "Rounding", "nearest": 0}]',
        expected_count=374,
        expected_values={
            "8507": 572,  # MALE
            "8532": 558,  # FEMALE
            "260139": 442,  # Acute Bronchitis
            "4311629": 310,  # Impaired glucose tolerance
            "19115351": 11, # Diazepam (drug)
            "44783196": 19, # Surgical manipulation of joint of knee
            "255848": 11, # Pneumonia
            "432867": 110, # Hyperlipidemia
        },
    ),
    DistributionTestCase(
        json_file_path="tests/queries/distribution/distribution.json",
        modifiers='[{"id": "Rounding", "nearest": 100}]',
        expected_count=374,
        expected_values={
            "8507": 600,  # MALE
            "8532": 600,  # FEMALE
            "260139": 400,  # Acute Bronchitis
            "4311629": 300,  # Impaired glucose tolerance
            "19115351": 0, # Diazepam (drug)
            "44783196": 0, # Surgical manipulation of joint of knee
            "255848": 0, # Pneumonia
            "432867": 100, # Hyperlipidemia
        },
    ),
    DistributionTestCase(
        json_file_path="tests/queries/distribution/distribution.json",
        modifiers='[{"id": "Rounding", "nearest": 10}, {"id": "Low Number Suppression", "threshold": 10}]',
        expected_count=374,
        expected_values={
            "8507": 570,  # MALE
            "8532": 560,  # FEMALE
            "260139": 440,  # Acute Bronchitis
            "4311629": 310,  # Impaired glucose tolerance
            "19115351": 10, # Diazepam (drug)
            "44783196": 20, # Surgical manipulation of joint of knee
            "255848": 10, # Pneumonia
            "432867": 110, # Hyperlipidemia
        },
    ),
    DistributionTestCase(
        json_file_path="tests/queries/distribution/distribution.json",
        modifiers='[{"id": "Rounding", "nearest": 10}, {"id": "Low Number Suppression", "threshold": 50}]',
        expected_count=197,
        expected_values={
            "8507": 570,  # MALE
            "8532": 560,  # FEMALE
            "260139": 440,  # Acute Bronchitis
            "4311629": 310,  # Impaired glucose tolerance
            "432867": 110, # Hyperlipidemia
        },
    ),
]


def assert_distribution_output(output_file_path: str, expected_count: int, expected_values: dict, encode_result: bool = True) -> None:
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
        output_data["queryResult"]["files"][0]["file_name"] == "code.distribution"
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
        == "BIOBANK	CODE	COUNT	DESCRIPTION	MIN	Q1	MEDIAN	MEAN	Q3	MAX	ALTERNATIVES	DATASET	OMOP	OMOP_DESCR	CATEGORY"
    )

    # Verify counts
    for line in lines[1:]:  # Skip header
        fields = line.split("\t")
        omop_code = fields[12]  # OMOP column

        count_str = fields[2]  # COUNT column as string

        # Assert nan is not in line
        assert "nan" not in count_str, f"Expected no 'nan' values, but got: {count_str}"
        
        # Assert count is an integer
        assert (
            count_str.isdigit()
        ), f"Expected an integer count, but got: {count_str}"
        count = int(count_str)  # Convert to int after validation
        if omop_code in expected_values:
            assert count == expected_values[omop_code]

    
@pytest.mark.end_to_end
@pytest.mark.parametrize("test_case", test_cases)
def test_cli_distribution(test_case: DistributionTestCase) -> None:
    """
    Test the CLI distribution command.

    This test will run the CLI distribution command with the given JSON file and modifiers,
    and assert the output is as expected.

    Args:
        test_case (DistributionTestCase): The test case containing the JSON file path, modifiers, and expected counts.

    Returns:
        None
    """
    # Arrange
    output_file_path = "tests/queries/distribution/output.json"

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

    result = subprocess.run(cmd, capture_output=True, text=True)

    # Assert
    assert result.returncode == 0, f"CLI failed with error: {result.stderr}"

    # Assert output file
    assert os.path.exists(output_file_path), "Output file was not created."

    assert_distribution_output(output_file_path, test_case.expected_count, test_case.expected_values, encode_result=True)

    # Clean up
    os.remove(output_file_path)


@pytest.mark.end_to_end
@pytest.mark.parametrize("test_case", test_cases)
def test_cli_distribution_inline(test_case: DistributionTestCase) -> None: 
    """
    Test the CLI distribution command with an inline JSON string as opposed to filepath. 

    This test will run the CLI distribution command with the --body-json option, corresponding JSON query string
    and assert the output is as expected.

    Args:
        test_case (DistributionTestCase): The test case containing the JSON file path, modifiers, and expected counts.

    Returns:
        None
    """
    # Arrange
    output_file_path = "tests/queries/distribution/output.json"

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

    assert_distribution_output(output_file_path, test_case.expected_count, test_case.expected_values, encode_result=True)

    # Clean up
    os.remove(output_file_path)


@pytest.mark.end_to_end
@pytest.mark.parametrize("test_case", test_cases)
def test_cli_distribution_with_no_encoding(test_case: DistributionTestCase) -> None: 
    """
    Test the CLI distribution command without encoding of the output.

    This test will run the CLI distribution command with the given JSON file, modifiers and --no-encode flag 
    and assert the output is as expected.

    Args:
        test_case (DistributionTestCase): The test case containing the JSON file path, modifiers, and expected counts.

    Returns:
        None
    """
    output_file_path = "tests/queries/distribution/output.json"

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
        "--no-encode"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    # Assert
    assert result.returncode == 0, f"CLI failed with error: {result.stderr}"

    # Assert output file
    assert os.path.exists(output_file_path), "Output file was not created."

    assert_distribution_output(output_file_path, test_case.expected_count, test_case.expected_values, encode_result=False)

    # Clean up
    os.remove(output_file_path)
