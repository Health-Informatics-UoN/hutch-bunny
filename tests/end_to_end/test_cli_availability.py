import subprocess
import pytest
import os
import json
import sys

test_cases = [
    ("tests/queries/availability/availability.json", "[]", 40),
    (
        "tests/queries/availability/availability.json",
        '[{"id": "Rounding", "nearest": 0}]',
        44,
    ),
    (
        "tests/queries/availability/availability.json",
        '[{"id": "Low Number Suppression", "threshold": 0}, {"id": "Rounding", "nearest": 0}]',
        44,
    ),
    (
        "tests/queries/availability/availability.json",
        '[{"id": "Low Number Suppression", "threshold": 30}]',
        40,
    ),
    (
        "tests/queries/availability/availability.json",
        '[{"id": "Low Number Suppression", "threshold": 40}]',
        40,
    ),
    (
        "tests/queries/availability/availability.json",
        '[{"id": "Low Number Suppression", "threshold": 20}, {"id": "Rounding", "nearest": 10}]',
        40,
    ),
    (
        "tests/queries/availability/availability.json",
        '[{"id": "Rounding", "nearest": 100}]',
        0,
    ),
    (
        "tests/queries/availability/availability.json",
        '[{"id": "Rounding", "nearest": 10}]',
        40,
    ),
    ("tests/queries/availability/basic_gender_or.json", "[]", 100),
    (
        "tests/queries/availability/basic_gender_or.json",
        '[{"id": "Rounding", "nearest": 0}]',
        99,
    ),
    (
        "tests/queries/availability/multiple_in_group_and.json",
        "[]",
        0,
    ),
    ("tests/queries/availability/multiple_in_group_or.json", "[]", 60),
    (
        "tests/queries/availability/multiple_in_group_or.json",
        '[{"id": "Rounding", "nearest": 0}]',
        55,
    ),
    ("tests/queries/availability/multiple_in_group_or_with_age1.json", "[]", 60),
    (
        "tests/queries/availability/multiple_in_group_or_with_age1.json",
        '[{"id": "Rounding", "nearest": 0}]',
        55,
    ),
    ("tests/queries/availability/multiple_in_group_or_with_age2.json", "[]", 60),
    (
        "tests/queries/availability/multiple_in_group_or_with_age2.json",
        '[{"id": "Rounding", "nearest": 0}]',
        55,
    ),
]


@pytest.mark.end_to_end
@pytest.mark.parametrize("json_file_path, modifiers, expected_count", test_cases)
def test_cli_availability(
    json_file_path: str, modifiers: str, expected_count: int
) -> None:
    """
    Test the CLI availability command.

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
    output_file_path = "tests/queries/availability/output.json"

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
        assert output_data["queryResult"]["datasetCount"] == 0
        assert output_data["queryResult"]["files"] == []
        assert output_data["message"] == ""
        assert output_data["collection_id"] == "collection_id"

    # Clean up
    os.remove(output_file_path)
