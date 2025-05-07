import subprocess
import pytest
import os
import json
import sys
from dataclasses import dataclass
from typing import Optional


@dataclass
class AvailabilityTestCase:
    json_file_path: str
    expected_count: int
    rounding: Optional[int] = None
    low_number_suppression: Optional[int] = None

    def get_modifiers_json(self) -> str:
        """Convert the modifiers to a JSON string format."""
        modifiers_list = []
        if self.rounding is not None:
            modifiers_list.append({"id": "Rounding", "nearest": self.rounding})
        if self.low_number_suppression is not None:
            modifiers_list.append(
                {
                    "id": "Low Number Suppression",
                    "threshold": self.low_number_suppression,
                }
            )
        return json.dumps(modifiers_list)


test_cases = [
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        expected_count=40,
    ),
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        rounding=0,
        expected_count=44,
    ),
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        rounding=0,
        low_number_suppression=0,
        expected_count=44,
    ),
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        low_number_suppression=30,
        expected_count=40,
    ),
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        low_number_suppression=40,
        expected_count=40,
    ),
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        rounding=10,
        low_number_suppression=20,
        expected_count=40,
    ),
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        rounding=100,
        expected_count=0,
    ),
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/availability.json",
        rounding=10,
        expected_count=40,
    ),
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/basic_gender_or.json",
        expected_count=100,
    ),
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/basic_gender_or.json",
        rounding=0,
        expected_count=99,
    ),
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_and.json",
        expected_count=0,
    ),
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_or.json",
        expected_count=60,
    ),
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_or.json",
        rounding=0,
        expected_count=55,
    ),
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_or_with_age1.json",
        expected_count=60,
    ),
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_or_with_age1.json",
        rounding=0,
        expected_count=55,
    ),
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_or_with_age2.json",
        expected_count=60,
    ),
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_or_with_age2.json",
        rounding=0,
        expected_count=55,
    ),
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/measurement.json",
        rounding=0,
        low_number_suppression=0,
        expected_count=12,
    ),
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/multiple_in_group_exclusion_time.json",
        rounding=0,
        low_number_suppression=0,
        expected_count=13,
    ),
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/basic_ethnicity_or.json",
        rounding=0,
        low_number_suppression=0,
        expected_count=41,
    ),
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/basic_race_or.json",
        rounding=0,
        low_number_suppression=0,
        expected_count=95,
    ),
    AvailabilityTestCase(
        json_file_path="tests/queries/availability/secondary_modifiers.json",
        rounding=0,
        low_number_suppression=0,
        expected_count=13,
    ),
]


@pytest.mark.end_to_end
@pytest.mark.parametrize("test_case", test_cases)
def test_cli_availability(test_case: AvailabilityTestCase) -> None:
    """
    Test the CLI availability command.

    This test will run the CLI availability command with the given JSON file and modifiers,
    and assert the output is as expected.

    Args:
        test_case (AvailabilityTestCase): The test case containing the JSON file path, modifiers, and expected count.

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
            test_case.json_file_path,
            "--modifiers",
            test_case.get_modifiers_json(),
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
        assert output_data["queryResult"]["datasetCount"] == 0
        assert output_data["queryResult"]["files"] == []
        assert output_data["message"] == ""
        assert output_data["collection_id"] == "collection_id"

    # Clean up
    os.remove(output_file_path)
