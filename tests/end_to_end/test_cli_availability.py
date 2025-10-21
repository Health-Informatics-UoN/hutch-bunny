import subprocess
import pytest
import os
import json
import sys
from tests.end_to_end.test_cases.availability_test_cases import (
    test_cases,
    AvailabilityTestCase,
)

test_cases = [test_cases[-1]]

@pytest.mark.end_to_end
@pytest.mark.parametrize("test_case", test_cases)
def test_cli_availability(test_case: AvailabilityTestCase) -> None:
    """
    Test the CLI availability command.

    This test will run the CLI availability command with the given JSON file and modifiers,
    and assert the output is as expected.

    Args:
        test_case: The test case containing the JSON file path, modifiers, and expected count.

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
