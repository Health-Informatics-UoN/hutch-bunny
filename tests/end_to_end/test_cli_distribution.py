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

    # Assert output file content
    file = {
        "file_name": "code.distribution",
        "file_data": "QklPQkFOSwlDT0RFCUNPVU5UCURFU0NSSVBUSU9OCU1JTglRMQlNRURJQU4JTUVBTglRMwlNQVgJQUxURVJOQVRJVkVTCURBVEFTRVQJT01PUAlPTU9QX0RFU0NSCUNBVEVHT1JZCmNvbGxlY3Rpb25faWQJT01PUDozODAwMzU2NAk0MAkJCQkJCQkJCQkzODAwMzU2NAlOb3QgSGlzcGFuaWMgb3IgTGF0aW5vCUV0aG5pY2l0eQpjb2xsZWN0aW9uX2lkCU9NT1A6MzgwMDM1NjMJNjAJCQkJCQkJCQkJMzgwMDM1NjMJSGlzcGFuaWMgb3IgTGF0aW5vCUV0aG5pY2l0eQpjb2xsZWN0aW9uX2lkCU9NT1A6ODUwNwk0MAkJCQkJCQkJCQk4NTA3CU1BTEUJR2VuZGVyCmNvbGxlY3Rpb25faWQJT01PUDo4NTMyCTYwCQkJCQkJCQkJCTg1MzIJRkVNQUxFCUdlbmRlcg==",
        "file_description": "Result of code.distribution analysis",
        "file_size": 0.496,
        "file_type": "BCOS",
        "file_sensitive": True,
        "file_reference": "",
    }

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
