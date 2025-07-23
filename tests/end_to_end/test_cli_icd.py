import subprocess
import pytest
import sys
from dataclasses import dataclass


@dataclass
class ICDTestCase:
    json_file_path: str
    modifiers: str


test_cases = [
    ICDTestCase(
        json_file_path="tests/queries/distribution/icd.json",
        modifiers="[]",
    ),
]


@pytest.mark.end_to_end
@pytest.mark.parametrize("test_case", test_cases)
def test_cli_icd(test_case: ICDTestCase) -> None:
    """
    Test the CLI ICD command.

    This test will run the CLI ICD command with the given JSON file,
    and assert the output is as expected.

    Args:
        test_case (ICDTestCase): The test case containing the JSON file path.

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

    # Assert ICD-MAIN queries are not supported
    assert result.returncode == 1, f"CLI failed with error: {result.stderr}"
    assert "ICD-MAIN queries are not yet supported." in result.stderr
