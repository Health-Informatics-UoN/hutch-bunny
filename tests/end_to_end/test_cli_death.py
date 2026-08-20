import subprocess
import pytest
import os
import json
import sys
from typing import Any

DEATH_ANY_QUERY_FILE = "tests/queries/availability/death_any.json"
DEATH_ALIVE_QUERY_FILE = "tests/queries/availability/death_alive.json"


def _run_cli(
    json_file_path: str, *, death_enabled: bool, output_file_path: str
) -> dict[Any, Any]:
    """Run the CLI against a query file with OMOP_DEATH_ENABLED forced on or off."""
    env = os.environ.copy()
    env["OMOP_DEATH_ENABLED"] = "true" if death_enabled else "false"

    cmd = [
        sys.executable,
        "-m",
        "hutch_bunny.cli",
        "--body",
        json_file_path,
        "--modifiers",
        json.dumps(
            [
                {"id": "Rounding", "nearest": 0},
                {"id": "Low Number Suppression", "threshold": 0},
            ]
        ),
        "--output",
        output_file_path,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    assert result.returncode == 0, f"CLI failed with error: {result.stderr}"
    assert os.path.exists(output_file_path), "Output file was not created."

    with open(output_file_path, "r") as f:
        output_data: dict[Any, Any] = json.load(f)

    os.remove(output_file_path)
    return output_data


@pytest.mark.end_to_end
def test_cli_death_disabled_any_returns_no_matches() -> None:
    """With OMOP_DEATH_ENABLED off, a Death '=' rule contributes zero matches,
    regardless of what the death table contains."""
    output_file_path = "tests/queries/availability/output_death_disabled_any.json"
    output_data = _run_cli(
        DEATH_ANY_QUERY_FILE, death_enabled=False, output_file_path=output_file_path
    )

    assert output_data["status"] == "ok"
    assert output_data["queryResult"]["count"] == 0


@pytest.mark.end_to_end
def test_cli_death_disabled_alive_matches_everyone() -> None:
    """With OMOP_DEATH_ENABLED off, a Death '!=' rule is a negation of an empty
    match set, so it matches every person rather than zero."""
    output_file_path = "tests/queries/availability/output_death_disabled_alive.json"
    output_data = _run_cli(
        DEATH_ALIVE_QUERY_FILE, death_enabled=False, output_file_path=output_file_path
    )

    assert output_data["status"] == "ok"
    assert output_data["queryResult"]["count"] == 1130


@pytest.mark.end_to_end
@pytest.mark.parametrize(
    "json_file_path, expected_count",
    [
        # 142 persons have a death record in the omop-lite >=0.7.0 synthetic data.
        (DEATH_ANY_QUERY_FILE, 142),
        # The remaining 988 of 1130 persons have no death record.
        (DEATH_ALIVE_QUERY_FILE, 988),
        # No death row in the synthetic data has this cause_concept_id.
        ("tests/queries/availability/death_specific_concept.json", 0),
        # Death AND Condition 260139 narrows the 142 deceased down to 54.
        ("tests/queries/availability/death_with_condition.json", 54),
    ],
)
def test_cli_death_enabled(json_file_path: str, expected_count: int) -> None:
    """With OMOP_DEATH_ENABLED on, Death rules query the death table directly."""
    output_file_path = "tests/queries/availability/output_death_enabled.json"
    output_data = _run_cli(
        json_file_path, death_enabled=True, output_file_path=output_file_path
    )

    assert output_data["status"] == "ok"
    assert output_data["protocolVersion"] == "v2"
    assert output_data["queryResult"]["count"] == expected_count
    assert output_data["queryResult"]["datasetCount"] == 0
    assert output_data["queryResult"]["files"] == []
