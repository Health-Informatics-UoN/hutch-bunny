import json
import os
import subprocess
import sys
from typing import Any

import pytest

LOCATION_QUERY_FILES = [
    "tests/queries/availability/location_geo_radius.json",
]


def _run_cli(
    json_file_path: str, *, location_enabled: bool, output_file_path: str
) -> dict[Any, Any]:
    """Run the CLI against a query file with OMOP_LOCATION_ENABLED forced on or off."""
    env = os.environ.copy()
    env["OMOP_LOCATION_ENABLED"] = "true" if location_enabled else "false"

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

    result = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    assert result.returncode == 0, f"CLI failed with error: {result.stderr}"
    assert os.path.exists(output_file_path), "Output file was not created."

    with open(output_file_path, "r") as f:
        output_data: dict[Any, Any] = json.load(f)

    os.remove(output_file_path)
    return output_data


@pytest.mark.end_to_end
@pytest.mark.parametrize("json_file_path", LOCATION_QUERY_FILES)
def test_cli_location_disabled_returns_no_matches(json_file_path: str) -> None:
    """With OMOP_LOCATION_ENABLED off, every Location rule contributes zero matches,
    regardless of what the location table contains."""
    output_file_path = "tests/queries/availability/output_location_disabled.json"
    output_data = _run_cli(
        json_file_path, location_enabled=False, output_file_path=output_file_path
    )

    assert output_data["status"] == "ok"
    assert output_data["queryResult"]["count"] == 0


@pytest.mark.end_to_end
@pytest.mark.parametrize(
    "json_file_path, expected_count",
    [
        # 391 persons fall within the 10km Edinburgh radius in the omop-lite >=0.7.0 synthetic data.
        ("tests/queries/availability/location_geo_radius.json", 391),
    ],
)
def test_cli_location_enabled(json_file_path: str, expected_count: int) -> None:
    """With OMOP_LOCATION_ENABLED on, Location geo-radius rules query the
    location table directly."""
    output_file_path = "tests/queries/availability/output_location_enabled.json"
    output_data = _run_cli(
        json_file_path, location_enabled=True, output_file_path=output_file_path
    )

    assert output_data["status"] == "ok"
    assert output_data["protocolVersion"] == "v2"
    assert output_data["queryResult"]["count"] == expected_count
    assert output_data["queryResult"]["datasetCount"] == 0
    assert output_data["queryResult"]["files"] == []
