from typing import Any
import json  
from unittest.mock import Mock, patch

from hutch_bunny.cli import main as cli_main


def test_no_encode_flag_set_correctly(monkeypatch: Any) -> None: 
    """
    Test CLI flag for switching off encoding is parsed correctly. 
    """
    monkeypatch.setattr(
        "sys.argv",
        [
            "prog",
            "--no-encode",
            "--body-json",
            json.dumps({"foo": "bar"}),
            "--output",
            "out.json",
        ],
    )

    with patch("hutch_bunny.cli.execute_query") as mock_execute:
        mock_execute.return_value.to_dict.return_value = {}

        monkeypatch.setattr("hutch_bunny.cli.get_db_client", Mock())
        monkeypatch.setattr("hutch_bunny.cli.save_to_output", Mock())
        monkeypatch.setattr("hutch_bunny.cli.configure_logger", Mock())

        cli_main()

        assert mock_execute.call_args.kwargs["encode_result"] is False


def test_encode_default_is_true(monkeypatch: Any) -> None: 
    """
    Test CLI flag for switching off encoding is defaults to true when --no-encode is not specified. 
    """
    monkeypatch.setattr(
        "sys.argv",
        [
            "prog",
            "--body-json",
            json.dumps({"foo": "bar"}),
            "--output",
            "out.json",
        ],
    )

    with patch("hutch_bunny.cli.execute_query") as mock_execute:
        mock_execute.return_value.to_dict.return_value = {}

        monkeypatch.setattr("hutch_bunny.cli.get_db_client", Mock())
        monkeypatch.setattr("hutch_bunny.cli.save_to_output", Mock())
        monkeypatch.setattr("hutch_bunny.cli.configure_logger", Mock())

        cli_main()

        assert mock_execute.call_args.kwargs["encode_result"] is True 
