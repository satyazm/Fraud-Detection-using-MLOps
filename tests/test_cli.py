"""Example test for the placeholder CLI subcommands."""

import pytest

from fraud_detection.cli import main


@pytest.mark.parametrize("command", ["train", "producer", "consumer", "api"])
def test_placeholder_commands_exit_cleanly(command):
    assert main([command]) == 0


def test_missing_command_is_a_usage_error():
    with pytest.raises(SystemExit):
        main([])
