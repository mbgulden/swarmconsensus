"""Tests for CLI."""

import sys
from unittest.mock import patch
from swarmconsensus.cli import main

def test_cli_status(capsys):
    """Test cli status."""
    test_args = ["swarmconsensus", "--node-id", "n1", "status"]
    with patch.object(sys, 'argv', test_args):
        try:
            main()
        except SystemExit:
            pass
            
    captured = capsys.readouterr()
    assert "Initializing node n1" in captured.out
    assert "Status:" in captured.out

def test_cli_leader(capsys):
    """Test cli leader."""
    test_args = ["swarmconsensus", "--node-id", "n1", "leader"]
    with patch.object(sys, 'argv', test_args):
        try:
            main()
        except SystemExit:
            pass
            
    captured = capsys.readouterr()
    assert "Leader: None" in captured.out
