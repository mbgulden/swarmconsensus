"""Tests for StateMachine."""

from swarmconsensus.state_machine import StateMachine
from swarmconsensus.types import LogEntry

def test_apply_set_and_delete():
    """Test applying set and delete commands."""
    sm = StateMachine()
    
    entry1 = LogEntry(term=1, index=1, command="set", data={"key": "x", "value": 42}, timestamp=0.0)
    res1 = sm.apply(entry1)
    assert res1 == 42
    
    entry2 = LogEntry(term=1, index=2, command="delete", data={"key": "x"}, timestamp=0.0)
    res2 = sm.apply(entry2)
    assert res2 == 42
    
    entry3 = LogEntry(term=1, index=3, command="delete", data={"key": "x"}, timestamp=0.0)
    res3 = sm.apply(entry3)
    assert res3 is None

def test_snapshot_restore():
    """Test snapshot and restore."""
    sm1 = StateMachine()
    sm1.apply(LogEntry(term=1, index=1, command="set", data={"key": "k", "value": "v"}, timestamp=0.0))
    
    snapshot = sm1.snapshot()
    
    sm2 = StateMachine()
    sm2.restore(snapshot)
    
    # Verify state is restored (delete should return the value)
    res = sm2.apply(LogEntry(term=1, index=2, command="delete", data={"key": "k"}, timestamp=0.0))
    assert res == "v"
