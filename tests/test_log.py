"""Tests for ConsensusLog."""

import tempfile
from pathlib import Path
from swarmconsensus.log import ConsensusLog
from swarmconsensus.types import LogEntry

def test_log_append_and_get():
    """Test appending and getting entries."""
    log = ConsensusLog()
    entry = LogEntry(term=1, index=1, command="set", data={"k": "v"}, timestamp=0.0)
    log.append(entry)
    
    retrieved = log.get(1)
    assert retrieved is not None
    assert retrieved.term == 1
    assert retrieved.command == "set"
    assert retrieved.data == {"k": "v"}

def test_log_persistence(tmp_path):
    """Test log persistence with SQLite."""
    db_path = tmp_path / "test.db"
    log1 = ConsensusLog(db_path)
    log1.append(LogEntry(term=2, index=1, command="cmd1", data={}, timestamp=0.0))
    
    log2 = ConsensusLog(db_path)
    retrieved = log2.get(1)
    assert retrieved is not None
    assert retrieved.term == 2

def test_log_truncate_and_slice():
    """Test truncating and slicing the log."""
    log = ConsensusLog()
    for i in range(1, 6):
        log.append(LogEntry(term=1, index=i, command=f"cmd{i}", data={}, timestamp=0.0))
        
    entries = log.slice(2, 4)
    assert len(entries) == 2
    assert entries[0].index == 2
    assert entries[1].index == 3
    
    log.truncate_after(3)
    assert log.last_index() == 3
    assert log.get(4) is None

def test_log_commit():
    """Test committing entries."""
    log = ConsensusLog()
    log.append(LogEntry(term=1, index=1, command="cmd", data={}, timestamp=0.0))
    log.append(LogEntry(term=1, index=2, command="cmd", data={}, timestamp=0.0))
    
    log.commit(1)
    assert log.commit_index == 1
    
    # Can't commit beyond last index
    log.commit(5)
    assert log.commit_index == 2
