"""Consensus log implementation."""

import json
import sqlite3
from pathlib import Path
from typing import List, Optional

from .types import LogEntry


class ConsensusLog:
    """An append-only log with term tracking for Raft consensus."""

    def __init__(self, db_path: Optional[Path] = None):
        """Initialize the consensus log.
        
        Args:
            db_path: Path to the SQLite database file. If None, uses in-memory.
        """
        self._db_path = db_path
        db_file = str(db_path) if db_path else ":memory:"
        self._conn = sqlite3.connect(db_file, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_db()
        self._commit_index = 0

    def _init_db(self) -> None:
        """Initialize the database schema."""
        cursor = self._conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS log_entries (
                "index" INTEGER PRIMARY KEY,
                term INTEGER NOT NULL,
                command TEXT NOT NULL,
                data TEXT NOT NULL,
                timestamp REAL NOT NULL
            )
        ''')
        # Insert a dummy entry at index 0 to simplify logic
        cursor.execute('''
            INSERT OR IGNORE INTO log_entries ("index", term, command, data, timestamp)
            VALUES (0, 0, 'init', '{}', 0.0)
        ''')
        self._conn.commit()

    def append(self, entry: LogEntry) -> None:
        """Append an entry to the log.
        
        Args:
            entry: The log entry to append.
        """
        cursor = self._conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO log_entries ("index", term, command, data, timestamp)
            VALUES (?, ?, ?, ?, ?)
        ''', (entry.index, entry.term, entry.command, json.dumps(entry.data), entry.timestamp))
        self._conn.commit()

    def get(self, index: int) -> Optional[LogEntry]:
        """Get the log entry at the specified index.
        
        Args:
            index: The index of the entry to get.
            
        Returns:
            The log entry, or None if not found.
        """
        cursor = self._conn.cursor()
        cursor.execute('SELECT * FROM log_entries WHERE "index" = ?', (index,))
        row = cursor.fetchone()
        if row is None:
            return None
        return LogEntry(
            term=row['term'],
            index=row['index'],
            command=row['command'],
            data=json.loads(row['data']),
            timestamp=row['timestamp'],
        )

    def last_index(self) -> int:
        """Get the index of the last entry in the log."""
        cursor = self._conn.cursor()
        cursor.execute('SELECT MAX("index") FROM log_entries')
        row = cursor.fetchone()
        return row[0] if row and row[0] is not None else 0

    def last_term(self) -> int:
        """Get the term of the last entry in the log."""
        entry = self.get(self.last_index())
        return entry.term if entry else 0

    def slice(self, start: int, end: int) -> List[LogEntry]:
        """Get a slice of log entries from start (inclusive) to end (exclusive).
        
        Args:
            start: The start index (inclusive).
            end: The end index (exclusive).
            
        Returns:
            A list of log entries.
        """
        cursor = self._conn.cursor()
        cursor.execute('''
            SELECT * FROM log_entries
            WHERE "index" >= ? AND "index" < ?
            ORDER BY "index" ASC
        ''', (start, end))
        
        entries = []
        for row in cursor.fetchall():
            entries.append(LogEntry(
                term=row['term'],
                index=row['index'],
                command=row['command'],
                data=json.loads(row['data']),
                timestamp=row['timestamp'],
            ))
        return entries

    def truncate_after(self, index: int) -> None:
        """Remove all entries after the specified index.
        
        Args:
            index: The index after which entries should be removed.
        """
        cursor = self._conn.cursor()
        cursor.execute('DELETE FROM log_entries WHERE "index" > ?', (index,))
        self._conn.commit()

    def commit(self, index: int) -> None:
        """Mark entries up to the specified index as committed.
        
        Args:
            index: The index up to which entries are committed.
        """
        if index > self.last_index():
            index = self.last_index()
        if index > self._commit_index:
            self._commit_index = index

    @property
    def commit_index(self) -> int:
        """Get the current commit index."""
        return self._commit_index
