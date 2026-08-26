"""Replicated state machine implementation."""

from typing import Any, Dict

from .types import LogEntry


class StateMachine:
    """A replicated state machine driven by a consensus log."""

    def __init__(self):
        """Initialize the state machine."""
        self._state: Dict[str, Any] = {}

    def apply(self, entry: LogEntry) -> Any:
        """Apply a log entry to the state machine.
        
        Args:
            entry: The log entry to apply.
            
        Returns:
            The result of applying the entry.
        """
        if entry.command == "set":
            key = entry.data.get("key")
            value = entry.data.get("value")
            if key is not None:
                self._state[key] = value
                return value
        elif entry.command == "delete":
            key = entry.data.get("key")
            if key in self._state:
                return self._state.pop(key)
        elif entry.command == "init":
            pass # No-op for init entry
        
        return None

    def snapshot(self) -> dict:
        """Create a snapshot of the current state.
        
        Returns:
            A dictionary representation of the state.
        """
        return {"state": self._state.copy()}

    def restore(self, snapshot: dict) -> None:
        """Restore the state machine from a snapshot.
        
        Args:
            snapshot: The snapshot to restore from.
        """
        self._state = snapshot.get("state", {}).copy()
