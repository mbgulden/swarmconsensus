"""Network transport abstraction."""

import asyncio
import json
from collections import defaultdict
from typing import Any, Dict, Optional


class Transport:
    """Base class for network transport abstractions."""
    
    async def send(self, target: str, message: Dict[str, Any]) -> None:
        """Send a message to a target peer."""
        raise NotImplementedError

    async def receive(self) -> Dict[str, Any]:
        """Receive the next message."""
        raise NotImplementedError


class InProcessTransport(Transport):
    """An in-memory transport for testing."""

    _queues: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)

    def __init__(self, node_id: str):
        """Initialize the transport for a specific node."""
        self.node_id = node_id

    async def send(self, target: str, message: Dict[str, Any]) -> None:
        """Send a message to a target peer."""
        # Serialize and deserialize to ensure immutability and simulate network
        serialized = json.dumps(message)
        payload = json.loads(serialized)
        await self._queues[target].put(payload)

    async def receive(self) -> Dict[str, Any]:
        """Receive the next message."""
        return await self._queues[self.node_id].get()
        
    @classmethod
    def clear(cls) -> None:
        """Clear all message queues (useful for test isolation)."""
        cls._queues.clear()
