"""Epoch management for swarm consensus."""

import time
from typing import Optional

from .types import EpochLease


class EpochManager:
    """Manages monotonic epoch leases to prevent split-brain."""

    def __init__(self, node_id: str):
        """Initialize the epoch manager.
        
        Args:
            node_id: The ID of the current node.
        """
        self.node_id = node_id
        self._current_epoch = 0
        self._current_lease: Optional[EpochLease] = None

    def grant_lease(self, term: int, ttl_seconds: float = 30.0) -> EpochLease:
        """Grant a new epoch lease.
        
        Args:
            term: The current Raft term.
            ttl_seconds: Time-to-live for the lease.
            
        Returns:
            The granted epoch lease.
        """
        # Ensure monotonic epoch numbers
        self._current_epoch = max(self._current_epoch + 1, term)
        now = time.time()
        
        self._current_lease = EpochLease(
            epoch=self._current_epoch,
            leader_id=self.node_id,
            granted_at=now,
            expires_at=now + ttl_seconds,
            term=term
        )
        return self._current_lease

    def renew_lease(self, lease: EpochLease, ttl_seconds: float = 30.0) -> Optional[EpochLease]:
        """Renew an existing epoch lease.
        
        Args:
            lease: The lease to renew.
            ttl_seconds: New time-to-live.
            
        Returns:
            The renewed epoch lease, or None if the renewal fails.
        """
        if not self.is_valid(lease):
            return None
            
        if lease.leader_id != self.node_id:
            return None
            
        now = time.time()
        self._current_lease = EpochLease(
            epoch=lease.epoch,
            leader_id=self.node_id,
            granted_at=now,
            expires_at=now + ttl_seconds,
            term=lease.term
        )
        return self._current_lease

    def is_valid(self, lease: EpochLease) -> bool:
        """Check if an epoch lease is valid.
        
        Args:
            lease: The lease to check.
            
        Returns:
            True if valid, False otherwise.
        """
        if lease is None:
            return False
            
        # Lease epoch cannot go backwards
        if lease.epoch < self._current_epoch:
            return False
            
        now = time.time()
        return now < lease.expires_at

    def current_epoch(self) -> int:
        """Get the current epoch number."""
        return self._current_epoch
