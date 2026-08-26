"""Tests for EpochManager."""

import time
from swarmconsensus.epoch import EpochManager

def test_grant_lease():
    """Test granting an epoch lease."""
    manager = EpochManager("n1")
    lease = manager.grant_lease(term=1, ttl_seconds=1.0)
    
    assert lease.epoch >= 1
    assert lease.leader_id == "n1"
    assert lease.term == 1
    assert manager.is_valid(lease) is True

def test_renew_lease():
    """Test renewing an epoch lease."""
    manager = EpochManager("n1")
    lease = manager.grant_lease(term=1, ttl_seconds=1.0)
    
    renewed = manager.renew_lease(lease, ttl_seconds=2.0)
    assert renewed is not None
    assert renewed.expires_at > lease.expires_at
    assert renewed.epoch == lease.epoch

def test_lease_expiry():
    """Test that leases expire."""
    manager = EpochManager("n1")
    lease = manager.grant_lease(term=1, ttl_seconds=0.01)
    
    time.sleep(0.02)
    assert manager.is_valid(lease) is False

def test_monotonic_epoch():
    """Test that epochs are strictly monotonic."""
    manager = EpochManager("n1")
    lease1 = manager.grant_lease(term=1)
    lease2 = manager.grant_lease(term=1) # Same term, new epoch
    
    assert lease2.epoch > lease1.epoch
    assert manager.is_valid(lease1) is False # Older epoch is invalid
