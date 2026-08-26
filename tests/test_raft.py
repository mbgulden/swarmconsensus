"""Tests for RaftNode."""

import asyncio
import pytest
from swarmconsensus.raft import RaftNode
from swarmconsensus.types import ClusterConfig, NodeState, NotLeaderError
from swarmconsensus.transport import InProcessTransport

def test_single_node_cluster():
    """Test that a single node cluster becomes leader immediately."""
    async def run():
        InProcessTransport.clear()
        config = ClusterConfig(node_id="n1", peers=[], election_timeout_ms=(10, 20))
        node = RaftNode(config)
        
        await node.start()
        await asyncio.sleep(0.05) # Wait for election timeout
        
        assert node.state == NodeState.LEADER
        
        entry = await node.propose("set", {"key": "foo", "value": "bar"})
        assert entry.command == "set"
        
        await node.stop()
        
    asyncio.run(run())

def test_not_leader_propose():
    """Test proposing when not leader raises error."""
    async def run():
        config = ClusterConfig(node_id="n1", peers=["n2"])
        node = RaftNode(config)
        
        with pytest.raises(NotLeaderError):
            await node.propose("set", {"key": "foo", "value": "bar"})
            
    asyncio.run(run())
    
def test_multi_node_election():
    """Test election in a multi-node cluster."""
    async def run():
        InProcessTransport.clear()
        
        # Use very short timeouts for tests
        c1 = ClusterConfig(node_id="n1", peers=["n2", "n3"], election_timeout_ms=(10, 20), heartbeat_interval_ms=5)
        c2 = ClusterConfig(node_id="n2", peers=["n1", "n3"], election_timeout_ms=(50, 100), heartbeat_interval_ms=5)
        c3 = ClusterConfig(node_id="n3", peers=["n1", "n2"], election_timeout_ms=(50, 100), heartbeat_interval_ms=5)
        
        n1 = RaftNode(c1)
        n2 = RaftNode(c2)
        n3 = RaftNode(c3)
        
        await n1.start()
        await n2.start()
        await n3.start()
        
        # Wait for election and some heartbeats
        await asyncio.sleep(0.1)
        
        # Check that exactly one is leader
        states = [n.state for n in [n1, n2, n3]]
        assert states.count(NodeState.LEADER) == 1
        
        leader_idx = states.index(NodeState.LEADER)
        nodes = [n1, n2, n3]
        leader = nodes[leader_idx]
        
        # Test replication
        await leader.propose("set", {"key": "test", "value": 123})
        
        # Wait for replication
        await asyncio.sleep(0.05)
        
        for n in nodes:
            assert n.log.commit_index >= 1
            
        await n1.stop()
        await n2.stop()
        await n3.stop()
        
    asyncio.run(run())
