"""Tests for Election."""

from swarmconsensus.election import Election
from swarmconsensus.types import ClusterConfig, VoteRequest, VoteResponse
from swarmconsensus.log import ConsensusLog

def test_start_election():
    """Test starting an election."""
    config = ClusterConfig(node_id="n1", peers=["n2", "n3"])
    log = ConsensusLog()
    election = Election(config, log)
    
    term = election.start_election()
    assert term == 1
    assert election.voted_for == "n1"
    assert len(election.votes_received) == 1

def test_request_vote_granted():
    """Test voting for a valid candidate."""
    config = ClusterConfig(node_id="n1", peers=["n2"])
    log = ConsensusLog()
    election = Election(config, log)
    
    req = VoteRequest(term=1, candidate_id="n2", last_log_index=0, last_log_term=0)
    resp = election.request_vote(req)
    
    assert resp.vote_granted is True
    assert election.voted_for == "n2"

def test_request_vote_rejected_lower_term():
    """Test rejecting a vote for a candidate with lower term."""
    config = ClusterConfig(node_id="n1", peers=["n2"])
    log = ConsensusLog()
    election = Election(config, log)
    election.current_term = 2
    
    req = VoteRequest(term=1, candidate_id="n2", last_log_index=0, last_log_term=0)
    resp = election.request_vote(req)
    
    assert resp.vote_granted is False

def test_receive_vote_majority():
    """Test reaching a majority of votes."""
    config = ClusterConfig(node_id="n1", peers=["n2", "n3"]) # Cluster size 3, majority 2
    log = ConsensusLog()
    election = Election(config, log)
    
    election.start_election() # Votes for self, votes_received=1
    assert len(election.votes_received) == 1
    
    resp = VoteResponse(term=1, vote_granted=True, voter_id="n2")
    is_majority = election.receive_vote(resp)
    
    assert is_majority is True
    assert len(election.votes_received) == 2
