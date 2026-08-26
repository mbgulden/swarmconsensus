"""Election logic for Raft consensus."""

import random
import time
from typing import Optional

from .types import ClusterConfig, VoteRequest, VoteResponse
from .log import ConsensusLog


class Election:
    """Manages the leader election protocol."""

    def __init__(self, config: ClusterConfig, log: ConsensusLog):
        """Initialize the election manager.
        
        Args:
            config: The cluster configuration.
            log: The consensus log.
        """
        self.config = config
        self.log = log
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self._election_timeout = 0.0
        self._last_heartbeat = time.time()
        self.reset_election_timer()
        self.votes_received = set()

    def start_election(self) -> int:
        """Start a new election.
        
        Returns:
            The new term number.
        """
        self.current_term += 1
        self.voted_for = self.config.node_id
        self.votes_received = {self.config.node_id}
        self.reset_election_timer()
        return self.current_term

    def request_vote(self, request: VoteRequest) -> VoteResponse:
        """Handle an incoming vote request.
        
        Args:
            request: The vote request.
            
        Returns:
            The vote response.
        """
        if request.term > self.current_term:
            self.current_term = request.term
            self.voted_for = None
            
        vote_granted = False
        
        if request.term == self.current_term:
            if self.voted_for is None or self.voted_for == request.candidate_id:
                # Check log freshness
                last_index = self.log.last_index()
                last_term = self.log.last_term()
                
                if (request.last_log_term > last_term) or \
                   (request.last_log_term == last_term and request.last_log_index >= last_index):
                    vote_granted = True
                    self.voted_for = request.candidate_id
                    self.reset_election_timer()
                    
        return VoteResponse(
            term=self.current_term,
            vote_granted=vote_granted,
            voter_id=self.config.node_id
        )

    def receive_vote(self, response: VoteResponse) -> bool:
        """Tally a received vote.
        
        Args:
            response: The vote response.
            
        Returns:
            True if a majority of votes has been reached.
        """
        if response.term > self.current_term:
            self.current_term = response.term
            self.voted_for = None
            return False
            
        if response.term == self.current_term and response.vote_granted:
            self.votes_received.add(response.voter_id)
            
        cluster_size = len(self.config.peers) + 1
        majority = (cluster_size // 2) + 1
        
        return len(self.votes_received) >= majority

    def reset_election_timer(self) -> None:
        """Reset the randomized election timeout."""
        min_ms, max_ms = self.config.election_timeout_ms
        timeout_ms = random.uniform(min_ms, max_ms)
        self._election_timeout = timeout_ms / 1000.0
        self._last_heartbeat = time.time()

    def has_timed_out(self) -> bool:
        """Check if the election timer has expired."""
        return (time.time() - self._last_heartbeat) > self._election_timeout
