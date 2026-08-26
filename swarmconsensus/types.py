"""Types and exceptions for swarmconsensus."""

import enum
from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple


class ConsensusError(Exception):
    """Base exception for consensus errors."""
    pass


class NotLeaderError(ConsensusError):
    """Exception raised when a non-leader node is asked to perform a leader action."""
    pass


class ElectionTimeoutError(ConsensusError):
    """Exception raised when an election times out."""
    pass


class QuorumNotReachedError(ConsensusError):
    """Exception raised when a quorum cannot be reached."""
    pass


class NodeState(enum.Enum):
    """The state of a Raft node."""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    """A log entry."""
    term: int
    index: int
    command: str
    data: Dict[str, Any]
    timestamp: float


@dataclass
class VoteRequest:
    """A request for a vote in an election."""
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class VoteResponse:
    """A response to a vote request."""
    term: int
    vote_granted: bool
    voter_id: str


@dataclass
class AppendRequest:
    """A request to append log entries."""
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry]
    leader_commit: int


@dataclass
class AppendResponse:
    """A response to an append request."""
    term: int
    success: bool
    match_index: int
    follower_id: str


@dataclass
class EpochLease:
    """A lease on a specific epoch."""
    epoch: int
    leader_id: str
    granted_at: float
    expires_at: float
    term: int


@dataclass
class ClusterConfig:
    """Configuration for a Raft cluster."""
    node_id: str
    peers: List[str]
    election_timeout_ms: Tuple[int, int] = (150, 300)
    heartbeat_interval_ms: int = 50


@dataclass
class ConsensusStats:
    """Statistics about a Raft node."""
    current_term: int
    state: NodeState
    leader_id: str
    log_length: int
    commit_index: int
    last_applied: int
    cluster_size: int
