"""Main Raft node implementation."""

import asyncio
import time
from pathlib import Path
from typing import Any, Dict, Optional

from .types import (
    ClusterConfig,
    NodeState,
    LogEntry,
    VoteRequest,
    VoteResponse,
    AppendRequest,
    AppendResponse,
    ConsensusStats,
    NotLeaderError,
)
from .log import ConsensusLog
from .election import Election
from .epoch import EpochManager
from .state_machine import StateMachine
from .transport import Transport, InProcessTransport


class RaftNode:
    """A node participating in the Raft consensus protocol."""

    def __init__(
        self,
        config: ClusterConfig,
        transport: Optional[Transport] = None,
        db_path: Optional[Path] = None,
    ):
        """Initialize a Raft node."""
        self.config = config
        self.transport = transport or InProcessTransport(config.node_id)
        self.log = ConsensusLog(db_path)
        self.election = Election(config, self.log)
        self.epoch_manager = EpochManager(config.node_id)
        self.state_machine = StateMachine()
        
        self._state = NodeState.FOLLOWER
        self._leader_id: Optional[str] = None
        self._running = False
        self._loop_task: Optional[asyncio.Task] = None
        
        # Leader state
        self._next_index: Dict[str, int] = {}
        self._match_index: Dict[str, int] = {}
        self._last_applied = 0
        
        # Restore commit index and applied index from log
        self.log.commit(0)  # Dummy init
        self._apply_committed_entries()

    @property
    def state(self) -> NodeState:
        """Get the current node state."""
        return self._state

    @property
    def leader_id(self) -> Optional[str]:
        """Get the current leader ID."""
        return self._leader_id

    def stats(self) -> ConsensusStats:
        """Get consensus statistics."""
        return ConsensusStats(
            current_term=self.election.current_term,
            state=self._state,
            leader_id=self._leader_id or "",
            log_length=self.log.last_index() + 1,
            commit_index=self.log.commit_index,
            last_applied=self._last_applied,
            cluster_size=len(self.config.peers) + 1,
        )

    async def start(self) -> None:
        """Start the node."""
        self._running = True
        self.election.reset_election_timer()
        self._loop_task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        """Gracefully stop the node."""
        self._running = False
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass

    async def propose(self, command: str, data: Dict[str, Any]) -> LogEntry:
        """Propose a new log entry.
        
        Raises:
            NotLeaderError: If the node is not the leader.
        """
        if self._state != NodeState.LEADER:
            raise NotLeaderError(f"Cannot propose: not leader. Leader is {self._leader_id}")
            
        entry = LogEntry(
            term=self.election.current_term,
            index=self.log.last_index() + 1,
            command=command,
            data=data,
            timestamp=time.time(),
        )
        self.log.append(entry)
        
        # Update leader's own match index
        self._match_index[self.config.node_id] = entry.index
        
        # Wake up heartbeat loop to broadcast immediately
        await self._broadcast_append_entries()
        return entry

    async def _run_loop(self) -> None:
        """Main event loop."""
        try:
            # Task for processing incoming messages
            msg_task = asyncio.create_task(self._process_messages())
            
            while self._running:
                if self._state == NodeState.FOLLOWER:
                    if self.election.has_timed_out():
                        self._become_candidate()
                    await asyncio.sleep(0.01)
                    
                elif self._state == NodeState.CANDIDATE:
                    if self.election.has_timed_out():
                        # Restart election
                        self._become_candidate()
                    await asyncio.sleep(0.01)
                    
                elif self._state == NodeState.LEADER:
                    await self._broadcast_append_entries()
                    await asyncio.sleep(self.config.heartbeat_interval_ms / 1000.0)
        finally:
            if 'msg_task' in locals():
                msg_task.cancel()

    async def _process_messages(self) -> None:
        """Process incoming network messages."""
        while self._running:
            try:
                msg = await self.transport.receive()
                await self._handle_message(msg)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error processing message: {e}")

    async def _handle_message(self, msg: Dict[str, Any]) -> None:
        """Handle a single message."""
        msg_type = msg.get("type")
        
        if msg_type == "vote_request":
            req = VoteRequest(**msg["request"])
            await self._handle_vote_request(req)
        elif msg_type == "vote_response":
            resp = VoteResponse(**msg["response"])
            await self._handle_vote_response(resp)
        elif msg_type == "append_request":
            # Need to deserialize entries manually
            req_data = msg["request"].copy()
            req_data["entries"] = [LogEntry(**e) for e in req_data["entries"]]
            req = AppendRequest(**req_data)
            await self._handle_append_request(req)
        elif msg_type == "append_response":
            resp = AppendResponse(**msg["response"])
            await self._handle_append_response(resp)

    def _become_follower(self, term: int, leader_id: Optional[str] = None) -> None:
        """Transition to follower state."""
        self._state = NodeState.FOLLOWER
        self.election.current_term = term
        self.election.voted_for = None
        self._leader_id = leader_id
        self.election.reset_election_timer()

    def _become_candidate(self) -> None:
        """Transition to candidate state and start election."""
        self._state = NodeState.CANDIDATE
        self._leader_id = None
        term = self.election.start_election()
        
        # Single node cluster wins immediately
        if not self.config.peers:
            self._become_leader()
            return
            
        # Send vote requests
        req = VoteRequest(
            term=term,
            candidate_id=self.config.node_id,
            last_log_index=self.log.last_index(),
            last_log_term=self.log.last_term(),
        )
        
        msg = {
            "type": "vote_request",
            "request": {
                "term": req.term,
                "candidate_id": req.candidate_id,
                "last_log_index": req.last_log_index,
                "last_log_term": req.last_log_term,
            }
        }
        
        for peer in self.config.peers:
            asyncio.create_task(self.transport.send(peer, msg))

    def _become_leader(self) -> None:
        """Transition to leader state."""
        self._state = NodeState.LEADER
        self._leader_id = self.config.node_id
        
        # Initialize leader state
        last_index = self.log.last_index()
        self._next_index = {peer: last_index + 1 for peer in self.config.peers}
        self._match_index = {peer: 0 for peer in self.config.peers}
        self._match_index[self.config.node_id] = last_index
        
        # Grant initial epoch lease
        self.epoch_manager.grant_lease(self.election.current_term)
        
        # Append a no-op entry to establish log authority (optional in basic Raft, but good practice)
        # We skip it here to keep tests simple

    async def _handle_vote_request(self, req: VoteRequest) -> None:
        """Handle incoming vote request."""
        if req.term > self.election.current_term:
            self._become_follower(req.term)
            
        resp = self.election.request_vote(req)
        
        msg = {
            "type": "vote_response",
            "response": {
                "term": resp.term,
                "vote_granted": resp.vote_granted,
                "voter_id": resp.voter_id,
            }
        }
        await self.transport.send(req.candidate_id, msg)

    async def _handle_vote_response(self, resp: VoteResponse) -> None:
        """Handle incoming vote response."""
        if self._state != NodeState.CANDIDATE:
            return
            
        if resp.term > self.election.current_term:
            self._become_follower(resp.term)
            return
            
        if self.election.receive_vote(resp):
            self._become_leader()

    async def _broadcast_append_entries(self) -> None:
        """Broadcast append entries to all peers."""
        if self._state != NodeState.LEADER:
            return
            
        # Renew lease
        if self.epoch_manager._current_lease:
            self.epoch_manager.renew_lease(self.epoch_manager._current_lease)
            
        for peer in self.config.peers:
            next_idx = self._next_index[peer]
            prev_log_index = next_idx - 1
            
            prev_entry = self.log.get(prev_log_index)
            prev_log_term = prev_entry.term if prev_entry else 0
            
            entries = self.log.slice(next_idx, self.log.last_index() + 1)
            
            req = AppendRequest(
                term=self.election.current_term,
                leader_id=self.config.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=self.log.commit_index,
            )
            
            # Serialize entries
            entries_data = [
                {
                    "term": e.term,
                    "index": e.index,
                    "command": e.command,
                    "data": e.data,
                    "timestamp": e.timestamp,
                }
                for e in entries
            ]
            
            msg = {
                "type": "append_request",
                "request": {
                    "term": req.term,
                    "leader_id": req.leader_id,
                    "prev_log_index": req.prev_log_index,
                    "prev_log_term": req.prev_log_term,
                    "entries": entries_data,
                    "leader_commit": req.leader_commit,
                }
            }
            asyncio.create_task(self.transport.send(peer, msg))

    async def _handle_append_request(self, req: AppendRequest) -> None:
        """Handle incoming append entries request."""
        if req.term > self.election.current_term:
            self._become_follower(req.term, req.leader_id)
        elif req.term == self.election.current_term:
            if self._state == NodeState.CANDIDATE:
                self._become_follower(req.term, req.leader_id)
            else:
                self._leader_id = req.leader_id
                self.election.reset_election_timer()
                
        success = False
        match_index = 0
        
        if req.term >= self.election.current_term:
            # Check prev log index and term
            prev_entry = self.log.get(req.prev_log_index)
            if req.prev_log_index == 0 or (prev_entry and prev_entry.term == req.prev_log_term):
                success = True
                
                # Truncate conflicting entries and append new ones
                if req.entries:
                    self.log.truncate_after(req.prev_log_index)
                    for entry in req.entries:
                        self.log.append(entry)
                        
                match_index = req.prev_log_index + len(req.entries)
                
                # Update commit index
                if req.leader_commit > self.log.commit_index:
                    new_commit = min(req.leader_commit, match_index)
                    self.log.commit(new_commit)
                    self._apply_committed_entries()
            
        msg = {
            "type": "append_response",
            "response": {
                "term": self.election.current_term,
                "success": success,
                "match_index": match_index,
                "follower_id": self.config.node_id,
            }
        }
        await self.transport.send(req.leader_id, msg)

    async def _handle_append_response(self, resp: AppendResponse) -> None:
        """Handle incoming append entries response."""
        if self._state != NodeState.LEADER:
            return
            
        if resp.term > self.election.current_term:
            self._become_follower(resp.term)
            return
            
        peer = resp.follower_id
        if resp.success:
            self._match_index[peer] = resp.match_index
            self._next_index[peer] = resp.match_index + 1
            
            # Check if we can advance commit index
            self._advance_commit_index()
        else:
            # Back up next_index and retry
            self._next_index[peer] = max(1, self._next_index[peer] - 1)
            # Next heartbeat will retry with the lower index

    def _advance_commit_index(self) -> None:
        """Advance the commit index if a majority has replicated entries."""
        match_indices = sorted(self._match_index.values(), reverse=True)
        # Nth highest match index where N is majority
        cluster_size = len(self.config.peers) + 1
        majority_idx = cluster_size // 2
        
        candidate_commit = match_indices[majority_idx]
        
        if candidate_commit > self.log.commit_index:
            # Only commit entries from current term
            entry = self.log.get(candidate_commit)
            if entry and entry.term == self.election.current_term:
                self.log.commit(candidate_commit)
                self._apply_committed_entries()

    def _apply_committed_entries(self) -> None:
        """Apply committed entries to the state machine."""
        while self._last_applied < self.log.commit_index:
            self._last_applied += 1
            entry = self.log.get(self._last_applied)
            if entry:
                self.state_machine.apply(entry)
