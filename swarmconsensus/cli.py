"""Command line interface for swarmconsensus."""

import argparse
import asyncio
import json
import sys
from pathlib import Path

from .types import ClusterConfig
from .raft import RaftNode
from .transport import InProcessTransport


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Swarm Consensus CLI")
    parser.add_argument("--node-id", type=str, default="node0", help="Node ID")
    parser.add_argument("--peers", type=str, default="", help="Comma-separated peer IDs")
    
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    status_parser = subparsers.add_parser("status", help="Get node status")
    
    leader_parser = subparsers.add_parser("leader", help="Get leader ID")
    
    epoch_parser = subparsers.add_parser("epoch", help="Get epoch info")
    
    force_parser = subparsers.add_parser("force-election", help="Force an election")
    
    propose_parser = subparsers.add_parser("propose", help="Propose a command")
    propose_parser.add_argument("cmd", type=str, help="Command name")
    propose_parser.add_argument("data", type=str, help="JSON data")
    
    args = parser.parse_args()
    
    peers = args.peers.split(",") if args.peers else []
    config = ClusterConfig(node_id=args.node_id, peers=peers)
    
    node = RaftNode(config)
    
    print(f"Initializing node {args.node_id} (peers: {peers})...")
    
    if args.command == "status":
        stats = node.stats()
        print(f"Status: {stats.state.value}")
        print(f"Term: {stats.current_term}")
        print(f"Leader: {stats.leader_id}")
    elif args.command == "leader":
        print(f"Leader: {node.leader_id}")
    elif args.command == "epoch":
        print(f"Epoch: {node.epoch_manager.current_epoch()}")
    elif args.command == "force-election":
        node._become_candidate()
        print("Forced election.")
    elif args.command == "propose":
        try:
            data = json.loads(args.data)
            # In a real CLI, we'd connect to a running node via RPC
            # Here we just mock the failure if not running/leader
            if not node._running:
                print("Error: Node is not running.", file=sys.stderr)
                sys.exit(1)
        except json.JSONDecodeError:
            print("Error: Invalid JSON data.", file=sys.stderr)
            sys.exit(1)

if __name__ == "__main__":
    main()
