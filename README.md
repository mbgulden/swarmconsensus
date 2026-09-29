# 🗳️ SwarmConsensus

[![CI](https://github.com/mbgulden/swarmconsensus/actions/workflows/ci.yml/badge.svg)](https://github.com/mbgulden/swarmconsensus/actions)
[![PyPI version](https://img.shields.io/badge/pypi-v0.1.0-blue.svg)](https://pypi.org/project/swarmconsensus/)
[![Python 3.9+](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Zero Dependencies](https://img.shields.io/badge/deps-zero%20runtime-emerald.svg)](https://github.com/mbgulden/swarmconsensus)

> **Distributed leader election and epoch quorum authority for the Prismatic Engine swarm ecosystem.**
> *Lightweight Raft consensus on stdlib asyncio — monotonic epoch leases, SQLite-backed log replication, zero runtime dependencies.*

---

## 💡 Why SwarmConsensus?

An agent swarm with no decider is a room full of people talking at once. When several agents can act on shared state — dispatching work, spending budget, writing to the same store — you need exactly one of them in charge at any moment, and you need every node to agree on *who* that is.

**SwarmConsensus** gives the swarm that authority:

- **Leader election** — nodes elect a single leader via Raft voting; followers step down the moment a higher term appears.
- **Epoch leases** — monotonic, time-bound leadership leases that keep a deposed leader from acting on stale authority (split-brain protection).
- **Replicated log** — every decision is an entry in an append-only log, replicated to a quorum before it commits.
- **Replicated state machine** — committed entries apply to a deterministic state machine with snapshot/restore.

---

## 🏛️ Architecture

```
                    ┌──────────────────────────────────────────────┐
                    │                  RaftNode                    │
                    │  follower → candidate → leader                │
                    └──────┬───────────────┬───────────────┬───────┘
                           │               │               │
              ┌────────────▼──────┐ ┌──────▼────────┐ ┌────▼──────────┐
              │     Election      │ │ EpochManager  │ │ ConsensusLog  │
              │  vote requests,   │ │ monotonic     │ │ SQLite-backed │
              │  randomized       │ │ epoch leases  │ │ append-only   │
              │  timeouts         │ │ (default TTL  │ │ log, term     │
              └─────────────────┘ │  30s)         │ │ tracking      │
                                  └───────────────┘ └──────┬────────┘
                                                         │
                                              ┌──────────▼────────┐
                                              │   StateMachine    │
                                              │  set / delete,    │
                                              │  snapshot/restore │
                                              └───────────────────┘

              ┌──────────────────────────────────────────────┐
              │              Transport (pluggable)           │
              │  InProcessTransport — in-memory, for tests   │
              │  and single-process clusters. Subclass       │
              │  Transport with send()/receive() for real    │
              │  networking.                               │
              └──────────────────────────────────────────────┘
```

Only the **leader** may propose new entries (`NotLeaderError` otherwise). Entries replicate via AppendEntries RPCs and commit once a quorum has them; followers then apply them to their own state machine copy.

---

## 📦 Installation

```bash
pip install swarmconsensus
```

*Pure Python standard library. Zero runtime dependencies. Typed (`py.typed` included). Python 3.9+.*

---

## 🚀 Quick Start

Under five minutes, single node:

```python
import asyncio
from swarmconsensus import ClusterConfig, NodeState, RaftNode


async def main():
    # A lone node wins its election immediately.
    node = RaftNode(
        ClusterConfig(node_id="n1", peers=[], election_timeout_ms=(50, 100))
    )
    await node.start()
    await asyncio.sleep(0.3)
    assert node.state == NodeState.LEADER

    entry = await node.propose("set", {"key": "hello", "value": "swarm"})
    print(f"Committed #{entry.index} in term {entry.term}")
    print("State machine:", node.state_machine.snapshot())
    # State machine: {'state': {'hello': 'swarm'}}

    await node.stop()


asyncio.run(main())
```

### Three-node cluster

```python
import asyncio
from swarmconsensus import ClusterConfig, NodeState, RaftNode
from swarmconsensus.transport import InProcessTransport


async def main():
    InProcessTransport.clear()  # isolate the in-memory message bus
    nodes = [
        RaftNode(
            ClusterConfig(
                node_id=f"n{i}",
                peers=[f"n{j}" for j in (1, 2, 3) if j != i],
                election_timeout_ms=(10, 20),
                heartbeat_interval_ms=5,
            )
        )
        for i in (1, 2, 3)
    ]
    for n in nodes:
        await n.start()
    await asyncio.sleep(0.5)

    leaders = [n for n in nodes if n.state == NodeState.LEADER]
    assert len(leaders) == 1  # exactly one leader, guaranteed
    await leaders[0].propose(
        "set", {"key": "decider", "value": leaders[0].config.node_id}
    )

    for n in nodes:
        await n.stop()


asyncio.run(main())
```

### Epoch leases (split-brain protection)

```python
from swarmconsensus import EpochManager

em = EpochManager("n1")
lease = em.grant_lease(term=1)  # 30s TTL by default
assert em.is_valid(lease)  # epoch only moves forward
print(lease.epoch, lease.leader_id)  # 1 n1
```

---

## 🖥️ CLI Usage

```bash
swarmconsensus --node-id n1 status
swarmconsensus --node-id n1 leader
swarmconsensus --node-id n1 epoch
swarmconsensus --node-id n1 force-election
swarmconsensus --node-id n1 propose set '{"key": "hello", "value": "swarm"}'
```

The CLI spins up a local in-process node for inspection and demos — it reports that node's own view (a fresh node starts as follower; `force-election` makes a peerless node leader). It does not attach to a running cluster; use the Python API and a real `Transport` for that.

---

## 🐍 Python SDK

```python
from swarmconsensus import (
    RaftNode,
    ClusterConfig,
    EpochManager,
    ConsensusLog,
    StateMachine,
    InProcessTransport,
    Transport,
    NodeState,
    ConsensusStats,
    ConsensusError,
    NotLeaderError,
    QuorumNotReachedError,
)

# Durable log: pass db_path for SQLite persistence across restarts
node = RaftNode(ClusterConfig(node_id="n1", peers=["n2", "n3"]), db_path="consensus.db")

stats: ConsensusStats = node.stats()  # term, state, leader, log length...

# State machine snapshots
snap = node.state_machine.snapshot()
node.state_machine.restore(snap)
```

**Tuning knobs** (`ClusterConfig`):

| Field | Default | Meaning |
|---|---|---|
| `election_timeout_ms` | `(150, 300)` | Randomized follower timeout window before starting an election |
| `heartbeat_interval_ms` | `50` | Leader AppendEntries heartbeat cadence |

**Bring your own network** by subclassing `Transport` (`async send(target, message)` / `async receive()`); `InProcessTransport` is the in-memory default for tests and single-process demos.

---

## 🤖 CI / CD Integration (GitHub Actions)

Every push and PR to `master` runs the full matrix (Ubuntu / Windows / macOS × Python 3.9–3.13): install, pytest, CLI smoke test, then a wheel + sdist build with `twine check` in a clean venv.

---

## 🗺️ Swarm Ecosystem

SwarmConsensus is part of the **Swarm Primitives Ecosystem** for autonomous agent swarms:

- 🗳️ **SwarmConsensus**: Raft leader election, epoch leases, replicated log (this package).
- 🔒 **SwarmLock**: Tokenized, non-blocking distributed advisory locks.
- ⏱️ **SwarmCron**: Native high-precision background cron scheduling.
- 🛡️ **SwarmProof**: Truth Oracle, evidence ledgers, and anti-hallucination gates.

Built for the [Prismatic Engine](https://prismaticengine.com) swarm ecosystem.

---

## 📄 License

MIT — see [LICENSE](LICENSE).
