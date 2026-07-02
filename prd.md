# MitraKV — Project PRD
### Distributed Key-Value Store in Go | Built from scratch

---

## Problem Statement

Distributed systems need a reliable place to store configuration and shared state. Nothing simple exists that is fast, crash-proof, and strongly consistent.

Existing options are either too heavy (etcd), not consistent enough (Redis), or not embeddable. **MitraKV is the simple version — built from scratch in Go with WAL persistence and Raft consensus.**

No domain lock-in. Any system that needs coordinated state can use it — the same way etcd was just "distributed /etc" before Kubernetes existed.

---

## What is this project?

MitraKV is a distributed, fault-tolerant key-value store built from scratch in Go.
It is NOT a tutorial clone. It is an original system that demonstrates:

- How data is stored and recovered at the disk level (WAL + storage engine)
- How multiple nodes agree on writes even when one crashes (Raft consensus)
- How a system behaves under failure and load (circuit breakers, benchmarks)

**Resume line:**
> Built MitraKV — a distributed key-value store in Go from scratch, implementing WAL-based persistence, Raft consensus across 3 nodes, and p99 latency benchmarks under concurrent load

---

## Why this name?

- **Mitra** (मित्र) = friend, ally in Sanskrit. Your reliable distributed ally — nodes that stand together and don't lose your data.
- **KV** = key-value store. Same pattern as TiKV, DKV — sounds like real infra, not a tutorial.
- Indian origin, globally pronounceable (MEE-trah-K-V). Two syllables on the root word.
- No major GitHub collision with an existing distributed KV project.
- Short, memorable, searchable on GitHub.

---

## Tech Stack

| Layer | Choice | Why |
|---|---|---|
| Language | Go | Your strongest language, correct for this domain |
| Transport | Raw TCP (net package) | Not HTTP — forces real protocol design |
| Storage | Custom WAL + in-memory map | You build it, you understand it |
| Consensus | Raft (implement from scratch) | Industry standard, teachable in interviews |
| Observability | Prometheus + slog | Already know this from sessions 17-18 |
| Testing | go test -race + benchmarks | Proves correctness under concurrency |
| Config | YAML / env vars | 3-node cluster config |

---

## Project Structure

```
mitrakv/
├── cmd/
│   └── mitrakv/
│       └── main.go              # Entry point, starts node
├── internal/
│   ├── server/
│   │   ├── server.go            # TCP server, accepts connections
│   │   └── handler.go           # Parses commands, routes to store
│   ├── store/
│   │   ├── store.go             # In-memory KV map, thread-safe
│   │   └── store_test.go
│   ├── wal/
│   │   ├── wal.go               # Write-Ahead Log, disk persistence
│   │   ├── wal_test.go
│   │   └── recovery.go          # Replay WAL on startup
│   ├── raft/
│   │   ├── node.go              # Raft node state machine
│   │   ├── log.go               # Raft log entries
│   │   ├── election.go          # Leader election logic
│   │   ├── replication.go       # AppendEntries RPC
│   │   └── raft_test.go
│   ├── protocol/
│   │   ├── protocol.go          # Custom wire protocol (encode/decode)
│   │   └── protocol_test.go
│   └── metrics/
│       └── metrics.go           # Prometheus counters, histograms
├── config/
│   ├── node1.yaml
│   ├── node2.yaml
│   └── node3.yaml
├── scripts/
│   ├── start-cluster.sh         # Starts all 3 nodes locally
│   └── kill-leader.sh           # Chaos test: kills leader, watch re-election
├── benchmarks/
│   └── bench_test.go            # p99 latency under load
├── docs/
│   └── architecture.md          # Design decisions, tradeoffs, diagrams
├── go.mod
├── go.sum
└── README.md                    # The document you show in interviews
```

---

## Phases — what to build, in order

### Phase 1 — Single Node KV Store (Week 1)

**Goal:** A working key-value store that accepts TCP connections and handles commands.

**What Cursor builds:**

1. Raw TCP server using `net.Listen` and goroutine-per-connection
2. Custom protocol parser:
   - `SET key value\n` → stores key
   - `GET key\n` → returns value or `(nil)`
   - `DEL key\n` → deletes key
   - `PING\n` → returns `PONG`
3. In-memory store using `map[string]string` with `sync.RWMutex`
4. Structured logging with `slog` (request in, response out, errors)
5. Prometheus metrics endpoint on `/metrics`:
   - `mitrakv_requests_total` counter by command
   - `mitrakv_request_duration_seconds` histogram

**You learn:**
- Raw TCP programming in Go (not net/http)
- Designing a wire protocol from scratch
- Concurrency safety with real read/write patterns

**Done when:**
```bash
# Terminal 1
go run ./cmd/mitrakv --port 6379

# Terminal 2
echo "SET name jigar" | nc localhost 6379
# → OK
echo "GET name" | nc localhost 6379
# → jigar
```

---

### Phase 2 — WAL: Survive a Crash (Week 2)

**Goal:** Data survives process restart. No data loss on crash.

**What Cursor builds:**

1. WAL (Write-Ahead Log) — append-only file on disk
   - Every SET/DEL writes a log entry BEFORE updating memory
   - Log entry format: `[timestamp][op][key][value]\n`
2. Recovery on startup:
   - Read WAL file from beginning
   - Replay every entry into memory map
   - State restored exactly as before crash
3. Log rotation (optional stretch): when WAL exceeds 100MB, snapshot + new log

**The rule:** Write to disk FIRST. Update memory SECOND.
If the process dies between step 1 and 2 — recovery replays the disk entry and memory catches up.
If the process dies before step 1 — nothing was committed, no partial state.

**You learn:**
- Why WAL exists (connects directly to Phase 4 Session 21)
- fsync behavior and durability guarantees
- Crash recovery as a first-class design concern

**Done when:**
```bash
go run ./cmd/mitrakv --port 6379 --data ./data/node1

# Set some keys
echo "SET city mumbai" | nc localhost 6379

# Kill the process (Ctrl+C or kill -9)
# Restart it
go run ./cmd/mitrakv --port 6379 --data ./data/node1

# Keys still there
echo "GET city" | nc localhost 6379
# → mumbai
```

---

### Phase 3 — Raft: 3 Nodes, One Leader (Weeks 3-4)

**Goal:** 3 MitraKV nodes run simultaneously. They elect a leader. All writes go through the leader. If the leader dies, a new one is elected in under 3 seconds.

**What Cursor builds:**

1. Raft node state machine (Follower → Candidate → Leader)
2. Leader election:
   - Followers wait for heartbeat from leader
   - If no heartbeat in `election_timeout` (150-300ms random) → become Candidate
   - Candidate requests votes from all peers
   - Majority wins → becomes Leader
3. Log replication (AppendEntries RPC):
   - Client writes to Leader
   - Leader appends to its own log + WAL
   - Leader sends AppendEntries to all Followers
   - Once majority acknowledge → entry is committed
   - Leader applies to state machine → responds OK to client
4. Heartbeat: Leader sends empty AppendEntries every 50ms to prevent new elections

**Key constraint:** A write is only confirmed to the client AFTER a majority of nodes have written it. This is what "strong consistency" means in practice.

**You learn:**
- Why Raft exists (connects directly to Phase 4 Session 24)
- What "majority quorum" means concretely
- The difference between committed and applied entries
- Leader election as a real distributed systems problem

**Done when:**
```bash
# Start 3 nodes
./scripts/start-cluster.sh

# Write to leader (auto-discovered or hardcoded for now)
echo "SET user jigar" | nc localhost 6379

# Kill the leader
./scripts/kill-leader.sh

# Watch logs — new leader elected in < 3s
# Write again — still works
echo "GET user" | nc localhost 6380
# → jigar
```

---

### Phase 4 — Failure Modes + Benchmarks (Week 5-6)

**Goal:** Prove the system holds under pressure. Document it.

**What Cursor builds:**

1. Circuit breaker on client-side:
   - If a peer is unreachable for N consecutive attempts → mark as DOWN
   - Stop sending requests → re-check after 30s
   - Pattern: closed → open → half-open → closed
2. Retry with exponential backoff:
   - Failed write to follower → retry after 100ms, 200ms, 400ms
   - Max 3 retries → log error + circuit open
3. Benchmark suite (`benchmarks/bench_test.go`):
   - `BenchmarkGET` — 1000 concurrent GET requests, measure p50/p95/p99
   - `BenchmarkSET` — 1000 concurrent SET requests, measure p50/p95/p99
   - `BenchmarkMixed` — 80% GET, 20% SET (realistic workload)
4. Chaos tests (scripts):
   - Kill leader mid-write → verify no data loss
   - Kill follower → verify cluster still serves reads
   - Kill 2 of 3 nodes → verify cluster refuses writes (no quorum)
5. Update README with real numbers

**You learn:**
- Circuit breaker pattern (connects to Phase 4 Session 23)
- What p99 latency means and why it matters more than average
- How to design for failure, not just the happy path

**Done when:**
```
# README shows real numbers like:
BenchmarkGET    p50: 0.8ms   p95: 2.1ms   p99: 4.3ms   (1000 concurrent)
BenchmarkSET    p50: 1.2ms   p95: 3.4ms   p99: 6.7ms   (1000 concurrent)
```

---

## README structure (what interviewers see)

The README is the most important file. Structure it as:

```
1. Problem statement (1 line)
   "Existing options are either too heavy (etcd), not consistent enough (Redis),
    or not embeddable. MitraKV is the simple version built from scratch in Go."
2. What is MitraKV (2 lines)
3. Architecture diagram (draw.io or ASCII)
4. How to run (3 commands max)
5. Benchmark results (real numbers)
6. Design decisions + tradeoffs (this is where interviews happen)
   - Why WAL before memory update?
   - Why Raft and not Paxos?
   - What happens if network partitions 2-1?
   - How does your circuit breaker decide when to re-open?
7. What's missing / what you'd do next (shows maturity)
```

---

## What you say in interviews

**"Tell me about a project you're proud of"**

> "I built MitraKV — a distributed key-value store from scratch in Go. The name means 'ally' in Sanskrit — nodes that stand together and don't lose your data. It runs as a 3-node cluster using Raft consensus. Each write is replicated to a majority before being confirmed to the client. It uses a write-ahead log for crash recovery — so even if the process dies mid-write, data is not lost. I benchmarked it at p99 under 5ms for GET under 1000 concurrent connections. The interesting part was implementing leader election from scratch — understanding why the election timeout needs to be randomized to avoid split votes."

That answer covers: distributed systems, consensus, persistence, concurrency, benchmarking, and real tradeoff thinking. No interviewer at an infra company walks away unimpressed.

---

## Phases mapped to your Phase 4 sessions

| MitraKV Phase | Phase 4 Session | What connects |
|---|---|---|
| Phase 1 — TCP server | Sessions 17-18 (goroutines, sync) | Concurrency you already know |
| Phase 2 — WAL | Session 21 (LSM + WAL) | You build what you just studied |
| Phase 3 — Raft | Session 24 (Raft + CAP) | You implement what you just studied |
| Phase 4 — Failures | Session 23 (failure modes) | Circuit breakers you just studied |
| Phase 4 — Benchmarks | Session 19 (pprof, benchmarks) | You already know this tool |

**The pattern:** Study the concept in Phase 4 → immediately implement it in MitraKV.
Theory and code together. This is why this project suits your learning style.

---

## Cursor instructions (paste this at the start of each session)

```
You are helping me build MitraKV — a distributed key-value store in Go.
Built from scratch. No Redis libraries. No etcd libraries for Raft.

Current phase: [FILL IN — Phase 1 / 2 / 3 / 4]
Current task: [FILL IN — e.g. "implement WAL recovery on startup"]

Rules:
- Pure Go standard library only (no external packages except prometheus client)
- Every function needs a comment explaining WHY, not just what
- Every new file needs a corresponding _test.go file
- No panics — all errors must be handled and logged
- Use slog for all logging (already set up)
- When you add a feature, tell me what to test manually to verify it works

Project structure is in /mitrakv — respect the existing layout.
```

---

## Timeline

| Week | Focus |
|---|---|
| Week 1 | Phase 1 — TCP server + protocol + in-memory store |
| Week 2 | Phase 2 — WAL + crash recovery (after Phase 4 Session 21) |
| Week 3-4 | Phase 3 — Raft consensus (after Phase 4 Session 24) |
| Week 5-6 | Phase 4 — Failure modes + benchmarks + README |

**Run parallel to Phase 4 sessions.**
Finish a Phase 4 session → implement that concept in MitraKV the next day.

---

*MitraKV — built by Jigar. Not a tutorial. A system.*
