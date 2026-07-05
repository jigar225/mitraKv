# MitraKV

**Problem:** Existing options are either too heavy (etcd), not consistent enough for strong writes (Redis), or not embeddable. MitraKV is the simple version — a distributed key-value store built from scratch in Go with WAL persistence, Raft consensus, and production-style failure handling.

**What it does:** Runs as a single node or a 3-node cluster. Clients talk over TCP (`SET`/`GET`/`DEL`/`PING`). Writes go through the Raft leader, replicate to a majority, then land in the WAL and in-memory store. Reads work on any node.

---

## Quick start

```bash
cd mitrakv
go test ./... -race                              # run all tests
./scripts/start-cluster.sh                       # start 3-node cluster
echo "SET user jigar" | nc localhost 6379        # write (leader only)
echo "GET user" | nc localhost 6380              # read from any node
```

---

## How to run

### Single node (dev / local)

```bash
go run ./cmd/mitrakv --port 6379 --metrics-port 9090 --data-dir ./data/node1
```

### 3-node Raft cluster (production-style)

```bash
./scripts/start-cluster.sh
```

| Node  | Client port | Raft RPC port | Metrics |
|-------|-------------|---------------|---------|
| node1 | 6379        | 7379          | 9090    |
| node2 | 6380        | 7380          | 9091    |
| node3 | 6381        | 7381          | 9092    |

Stop the cluster with `Ctrl+C` in the terminal running `start-cluster.sh`.

---

## Architecture

```
                    ┌─────────────────────────────────────┐
                    │           Client (nc / app)          │
                    └─────────────────┬───────────────────┘
                                      │ TCP SET/GET/DEL
                    ┌─────────────────▼───────────────────┐
                    │  handler → Raft Propose (leader)    │
                    │              ↓ committed             │
                    │         WAL + in-memory store        │
                    └─────────────────┬───────────────────┘
                                      │ AppendEntries / RequestVote
              ┌───────────────────────┼───────────────────────┐
              │                       │                       │
         ┌────▼────┐             ┌────▼────┐             ┌────▼────┐
         │ node 1  │◄───RPC────►│ node 2  │◄───RPC────►│ node 3  │
         └─────────┘             └─────────┘             └─────────┘

Outbound RPCs: circuit breaker (3 failures → pause 30s) + exponential backoff (100/200/400ms)
```

**Write path:** Client → leader → Raft log → replicate to followers → majority ack → commit → WAL + store → `OK`

**Read path:** Client → any node → in-memory store → response (no Raft round-trip)

---

## What to test

### 1. Basic commands

```bash
echo "PING" | nc localhost 6379              # → PONG
echo "SET name jigar" | nc localhost 6379    # → OK
echo "GET name" | nc localhost 6379          # → jigar
echo "DEL name" | nc localhost 6379          # → OK
echo "GET name" | nc localhost 6379          # → (nil)
curl -s localhost:9090/metrics | head          # Prometheus metrics
```

### 2. Crash recovery (WAL)

```bash
echo "SET city mumbai" | nc localhost 6379   # → OK
# Ctrl+C the server, restart with same --data-dir
echo "GET city" | nc localhost 6379          # → mumbai
```

Data survives because every `SET`/`DEL` is written to `data_dir/wal.log` before the in-memory map updates.

### 3. Raft replication

```bash
./scripts/start-cluster.sh

# Write to leader (try 6379; if ERR not leader, try 6380 or 6381)
echo "SET user jigar" | nc localhost 6379    # → OK

# Read from any node — all should match
echo "GET user" | nc localhost 6379          # → jigar
echo "GET user" | nc localhost 6380          # → jigar
echo "GET user" | nc localhost 6381          # → jigar

# Write to follower — rejected
echo "SET x 1" | nc localhost 6380           # → ERR not leader
```

### 4. Leader failover

```bash
./scripts/kill-leader.sh 6379                # kill node on port 6379
sleep 3                                      # wait for re-election
echo "GET user" | nc localhost 6380          # → jigar (data survived)
echo "SET city mumbai" | nc localhost 6380   # → OK (new leader)
```

### 5. Failure scenarios (chaos scripts)

Cluster must be running first (`./scripts/start-cluster.sh`):

```bash
./scripts/chaos-kill-leader.sh      # kill leader → data survives, new leader elected
./scripts/chaos-kill-follower.sh    # kill follower → cluster still reads/writes
./scripts/chaos-no-quorum.sh        # kill 2 nodes → lone survivor refuses writes
```

### 6. Automated tests

```bash
go test ./... -race                 # unit + integration tests (always run before pushing)
```

---

## Benchmarks

### Run

```bash
go test -bench=. -benchtime=1x ./benchmarks/
```

### Results (MacBook, single node, 1000 concurrent connections)

| Benchmark | p50 | p95 | p99 | What it measures |
|-----------|-----|-----|-----|------------------|
| **GET** | 3.7ms | 7.4ms | 8.2ms | 1000 goroutines each `GET benchkey` at the same time |
| **SET** | 2.1ms | 3.4ms | 3.8ms | 1000 goroutines each `SET benchkey value` at the same time |
| **Mixed** | 3.2ms | 6.4ms | 6.4ms | 80% GET / 20% SET (realistic read-heavy workload) |

> Re-run on your machine to get fresh numbers. Cluster mode (Raft replication) will be slower than these single-node numbers.

### How the benchmark works

1. Starts a real MitraKV TCP server in-process (same code path as production).
2. Spawns **1000 goroutines** — each dials, sends one command, reads the response, records latency.
3. Sorts all 1000 latencies and reports **p50, p95, p99**.

### What p50 / p95 / p99 mean 

| Metric | Plain English | Example |
|--------|---------------|---------|
| **p50** (median) | Half of requests finished faster than this | "Typical user experience" |
| **p95** | 95% of requests finished faster than this | "Almost everyone, except slow outliers" |
| **p99** | 99% of requests finished faster than this | "Worst realistic case — tail latency" |

**Why not average?** Average hides slow requests. If 999 requests take 1ms and 1 takes 10 seconds, average looks fine (~11ms) but one user had a terrible experience. p99 catches that.

---

## Design decisions

| Decision | Why |
|----------|-----|
| **WAL before memory update** | If the process crashes after WAL write, restart replays the log. Memory-only would lose data. |
| **Raft over Paxos** | Same safety guarantees, easier to understand and implement. Industry default (etcd, Consul). |
| **Leader-only writes** | Strong consistency — every write goes through one ordered log. No conflicting writes on followers. |
| **Majority quorum** | 3 nodes, need 2 to commit. Survives 1 node failure. Kill 2 → no writes (chaos-no-quorum proves this). |
| **Circuit breaker on peer RPC** | Dead nodes were retried every 50ms heartbeat, flooding logs. 3 failures → stop for 30s → probe once. |
| **Exponential backoff** | 100ms → 200ms → 400ms between retries. Gives a recovering node time before we hammer it again. |
| **Text WAL** | Debuggable with `cat wal.log`. Tradeoff: larger on disk vs binary format. |
| **Pure Go stdlib** | No hidden dependencies. Only external package: Prometheus client for metrics. |

### What happens on a 2-1 network partition?

The side with **2 nodes** keeps quorum → can elect a leader and accept writes.

The **isolated single node** cannot reach majority → cannot commit new writes → returns errors or stays stale. This prevents split-brain (two leaders writing different data).

---

## Project structure

```
mitrakv/
├── cmd/mitrakv/main.go          # entry point
├── internal/
│   ├── server/                  # TCP server + command handler
│   ├── protocol/                # wire format (SET/GET/DEL/PING)
│   ├── store/                   # in-memory KV map
│   ├── wal/                     # write-ahead log + crash recovery
│   ├── raft/                    # election, replication, circuit breaker
│   └── metrics/                 # Prometheus instrumentation
├── benchmarks/bench_test.go       # latency benchmarks
├── config/node{1,2,3}.yaml       # cluster configs
└── scripts/                     # start-cluster, chaos tests
```
