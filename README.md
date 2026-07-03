# MitraKV

A distributed key-value store built from scratch in Go — TCP wire protocol, WAL persistence, and Raft consensus.

## Run (single node)

```bash
cd mitrakv
go run ./cmd/mitrakv --port 6379 --metrics-port 9090 --data-dir ./data/node1
```

## Run (3-node Raft cluster)

```bash
cd mitrakv
./scripts/start-cluster.sh
```

Nodes:
| Node | Client port | Raft RPC port |
|------|-------------|---------------|
| node1 | 6379 | 7379 |
| node2 | 6380 | 7380 |
| node3 | 6381 | 7381 |

## Verify (Phase 1)

```bash
echo "PING" | nc localhost 6379              # PONG
echo "SET name jigar" | nc localhost 6379    # OK
echo "GET name" | nc localhost 6379          # jigar
curl -s localhost:9090/metrics | head        # Prometheus metrics
```

## Verify WAL / crash recovery (Phase 2)

```bash
echo "SET city mumbai" | nc localhost 6379   # OK
# Stop server (Ctrl+C), restart with same --data-dir
echo "GET city" | nc localhost 6379          # mumbai
```

## Verify Raft cluster (Phase 3)

```bash
# 1) Start cluster
./scripts/start-cluster.sh

# 2) Write to leader (try node1 first; if ERR not leader, try 6380/6381)
echo "SET user jigar" | nc localhost 6379

# 3) Read from any node
echo "GET user" | nc localhost 6380          # jigar

# 4) Kill leader process on port 6379
./scripts/kill-leader.sh 6379

# 5) Wait ~3s for re-election, write/read again on another port
echo "GET user" | nc localhost 6380          # jigar
```

**Note:** Only the **leader** accepts `SET`/`DEL`. Followers return `ERR not leader`.

## Test

```bash
go test ./... -race
```

## Architecture

```
Client → TCP (:6379) → handler → Raft Propose (leader only) → replicate to peers
                                    ↓ committed
                              WAL + in-memory store (all nodes)
Peers  → Raft RPC (:7379) → RequestVote / AppendEntries
```

## Phases

| Phase | Status |
|-------|--------|
| 1 — TCP + protocol + metrics | Done |
| 2 — WAL + crash recovery | Done |
| 3 — Raft (3 nodes) | Done |
| 4 — Failure modes + benchmarks | Next |
