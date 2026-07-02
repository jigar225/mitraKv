# MitraKV

A distributed key-value store built from scratch in Go — TCP wire protocol, WAL persistence, and (Phase 3+) Raft consensus.

## Run

```bash
cd mitrakv
go run ./cmd/mitrakv --port 6379 --metrics-port 9090 --data-dir ./data/node1
```

## Verify (Phase 1)

```bash
echo "PING" | nc localhost 6379              # PONG
echo "SET name jigar" | nc localhost 6379    # OK
echo "GET name" | nc localhost 6379          # jigar
curl -s localhost:9090/metrics | head        # Prometheus metrics
```

## Verify WAL / crash recovery (Phase 2)

```bash
# 1) Start server (see Run above), then write data
echo "SET city mumbai" | nc localhost 6379   # OK

# 2) Stop server (Ctrl+C), then start again with the SAME --data-dir
go run ./cmd/mitrakv --port 6379 --data-dir ./data/node1

# 3) Data should survive restart
echo "GET city" | nc localhost 6379          # mumbai
```

WAL file location: `./data/node1/wal.log` (text format, one entry per line).

## Test

```bash
go test ./... -race
```

## Architecture (current)

```
Client (nc) → TCP server → protocol parser → handler → WAL (disk first) → in-memory store
                                                      ↘ metrics (/metrics on :9090)
Startup: replay WAL → rebuild memory → serve requests
```

## Phases

| Phase | Status |
|-------|--------|
| 1 — TCP + protocol + metrics | Done |
| 2 — WAL + crash recovery | Done |
| 3 — Raft (3 nodes) | Next |
| 4 — Failure modes + benchmarks | Planned |
