#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

mkdir -p ./data/node1 ./data/node2 ./data/node3

echo "Starting MitraKV 3-node cluster..."
go run ./cmd/mitrakv --config ./config/node1.yaml &
PID1=$!
go run ./cmd/mitrakv --config ./config/node2.yaml &
PID2=$!
go run ./cmd/mitrakv --config ./config/node3.yaml &
PID3=$!

echo "node1 pid=$PID1 (client :6379, raft :7379)"
echo "node2 pid=$PID2 (client :6380, raft :7380)"
echo "node3 pid=$PID3 (client :6381, raft :7381)"
echo "Cluster running. Press Ctrl+C to stop."

trap 'kill $PID1 $PID2 $PID3 2>/dev/null || true' INT TERM
wait
