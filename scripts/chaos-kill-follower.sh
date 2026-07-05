#!/usr/bin/env bash
# Chaos: kill one follower, cluster should still serve reads and leader writes.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

try_set() {
  local port="$1"
  local resp
  resp="$(echo "SET chaos_follower key" | nc -w 1 localhost "$port" 2>/dev/null || true)"
  [[ "$resp" == "OK" ]]
}

try_get() {
  local port="$1"
  local resp
  resp="$(echo "GET chaos_follower" | nc -w 1 localhost "$port" 2>/dev/null || true)"
  [[ "$resp" == "key" ]]
}

echo "== chaos: kill follower =="

LEADER_PORT=""
for port in 6379 6380 6381; do
  if try_set "$port"; then
    LEADER_PORT="$port"
    break
  fi
done

if [[ -z "$LEADER_PORT" ]]; then
  echo "FAIL: no leader found. Start cluster with ./scripts/start-cluster.sh"
  exit 1
fi
echo "leader on port $LEADER_PORT"

FOLLOWER_PORT=6381
if [[ "$LEADER_PORT" == "6381" ]]; then
  FOLLOWER_PORT=6380
fi

PIDS="$(lsof -ti tcp:"$FOLLOWER_PORT" || true)"
if [[ -z "$PIDS" ]]; then
  echo "FAIL: no follower on port $FOLLOWER_PORT"
  exit 1
fi

echo "killing follower on port $FOLLOWER_PORT pid(s): $PIDS"
kill $PIDS
sleep 2

if try_get "$LEADER_PORT"; then
  echo "PASS: leader still serves reads after follower kill"
else
  echo "FAIL: read failed after follower kill"
  exit 1
fi

if try_set "$LEADER_PORT"; then
  echo "PASS: leader still accepts writes with 2/3 quorum"
else
  echo "FAIL: write failed after follower kill"
  exit 1
fi
