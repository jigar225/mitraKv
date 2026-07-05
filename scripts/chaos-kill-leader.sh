#!/usr/bin/env bash
# Chaos: kill leader mid-write, wait for re-election, verify data survives.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

try_set() {
  local port="$1"
  local key="$2"
  local value="$3"
  local resp
  resp="$(echo "SET $key $value" | nc -w 1 localhost "$port" 2>/dev/null || true)"
  [[ "$resp" == "OK" ]]
}

try_get() {
  local port="$1"
  local key="$2"
  local expected="$3"
  local resp
  resp="$(echo "GET $key" | nc -w 1 localhost "$port" 2>/dev/null || true)"
  [[ "$resp" == "$expected" ]]
}

find_leader_port() {
  local key="$1"
  local value="$2"
  for port in 6379 6380 6381; do
    if try_set "$port" "$key" "$value"; then
      echo "$port"
      return 0
    fi
  done
  return 1
}

echo "== chaos: kill leader mid-write =="

LEADER_PORT="$(find_leader_port chaos_user jigar)" || {
  echo "FAIL: no leader found. Start cluster with ./scripts/start-cluster.sh"
  exit 1
}
echo "leader on port $LEADER_PORT"

PIDS="$(lsof -ti tcp:"$LEADER_PORT" || true)"
if [[ -z "$PIDS" ]]; then
  echo "FAIL: no process on leader port $LEADER_PORT"
  exit 1
fi

echo "killing leader pid(s): $PIDS"
kill $PIDS

echo "waiting 3s for re-election..."
sleep 3

SURVIVOR_PORT=6380
if [[ "$LEADER_PORT" == "6380" ]]; then
  SURVIVOR_PORT=6381
fi

if try_get "$SURVIVOR_PORT" chaos_user jigar; then
  echo "PASS: data survived leader kill (GET chaos_user => jigar on :$SURVIVOR_PORT)"
else
  echo "FAIL: data missing after leader kill"
  exit 1
fi

NEW_LEADER="$(find_leader_port chaos_city mumbai)" || {
  echo "FAIL: cluster did not elect a new leader within timeout"
  exit 1
}
echo "PASS: new leader elected on port $NEW_LEADER"
