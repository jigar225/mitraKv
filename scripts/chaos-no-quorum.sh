#!/usr/bin/env bash
# Chaos: kill 2 of 3 nodes — cluster must not accept new writes (no quorum).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

port_alive() {
  local port="$1"
  echo "PING" | nc -w 1 localhost "$port" >/dev/null 2>&1
}

try_set() {
  local port="$1"
  local resp
  resp="$(echo "SET chaos_quorum x" | nc -w 1 localhost "$port" 2>/dev/null || true)"
  [[ "$resp" == "OK" ]]
}

echo "== chaos: no quorum (2 of 3 nodes down) =="

ALIVE=()
for port in 6379 6380 6381; do
  if port_alive "$port"; then
    ALIVE+=("$port")
  fi
done

if [[ "${#ALIVE[@]}" -lt 2 ]]; then
  echo "Need at least 2 live nodes before chaos. Start cluster with ./scripts/start-cluster.sh"
  exit 1
fi

KILL_PORTS=("${ALIVE[@]:0:2}")
for port in "${KILL_PORTS[@]}"; do
  PIDS="$(lsof -ti tcp:"$port" || true)"
  if [[ -n "$PIDS" ]]; then
    echo "killing node on port $port pid(s): $PIDS"
    kill $PIDS
  fi
done

sleep 2

SURVIVOR=""
for port in 6379 6380 6381; do
  if port_alive "$port"; then
    SURVIVOR="$port"
    break
  fi
done

if [[ -z "$SURVIVOR" ]]; then
  echo "FAIL: no survivor node left"
  exit 1
fi

if try_set "$SURVIVOR"; then
  echo "FAIL: write succeeded without quorum on port $SURVIVOR"
  exit 1
fi

echo "PASS: lone survivor on port $SURVIVOR refused write (no quorum)"
