#!/usr/bin/env bash
set -euo pipefail

# Kills the process listening on node1 client port (default first write target in PRD).
PORT="${1:-6379}"

PIDS="$(lsof -ti tcp:"$PORT" || true)"
if [[ -z "$PIDS" ]]; then
  echo "No process found on port $PORT"
  exit 1
fi

echo "Killing leader candidate on port $PORT: $PIDS"
kill $PIDS
echo "Done. Watch remaining node logs for leader re-election."
