#!/usr/bin/env bash
# Stop the services started by make up (ingest-api, consumer-sink, admin-api, outbox-relay) (scripts/up.sh) (SIGTERM: graceful drain and commit).
# Containers keep running (they are shared with other stages): make kafka-down stops the brokers.
set -uo pipefail
cd "$(dirname "$0")/.."
for name in outbox-relay admin-api ingest-api consumer-sink; do
  f=".run/$name.pid"
  [ -f "$f" ] || { echo "$name: not started by make up"; continue; }
  pid=$(cat "$f")
  if kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid"
    for i in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
    kill -0 "$pid" 2>/dev/null && { echo "$name did not stop in 15 s, sending SIGKILL"; kill -KILL "$pid"; }
    echo "$name stopped"
  else
    echo "$name: not running"
  fi
  rm -f "$f"
done
