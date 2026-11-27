#!/usr/bin/env bash
# SIGKILL the consumer-sink process listening on a metrics port (a hard crash: no commit, no cleanup).
# Usage: scripts/kill-consumer.sh [PORT]   (default 8001)
set -euo pipefail
PORT="${1:-8001}"
PID=$(ss -ltnp "sport = :$PORT" 2>/dev/null | grep -o 'pid=[0-9]*' | head -1 | cut -d= -f2)
[ -n "$PID" ] || { echo "nothing listening on :$PORT"; exit 1; }
kill -KILL "$PID" && echo "SIGKILLed consumer pid $PID (port $PORT)"
