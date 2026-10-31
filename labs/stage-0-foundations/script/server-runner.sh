#!/usr/bin/env bash
#
# E0.5 server launcher: start the Stage 0 lab server under a LOW fd limit.
#
#   ./labs/stage-0-foundations/scripts/e0.5-server.sh                 # limit 256 (experiment)
#   FD_LIMIT=65536 ./labs/stage-0-foundations/scripts/e0.5-server.sh  # control run
#
# It records the server PID / port / log path in a state file so the
# experiment runner (loadtests/k6/stage-0/E0.5/run.sh) can find it.
#
# NOTE: we use plain `ulimit -n N` (sets soft AND hard). Node raises its own
# soft limit up to the hard limit at startup, so `ulimit -Sn 256` alone would
# silently be undone and the experiment would never hit EMFILE.

set -Eeuo pipefail

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

LAB_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

FD_LIMIT="${FD_LIMIT:-256}"
PORT="${PORT:-8000}"
STATE_DIR="${E05_STATE_DIR:-/tmp/eventpulse-e0.5}"
STATE_FILE="$STATE_DIR/server.env"
SERVER_LOG="$STATE_DIR/server.log"
ENTRY="${SERVER_ENTRY:-src/server.js}"

[[ "$FD_LIMIT" =~ ^[1-9][0-9]*$ ]] || die "FD_LIMIT must be a positive integer."
[[ -f "$LAB_DIR/$ENTRY" ]]         || die "Server entry not found: $LAB_DIR/$ENTRY"
command -v node >/dev/null 2>&1    || die "node not found in PATH."

if [[ -f "$STATE_FILE" ]]; then
    # shellcheck disable=SC1090
    old_pid="$(. "$STATE_FILE"; echo "${SERVER_PID:-}")"
    if [[ -n "$old_pid" ]] && kill -0 "$old_pid" 2>/dev/null; then
        die "A previous E0.5 server is still running (pid $old_pid). Stop it first."
    fi
fi

mkdir -p -- "$STATE_DIR"
: > "$SERVER_LOG"

ulimit -n "$FD_LIMIT" ||
    die "Could not set 'ulimit -n $FD_LIMIT' (hard limit is $(ulimit -Hn))."

export PORT

# After `exec node` below, this shell's PID *becomes* the node PID.
cat > "$STATE_FILE" <<STATE
SERVER_PID=$$
PORT=$PORT
FD_LIMIT_REQUESTED=$FD_LIMIT
SERVER_LOG=$SERVER_LOG
STARTED_AT="$(date '+%Y-%m-%d %H:%M:%S')"
STATE

printf '================ E0.5 server ================\n'
printf ' pid          : %s\n' "$$"
printf ' port         : %s\n' "$PORT"
printf ' ulimit -Sn   : %s\n' "$(ulimit -Sn)"
printf ' ulimit -Hn   : %s\n' "$(ulimit -Hn)"
printf ' node         : %s\n' "$(node -v)"
printf ' log          : %s\n' "$SERVER_LOG"
printf '=============================================\n'

# Mirror stdout+stderr to a log file (tee -i: survive Ctrl-C until node exits).
exec > >(tee -ia "$SERVER_LOG") 2>&1

cd -- "$LAB_DIR"
exec node "$ENTRY"