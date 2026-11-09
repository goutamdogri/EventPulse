#!/usr/bin/env bash
#
# server launcher: start the Stage 0 lab server, optionally under a LOW fd
# limit, pinned to specific CPUs.
#
#   ./labs/stage-0-foundations/scripts/server-runner.sh                  # no ulimit change, pinned to CPUs 0,6
#   FD_LIMIT=256   ./labs/stage-0-foundations/scripts/server-runner.sh   # experiment: low fd limit
#   FD_LIMIT=65536 ./labs/stage-0-foundations/scripts/server-runner.sh   # control run: explicit high limit
#   SERVER_CPUS=2,3 ./labs/stage-0-foundations/scripts/server-runner.sh  # pin to other CPUs
#   SERVER_CPUS=-1  ./labs/stage-0-foundations/scripts/server-runner.sh  # no pinning (no taskset)
#
# It records the server PID / port / log path in a state file so the
# experiment runner (loadtests/k6/stage-0/E0.5/run.sh) can find it.
#
# FD limit:
#   If FD_LIMIT is NOT set, `ulimit` is not touched; the server inherits the
#   shell's limits and Node raises its own soft limit to the hard limit.
#   If FD_LIMIT IS set, we use plain `ulimit -n N` (sets soft AND hard). Node
#   raises its soft limit up to the hard limit at startup, so `ulimit -Sn 256`
#   alone would be silently undone and the experiment would never hit EMFILE.
#
# CPU pinning:
#   SERVER_CPUS is a taskset CPU list (e.g. "0,6", "0-3"). Default: 0,6.
#   Use -1 to start the server without taskset.
#   `exec taskset ... node` keeps the PID, so the PID in the state file is
#   still the node PID.

set -Eeuo pipefail

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

LAB_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

FD_LIMIT="${FD_LIMIT:-}"              # empty = leave limits alone
SERVER_CPUS="${SERVER_CPUS:-0,6}"     # -1 = do not pin
PORT="${PORT:-8000}"
STATE_DIR="${E05_STATE_DIR:-/tmp/eventpulse-e0.5}"
STATE_FILE="$STATE_DIR/server.env"
SERVER_LOG="$STATE_DIR/server.log"
ENTRY="${SERVER_ENTRY:-src/server.js}"

if [[ -n "$FD_LIMIT" ]]; then
    [[ "$FD_LIMIT" =~ ^[1-9][0-9]*$ ]] || die "FD_LIMIT must be a positive integer."
fi

if [[ "$SERVER_CPUS" != "-1" ]]; then
    [[ "$SERVER_CPUS" =~ ^[0-9]+(-[0-9]+)?(,[0-9]+(-[0-9]+)?)*$ ]] ||
        die "SERVER_CPUS must be a CPU list like '0,6' or '0-3', or -1 to disable pinning."
    command -v taskset >/dev/null 2>&1 || die "taskset not found in PATH (set SERVER_CPUS=-1 to skip pinning)."
fi

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

if [[ -n "$FD_LIMIT" ]]; then
    ulimit -n "$FD_LIMIT" ||
        die "Could not set 'ulimit -n $FD_LIMIT' (hard limit is $(ulimit -Hn))."
    FD_LIMIT_MODE="set"
else
    FD_LIMIT_MODE="inherited"
fi

if [[ "$SERVER_CPUS" == "-1" ]]; then
    CPU_PIN_MODE="none"
    RUN_CMD=(node "$ENTRY")
else
    CPU_PIN_MODE="taskset"
    RUN_CMD=(taskset -c "$SERVER_CPUS" node "$ENTRY")
fi

export PORT

# After `exec` below, this shell's PID *becomes* the node PID
# (taskset execs node in place, so the PID is preserved).
cat > "$STATE_FILE" <<STATE
SERVER_PID=$$
PORT=$PORT
FD_LIMIT_REQUESTED=${FD_LIMIT:-none}
FD_LIMIT_MODE=$FD_LIMIT_MODE
SERVER_CPUS=$SERVER_CPUS
CPU_PIN_MODE=$CPU_PIN_MODE
SERVER_LOG=$SERVER_LOG
STARTED_AT="$(date '+%Y-%m-%d %H:%M:%S')"
STATE

printf '================ E0.5 server ================\n'
printf ' pid          : %s\n' "$$"
printf ' port         : %s\n' "$PORT"
printf ' fd limit     : %s\n' "${FD_LIMIT:-not set (inherited, Node handles it)}"
printf ' ulimit -Sn   : %s\n' "$(ulimit -Sn)"
printf ' ulimit -Hn   : %s\n' "$(ulimit -Hn)"
if [[ "$CPU_PIN_MODE" == "taskset" ]]; then
    printf ' cpu pinning  : taskset -c %s\n' "$SERVER_CPUS"
else
    printf ' cpu pinning  : none (SERVER_CPUS=-1)\n'
fi
printf ' node         : %s\n' "$(node -v)"
printf ' log          : %s\n' "$SERVER_LOG"
printf '=============================================\n'

# Mirror stdout+stderr to a log file (tee -i: survive Ctrl-C until node exits).
exec > >(tee -ia "$SERVER_LOG") 2>&1

cd -- "$LAB_DIR"
exec "${RUN_CMD[@]}"