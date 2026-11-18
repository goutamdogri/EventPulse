#!/usr/bin/env bash
# pg-container-exporter.sh - Postgres container CPU / memory / disk for Prometheus
#
# Why: B1 asks for "Postgres CPU" and "disk utilization". node_exporter only sees
# the whole machine, not one container. Docker puts every container in its own
# cgroup (a kernel group that counts CPU, memory and IO for its processes), so
# I read the numbers straight from the cgroup files and write them in the
# node_exporter textfile format, the same way listen-queue-exporter.sh does.
#
# Usage:  PG_CONTAINER=postgres ./infra/scripts/pg-container-exporter.sh &
# Env:    PG_CONTAINER   container name (required)
#         INTERVAL       seconds between samples (default 1)
#         TEXTFILE_DIR   default <repo>/infra/compose/textfile
#
# Needs cgroup v2 (check with `stat -fc %T /sys/fs/cgroup`,
# it should print cgroup2fs). Does not work on Docker Desktop / WSL if the cgroup
# files are not visible from the host; it then exits with a message.
set -u
PG_CONTAINER="${PG_CONTAINER:?set PG_CONTAINER to the postgres container name}"
INTERVAL="${INTERVAL:-1}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEXTFILE_DIR="${TEXTFILE_DIR:-$HERE/../compose/textfile}"
OUT="$TEXTFILE_DIR/ep_pg_container.prom"

cid=$(docker inspect -f '{{.Id}}' "$PG_CONTAINER") || { echo "container $PG_CONTAINER not found" >&2; exit 1; }
# systemd cgroup driver: .../system.slice/docker-<id>.scope ; cgroupfs driver: .../docker/<id>
cg=$(find /sys/fs/cgroup -maxdepth 4 -type d \( -name "docker-$cid.scope" -o -name "$cid" \) 2>/dev/null | head -1)
[ -n "$cg" ] && [ -f "$cg/cpu.stat" ] || { echo "cgroup v2 files for $PG_CONTAINER not found (cgroup v1 or Docker Desktop?)" >&2; exit 1; }
mkdir -p "$TEXTFILE_DIR"
trap 'rm -f "$OUT" "$OUT.tmp"; exit 0' INT TERM EXIT
echo "pg-container-exporter: $PG_CONTAINER -> $cg -> $OUT every ${INTERVAL}s" >&2

while :; do
  # cpu.stat lines: usage_usec / user_usec / system_usec / nr_throttled / throttled_usec ...
  # throttled_usec is the time the container was PAUSED because it hit its CPU limit (cpus: 2).
  cpu=$(awk '$1=="user_usec"{u=$2} $1=="system_usec"{s=$2} $1=="throttled_usec"{t=$2} END{printf "%.6f %.6f %.6f", u/1e6, s/1e6, t/1e6}' "$cg/cpu.stat")
  mem=$(cat "$cg/memory.current" 2>/dev/null || echo 0)
  # io.stat: one line per device, e.g. "259:0 rbytes=.. wbytes=.. rios=.. wios=.." -> add all devices up
  io=$(awk '{for(i=2;i<=NF;i++){split($i,kv,"="); if(kv[1]=="rbytes")r+=kv[2]; if(kv[1]=="wbytes")w+=kv[2]}} END{printf "%d %d", r, w}' "$cg/io.stat" 2>/dev/null)
  set -- $cpu; user=$1; sys=$2; thr=$3
  set -- $io;  rb=${1:-0}; wb=${2:-0}
  {
    echo '# TYPE ep_pg_container_cpu_seconds_total counter'
    echo "ep_pg_container_cpu_seconds_total{mode=\"user\"} $user"
    echo "ep_pg_container_cpu_seconds_total{mode=\"system\"} $sys"
    echo '# TYPE ep_pg_container_cpu_throttled_seconds_total counter'
    echo "ep_pg_container_cpu_throttled_seconds_total $thr"
    echo '# TYPE ep_pg_container_memory_bytes gauge'
    echo "ep_pg_container_memory_bytes $mem"
    echo '# TYPE ep_pg_container_disk_bytes_total counter'
    echo "ep_pg_container_disk_bytes_total{direction=\"read\"} $rb"
    echo "ep_pg_container_disk_bytes_total{direction=\"write\"} $wb"
  } > "$OUT.tmp" && mv "$OUT.tmp" "$OUT"     # write then rename, so node_exporter never reads half a file
  sleep "$INTERVAL"
done
