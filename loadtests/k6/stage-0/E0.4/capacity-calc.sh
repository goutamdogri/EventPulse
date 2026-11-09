#!/usr/bin/env bash
# Measures idle job time of a route and prints theoretical capacity C = WORKERS / job_time.
# NOTE: sequential calls measure ONE job on an idle machine. With WORKERS > 1 on SMT siblings
# the real job time inflates under concurrency, so C here is an UPPER BOUND.
set -euo pipefail

BASE="${BASE:-http://localhost:8000}"
ROUTE="${ROUTE:-/cpu-worker}"
N="${N:-38}"
RUNS="${RUNS:-5}"
WORKERS="${WORKERS:-1}"     # must match the WORKERS the server was started with

curl -fsS -o /dev/null "$BASE$ROUTE?n=$N"   # warm-up, discarded (JIT, worker spawn)

times=()
for ((i = 1; i <= RUNS; i++)); do
  times+=("$(curl -fsS -o /dev/null -w '%{time_total}' "$BASE$ROUTE?n=$N")")
done

echo "route=$ROUTE n=$N workers=$WORKERS"
echo "runs (s): ${times[*]}"
printf '%s\n' "${times[@]}" | awk -v w="$WORKERS" '
  { 
    s += $1;
    if (NR == 1 || $1 < mn) mn = $1;
    if ($1 > mx) mx = $1
  }
  END {
    m = s / NR
    printf "mean=%.4fs  min=%.4fs  max=%.4fs\n", m, mn, mx
    printf "C_theoretical = %.3f rps  (WORKERS=%d / mean)\n\n", w / m, w
    printf "Copy into the test command:\n  WORKERS=%d JOB_TIME=%.4f\n", w, m
  }'
