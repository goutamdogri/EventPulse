#!/usr/bin/env bash
# K1: Throughput versus the Stage 1 baseline
# ONE command runs a whole pass:
#     ./run.sh coarse     wide rates, 30 s, 1 repeat  -> finds roughly where the new knee is
#     ./run.sh fine       even steps around that knee, 60 s, 3 repeats, + 2 probe runs
#     ./run.sh ceiling    kafka-producer-perf-test.sh alone (no API, no k6) -> the raw broker ceiling
#
# What a pass does (so I can re-read it later):
#   1. preflight   - tools, Prometheus, node_exporter, brokers healthy, ingest-api built, consumer-sink OFF
#   2. pinning     - docker update --cpuset-cpus: brokers get their OWN cores (see CPU plan), restored on exit
#   3. env snapshot- versions, broker settings, API knobs, pinning, saved next to the results
#   4. for every repeat x rate: stop server, delete + recreate events.raw, start server, warm up, k6 via runner.sh,
#                    light snapshots before/after, audit (202s vs growth of the topic), one summary row
#   5. probes (fine pass only) - SEPARATE short runs where heavy tools (top -H, docker stats, iostat) are allowed.
#                    Never used for headline numbers (Stage 0 lesson: strace inside a run polluted it).
#   ceiling mode: no server and no k6. Only the perf tool runs, in a throw-away container.
#
# Usage:  DRY_RUN=1 ./run.sh coarse             (print the plan, run nothing)
#         RATES="2000 8000" REPEATS=1 RESULTS_NAME=k1-trial ./run.sh coarse     (small trial)
#         MANAGE_SERVER=0 ./run.sh coarse       (I started ingest-api myself; then no per-run restart)
#
# CPU plan (12 logical CPUs = 6 cores, SMT pairs 0/6 1/7 2/8 3/9 4/10 5/11). Same as B1 for server and k6, so the
# comparison with Stage 1 is fair. Postgres is idle in K1 (the sink is off), so its cores go to the brokers:
#   server   0,6         brokers  1,7,2,8 (3 brokers, 1 CPU quota each, they share these 4 threads)
#   k6       3,9,4,10    monitoring + idle postgres  5,11
set -euo pipefail

MODE="${1:-coarse}"
case "$MODE" in coarse|fine|ceiling) ;; *) echo "usage: $0 coarse|fine|ceiling" >&2; exit 1;; esac
EXP_ID="K1"

# ---- 1. Settings (all can be overridden from the environment) ---------------
case "$MODE" in
  coarse)  RATES="${RATES:-1000 2500 5000 7500 10000 15000 20000 30000}"   # wide steps to find the knee
           REPEATS="${REPEATS:-1}"; DURATION="${DURATION:-30s}"; PROBES="${PROBES:-0}"
           RESULTS_NAME="${RESULTS_NAME:-k1-coarse}";;
  fine)    REPEATS="${REPEATS:-3}"; DURATION="${DURATION:-60s}"; PROBES="${PROBES:-1}"
           RESULTS_NAME="${RESULTS_NAME:-k1-fine}";;
  ceiling) REPEATS="${REPEATS:-3}"; RATES=""; DURATION="-"; PROBES=0
           RESULTS_NAME="${RESULTS_NAME:-k1-ceiling}";;
esac
PROBE_DURATION="${PROBE_DURATION:-30s}"
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
MANAGE_SERVER="${MANAGE_SERVER:-1}"
SERVER_CPUS="${SERVER_CPUS:-0,6}"
KAFKA_CPUS="${KAFKA_CPUS:-1,7,2,8}"
export K6_CPUS="${K6_CPUS:-3,9,4,10}"           # runner.sh reads this (taskset for k6); the perf tool container uses it too
MON_CPUS="${MON_CPUS:-5,11}"
PG_CONTAINER="${PG_CONTAINER:-}"                # empty = auto-detect (it is only moved out of the brokers' way)
MON_CONTAINERS="${MON_CONTAINERS:-}"            # empty = auto-detect (prometheus, grafana, node-exporter)
KAFKA_CONTAINERS="${KAFKA_CONTAINERS:-}"        # empty = auto-detect (ep-kafka1..3)
KEEP_PINNING="${KEEP_PINNING:-0}"               # 1 = do not restore the old cpusets at the end
DRY_RUN="${DRY_RUN:-0}"
OVERHEAD_S=60                                   # restart + topic recreate + warm-up + settle per run (rough)

# Topic shape: the same as the real events.raw (kafka-init in docker-compose.kafka.yml). Checked with --describe.
TOPIC="${TOPIC:-events.raw}"; PARTITIONS="${PARTITIONS:-6}"; RETENTION_MS="${RETENTION_MS:-86400000}"

# API knobs. Only the first two are set by me; the rest are left to the API's own defaults unless I export them.
#  KAFKA_KEY_MODE=event: the key is the event id, so records spread over all 6 partitions. The default (tenant)
#  would put every event of my single API key on ONE partition, and I would measure that skew, not the API.
K_BROKERS="${KAFKA_BROKERS:-localhost:9092,localhost:9094,localhost:9096}"
K_KEY_MODE="${KAFKA_KEY_MODE:-event}"
API_KNOBS=(KAFKA_ACKS KAFKA_IDEMPOTENT KAFKA_COMPRESSION KAFKA_LINGER_MS BATCH_MAX_MESSAGES BATCH_MAX_WAIT_MS BATCH_MAX_BYTES BATCH_MAX_IN_FLIGHT BATCH_MAX_QUEUE)

# Raw broker ceiling (ceiling mode). Record size 500 = the JSON value the API produces for the k6 payload (about 496 B).
RECORD_SIZE="${RECORD_SIZE:-500}"; NUM_RECORDS="${NUM_RECORDS:-2000000}"; WARM_RECORDS="${WARM_RECORDS:-300000}"
LINGER_MS="${LINGER_MS:-5}"
PROFILES="${PROFILES:-big:262144 small:32768}"  # name:batch.size bytes. "small" is closer to the API's batches

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-2/K1
STAGE_DIR="$(dirname "$HERE")"                             # loadtests/k6/stage-2
RUNNER_DIR="$(dirname "$STAGE_DIR")"                       # loadtests/k6  (runner.sh and analyze.py live here)
ANALYZE="$RUNNER_DIR/analyze.py"
REPO="$(cd "$RUNNER_DIR/../.." && pwd)"
API_DIR="$REPO/services/ingest-api"
OUT="$HERE/results/$RESULTS_NAME"                    # runner.sh writes <timestamp>/ folders in here
CAP="$OUT/captures"; RUNS_TSV="$OUT/runs.tsv"; SUMMARY="$OUT/summary.tsv"; CEIL_TSV="$OUT/ceiling.tsv"
SUGGEST="$HERE/results/k1-coarse/suggestion.env"

if [ "$MODE" = "fine" ] && [ -z "${RATES:-}" ]; then
  [ -f "$SUGGEST" ] || { echo "ERROR: no RATES given and no $SUGGEST. Run './run.sh coarse' first (or pass RATES=...)." >&2; exit 1; }
  # shellcheck disable=SC1090
  . "$SUGGEST"                                             # sets RATES and PROBE_RATES
fi
PROBE_RATES="${PROBE_RATES:-}"
[ "$PROBES" = "1" ] && [ -z "$PROBE_RATES" ] && PROBES=0

die() { echo "ERROR: $*" >&2; exit 1; }
to_seconds() { case "$1" in *h) echo $(( ${1%h}*3600 ));; *m) echo $(( ${1%m}*60 ));; *s) echo "${1%s}";; *) echo "$1";; esac; }

# ---- 3. Plan / dry run ---------------------------------------------------------
if [ "$MODE" = ceiling ]; then
  n_prof=$(wc -w <<<"$PROFILES")
  total_s=$(( n_prof*REPEATS*(60+OVERHEAD_S) + 120 ))     # ~60 s per perf run is a guess (depends on the ceiling)
  echo "== $EXP_ID ceiling: $((n_prof*REPEATS)) perf-test runs (+1 warm-up) of $NUM_RECORDS records x ${RECORD_SIZE} B, results in $OUT =="
  echo "== profiles (name:batch.size): $PROFILES | linger.ms=$LINGER_MS acks=all idempotence=true | expected total about $(( total_s/60 )) min =="
else
  n_rates=$(wc -w <<<"$RATES")
  n_probe=0; [ "$PROBES" = "1" ] && n_probe=$(wc -w <<<"$PROBE_RATES")
  total_s=$(( n_rates*REPEATS*($(to_seconds "$DURATION")+OVERHEAD_S) + n_probe*($(to_seconds "$PROBE_DURATION")+OVERHEAD_S+20) ))
  echo "== $EXP_ID $MODE: $((n_rates*REPEATS)) measured runs + $n_probe probe runs, results in $OUT =="
  echo "== rates: $RATES | duration $DURATION | repeats $REPEATS | expected total about $(( total_s/60 )) min =="
fi
if [ "$DRY_RUN" = "1" ]; then
  if [ "$MODE" = ceiling ]; then
    echo "  warm-up (discarded): $WARM_RECORDS records"
    for rep in $(seq 1 "$REPEATS"); do for p in $PROFILES; do echo "  ceiling: repeat=$rep profile=${p%%:*} batch.size=${p##*:}"; done; done
  else
    for rep in $(seq 1 "$REPEATS"); do for r in $RATES; do echo "  measured: repeat=$rep rate=$r ($DURATION)"; done; done
    for r in $( [ "$PROBES" = 1 ] && echo "$PROBE_RATES" ); do echo "  probe:    rate=$r ($PROBE_DURATION)"; done
    echo "  API: KAFKA_BROKERS=$K_BROKERS KAFKA_KEY_MODE=$K_KEY_MODE (other knobs: API defaults unless exported)"
  fi
  if [ "$MODE" = ceiling ]; then T_DRY=k1-perf; else T_DRY="$TOPIC"; fi
  echo "  topic: $T_DRY deleted + recreated before every run ($PARTITIONS partitions, RF 3, min.insync.replicas=2)"
  echo "  CPU plan: server=$SERVER_CPUS brokers=$KAFKA_CPUS k6=$K6_CPUS monitoring+postgres=$MON_CPUS"
  exit 0
fi

# ---- 4. Helpers ------------------------------------------------------------------
# All Kafka CLI tools run INSIDE broker 1 against its INTERNAL listener (kafka1:19092 listens as localhost there).
KC1=""      # first broker container, set by detect_containers
kt()  { docker exec "$KC1" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 "$@"; }
# --time -1 = latest offset of every partition. Output is topic:partition:offset, so summing column 3 = records in the topic.
topic_sum() { docker exec "$KC1" /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:19092 --topic "$1" --time -1 | awk -F: '{s+=$3} END{print s+0}'; }

detect_containers() {
  if [ -z "$KAFKA_CONTAINERS" ]; then
    KAFKA_CONTAINERS=$(docker ps --format '{{.Names}}' | grep -E '^ep-kafka[0-9]+$' | sort | tr '\n' ' ' || true)
  fi
  [ "$(wc -w <<<"$KAFKA_CONTAINERS")" = 3 ] || die "expected 3 broker containers, found [$KAFKA_CONTAINERS]; set KAFKA_CONTAINERS or start docker-compose.kafka.yml"
  KC1=$(awk '{print $1}' <<<"$KAFKA_CONTAINERS")
  if [ -z "$PG_CONTAINER" ]; then PG_CONTAINER=$(docker ps --format '{{.Names}} {{.Image}}' | awk '$2 ~ /^postgres/ {print $1}' | head -1); fi
  if [ -z "$MON_CONTAINERS" ]; then
    MON_CONTAINERS=$(docker ps --format '{{.Names}} {{.Image}}' | awk '$2 ~ /prometheus|grafana|node-exporter|node_exporter/ {print $1}' | tr '\n' ' ')
  fi
}

preflight() {
  for c in k6 taskset curl docker python3 awk; do command -v "$c" >/dev/null || die "'$c' not found in PATH"; done
  command -v iostat >/dev/null || echo "WARN: iostat missing (apt install sysstat); probe runs will skip it" >&2
  [ -x "$RUNNER_DIR/runner.sh" ] || die "$RUNNER_DIR/runner.sh missing or not executable"
  [ -f "$ANALYZE" ] || die "$ANALYZE missing"
  curl -fsS --max-time 3 "$PROM_URL/-/ready" >/dev/null || die "Prometheus not ready at $PROM_URL (docker compose -f infra/compose/docker-compose.yml up -d)"
  curl -fsS --max-time 3 "$PROM_URL/api/v1/query?query=up%7Bjob%3D%22node%22%7D" | grep -q '"value":\[[^]]*"1"\]' \
    || echo "WARN: node_exporter (job=node) is not 'up'; host CPU panels will be empty" >&2
  detect_containers
  for c in $KAFKA_CONTAINERS; do
    [ "$(docker inspect -f '{{.State.Health.Status}}' "$c")" = healthy ] || die "$c is not healthy"
  done
  kt --list >/dev/null || die "kafka-topics.sh inside $KC1 failed"
  # The consumer-sink would eat CPU on the same machine and change the numbers: it must be off for K1.
  for p in 8001 8002 8003; do ss -ltn "sport = :$p" | grep -q LISTEN && die "something listens on :$p (consumer-sink?). Stop it: K1 measures ingest only"; done
  if [ "$MODE" != ceiling ]; then
    if [ "$MANAGE_SERVER" = 1 ]; then
      ss -ltn "sport = :$(sed -E 's#^[a-z]+://[^:/]+:([0-9]+).*#\1#' <<<"$BASE_URL")" | grep -q LISTEN && die "port of $BASE_URL is already in use: stop that server, or use MANAGE_SERVER=0"
      [ -f "$API_DIR/dist/main.js" ] || die "$API_DIR/dist/main.js missing: run 'pnpm build' in services/ingest-api"
      [ -f "$API_DIR/.env" ] || die "$API_DIR/.env missing"
    fi
    if [ -z "${API_KEY:-}" ]; then      # first key in API_KEYS=key:tenant,key2:tenant2
      API_KEY=$(grep -E '^API_KEYS=' "$API_DIR/.env" 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"' | cut -d, -f1 | cut -d: -f1 || true)
      [ -n "$API_KEY" ] || die "API_KEY not set and not found in $API_DIR/.env (API_KEYS=key:tenant)"
    fi
    export API_KEY
  fi
  echo "brokers: $KAFKA_CONTAINERS| postgres container (moved aside): ${PG_CONTAINER:-none} | monitoring: ${MON_CONTAINERS:-none found}"
}

# ---- 5. CPU pinning (docker update changes a running container; no compose edit needed) ----
declare -A OLD_CPUSET=()
pin_container() {   # pin_container <name> <cpus>
  local c="$1" cpus="$2"
  OLD_CPUSET["$c"]="$(docker inspect -f '{{.HostConfig.CpusetCpus}}' "$c")"
  docker update --cpuset-cpus "$cpus" "$c" >/dev/null && echo "pinned $c -> $cpus (was '${OLD_CPUSET[$c]}')"
}
restore_pinning() {
  [ "$KEEP_PINNING" = 1 ] && return 0
  for c in "${!OLD_CPUSET[@]}"; do
    local old="${OLD_CPUSET[$c]}"; [ -n "$old" ] || old="0-$(( $(nproc) - 1 ))"   # was unpinned -> all cpus again
    docker update --cpuset-cpus "$old" "$c" >/dev/null 2>&1 || true
  done
}

# Delete and recreate a topic so every run starts from an empty log: no disk growth from earlier runs, same segment state.
# Deletion is asynchronous in Kafka, so I wait until the name is gone before creating it again.
recreate_topic() {   # recreate_topic <name>
  local t="$1" i ok
  kt --delete --topic "$t" >/dev/null 2>&1 || true
  for i in $(seq 1 60); do kt --list 2>/dev/null | grep -qx "$t" || break; sleep 1; done
  for i in $(seq 1 30); do
    kt --create --topic "$t" --partitions "$PARTITIONS" --replication-factor 3 \
       --config min.insync.replicas=2 --config retention.ms="$RETENTION_MS" >/dev/null 2>&1 && break
    [ "$i" = 30 ] && die "could not create topic $t"; sleep 1
  done
  # wait until every partition has all 3 replicas in sync (Replicas: and Isr: lists have the same length)
  for i in $(seq 1 30); do
    ok=$(kt --describe --topic "$t" 2>/dev/null | awk '/Partition:/ { for(k=1;k<=NF;k++){ if($k=="Replicas:") r=$(k+1); if($k=="Isr:") s=$(k+1) } if (split(r,a,",")==split(s,b,",")) n++ } END{print n+0}')
    [ "$ok" = "$PARTITIONS" ] && return 0; sleep 1
  done
  die "topic $t: not all partitions have a full ISR after 30 s"
}

# ---- 6. Server lifecycle (ingest-api; no Stage 0 server-runner.sh here) ----
SERVER_PID=""
start_server() {
  if [ "$MANAGE_SERVER" != 1 ]; then return 0; fi
  stop_server
  # Only knobs I exported are passed on; everything else stays at the API's own default. Process env wins over .env.
  local extra=() k
  for k in "${API_KNOBS[@]}"; do [ -n "${!k:-}" ] && extra+=("$k=${!k}"); done
  ( cd "$API_DIR" && exec env KAFKA_BROKERS="$K_BROKERS" KAFKA_KEY_MODE="$K_KEY_MODE" "${extra[@]}" \
      taskset -c "$SERVER_CPUS" node --env-file=.env dist/main.js ) >"$OUT/server.out" 2>&1 &
  SERVER_PID=$!       # exec keeps the pid: this is the node pid
  for _ in $(seq 1 30); do curl -fsS --max-time 1 "$BASE_URL/healthz" >/dev/null 2>&1 && break; sleep 1; done
  for _ in $(seq 1 20); do curl -fsS --max-time 2 "$BASE_URL/readyz" >/dev/null 2>&1 && return 0; sleep 1; done   # readyz waits for the Kafka producer
  die "ingest-api not ready (see $OUT/server.out)"
}
stop_server() {
  [ "$MANAGE_SERVER" = 1 ] || return 0
  if [ -n "${SERVER_PID:-}" ] && kill -0 "$SERVER_PID" 2>/dev/null; then kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; fi
  SERVER_PID=""
}

cleanup() { stop_server; kt --delete --topic k1-perf >/dev/null 2>&1 || true; restore_pinning; }
trap cleanup EXIT

# ---- 7. Per-run helpers ---------------------------------------------------------
warmup() {          # 100 requests, 10 at a time: warms up V8 and the producer connections. They are subtracted from the audit.
  seq 1 100 | xargs -P 10 -I{} sh -c 'curl -s -o /dev/null -X POST "'"$BASE_URL"'/v1/events" \
    -H "Content-Type: application/json" -H "Authorization: Bearer $API_KEY" \
    -d "{\"event_id\":\"$(cat /proc/sys/kernel/random/uuid)\",\"type\":\"warmup\",\"occurred_at\":\"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\",\"schema_version\":1,\"payload\":{\"w\":1}}"'
}

CAP_FILE=""
tag() { printf '\n=== exp=%s testid=%s phase=%s ts=%s cmd: %s\n' "$EXP_ID" "$testid" "$1" "$(date -Is)" "$2" >>"$CAP_FILE"; }
# Cheap snapshots: one small query each, before and after the run (never during it). Counters: subtract pre from post.
light_snapshot() {
  tag "$1" "kafka-get-offsets (per partition) + /debug/stats"
  docker exec "$KC1" /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:19092 --topic "$TOPIC" --time -1 >>"$CAP_FILE" 2>&1 || true
  curl -fsS --max-time 2 "$BASE_URL/debug/stats" >>"$CAP_FILE" 2>&1 || true; echo >>"$CAP_FILE"
}
# Heavy tools: ONLY from probe runs. They cost CPU on the machine.
heavy_probe() {
  tag probe-mid "top -H -b -n 1 -p $SERVER_PID  (one line per thread of the node process)"
  [ -n "$SERVER_PID" ] && top -H -b -n 1 -w 200 -p "$SERVER_PID" >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "docker stats --no-stream (CPU % per broker container)"
  # shellcheck disable=SC2086
  docker stats --no-stream $KAFKA_CONTAINERS >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "kafka-topics --describe --under-replicated-partitions"
  kt --describe --under-replicated-partitions >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "iostat -x 1 5"; command -v iostat >/dev/null && iostat -x 1 5 >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "vmstat 1 5";   vmstat 1 5 >>"$CAP_FILE" 2>&1 || true
}

run_k6() {   # run_k6 <rate> <duration> <testid>
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$3" SCRIPT="stage-2/K1/script.js" \
      RATE="$1" DURATION="$2" PHASE="$PHASE" API_KEY="$API_KEY" BASE_URL="$BASE_URL" ROUTE="/healthz" ./runner.sh )
}
record() { printf '%s\t%s\t%s\t%s\t%s\n' "$testid" "$1" "$2" "$3" "$4" >>"$RUNS_TSV"; }

one_run() {  # one_run <phase> <rate> <duration> <rep>
  PHASE="$1"; local rate="$2" dur="$3" rep="$4"
  testid="k1-${PHASE:0:1}-r${rate}-n${rep}"
  [ "$PHASE" = probe ] && testid="k1-probe-r${rate}"
  echo; echo "== $PHASE: rate=$rate repeat=$rep ($testid) =="
  CAP_FILE="$CAP/$testid.txt"; : >"$CAP_FILE"
  stop_server; recreate_topic "$TOPIC"; start_server; warmup
  echo "   settling 10 s (topic creation and log flushes must finish before the window)"; sleep 10
  local before after; before=$(topic_sum "$TOPIC")
  light_snapshot pre; local t0; t0=$(date +%s); local status=ok
  if [ "$PHASE" = probe ]; then
    run_k6 "$rate" "$dur" "$testid" & local kpid=$!
    for _ in $(seq 1 60); do pgrep -x k6 >/dev/null && break; sleep 1; done
    sleep $(( $(to_seconds "$dur") / 3 )); heavy_probe
    wait "$kpid" || status=FAILED
  else
    run_k6 "$rate" "$dur" "$testid" || { status=FAILED; echo "!! $testid failed: see its manifest.md"; }
  fi
  sleep 3                                      # let requests that were still running finish before counting records
  after=$(topic_sum "$TOPIC"); local rows=$(( after - before ))
  light_snapshot post; record "$PHASE" "$t0" "$(date +%s)" "$status"
  local js; js=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  if [ -n "$js" ]; then
    python3 "$ANALYZE" row --json "$js" --testid "$testid" --phase "$PHASE" --rate "$rate" --rows "$rows" --prefix k1 --out "$SUMMARY"
    # columns of the shared analyzer: 4 achieved_rps, 10 err_pct, 13 p99_ms, 15 dropped, 17 audit, 18 verdict
    tail -1 "$SUMMARY" | awk -F'\t' '{printf "   -> achieved %s rps, err %s%%, p99 %s ms, dropped %s, audit: %s | %s\n",$4,$10,$13,$15,$17,$18}'
  else
    echo "   !! no summary json for $testid (k6 failed before finishing?)"
  fi
}

# ceiling_one <profile> <batch.size> <rep> <records>: one perf-test run in a throw-away container with its own cpuset (the
# k6 cores, which are idle in this mode), so it does not take CPU from the brokers. The last line of its output is the summary.
ceiling_one() {
  local profile="$1" batch="$2" rep="$3" n="$4" status=ok
  testid="k1-ceiling-${profile}-n${rep}"
  CAP_FILE="$CAP/$testid.txt"; : >"$CAP_FILE"
  echo; echo "== ceiling: profile=$profile batch.size=$batch repeat=$rep ($testid) =="
  recreate_topic k1-perf; echo "   settling 10 s"; sleep 10
  local t0; t0=$(date +%s)
  tag run "kafka-producer-perf-test.sh --num-records $n --record-size $RECORD_SIZE --throughput -1 acks=all linger.ms=$LINGER_MS batch.size=$batch"
  docker run --rm --network "$KNET" --cpuset-cpus "$K6_CPUS" "$KIMAGE" /opt/kafka/bin/kafka-producer-perf-test.sh \
    --topic k1-perf --num-records "$n" --record-size "$RECORD_SIZE" --throughput -1 \
    --producer-props bootstrap.servers="$K_INTERNAL" acks=all enable.idempotence=true linger.ms="$LINGER_MS" batch.size="$batch" \
    >>"$CAP_FILE" 2>&1 || status=FAILED
  record ceiling "$t0" "$(date +%s)" "$status"
  [ "$testid" = "k1-ceiling-warmup-n0" ] && return 0
  # last line looks like: N records sent, 98765.4 records/sec (47.09 MB/sec), 5.12 ms avg latency, 250.00 ms max latency, 3 ms 50th, 12 ms 95th, 40 ms 99th, 120 ms 99.9th.
  local line; line=$(grep 'records sent' "$CAP_FILE" | tail -1 || true)
  if [ -n "$line" ]; then
    sed -E 's/^([0-9]+) records sent, ([0-9.]+) records\/sec \(([0-9.]+) MB\/sec\), ([0-9.]+) ms avg latency, ([0-9.]+) ms max latency, ([0-9]+) ms 50th, ([0-9]+) ms 95th, ([0-9]+) ms 99th, ([0-9]+) ms 99.9th.*/'"$testid"'\t'"$profile"'\t'"$batch"'\t\1\t\2\t\3\t\4\t\6\t\7\t\8\t\9/' <<<"$line" >>"$CEIL_TSV"
    tail -1 "$CEIL_TSV" | awk -F'\t' '{printf "   -> %s records/s (%s MB/s), avg %s ms, p99 %s ms\n",$5,$6,$7,$10}'
  else
    echo "   !! no summary line for $testid (see $CAP_FILE)"
  fi
}

# ---- 8. Main ----------------------------------------------------------------------
mkdir -p "$CAP"
preflight
printf 'testid\tphase\tstart_epoch\tend_epoch\tstatus\n' >"$RUNS_TSV"

for c in $KAFKA_CONTAINERS; do pin_container "$c" "$KAFKA_CPUS"; done
[ -n "$PG_CONTAINER" ] && pin_container "$PG_CONTAINER" "$MON_CPUS"
for c in $MON_CONTAINERS; do pin_container "$c" "$MON_CPUS"; done

KIMAGE=$(docker inspect -f '{{.Config.Image}}' "$KC1")
KNET=$(docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{"\n"}}{{end}}' "$KC1" | head -1)
K_INTERNAL=$(for c in $KAFKA_CONTAINERS; do printf '%s:19092,' "${c#ep-}"; done); K_INTERNAL="${K_INTERNAL%,}"   # kafka1:19092,kafka2:19092,kafka3:19092

{   # environment snapshot
  echo "experiment: $EXP_ID ($MODE)   date: $(date -Is)"
  echo "git: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo n/a), $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l) dirty files"
  echo "kernel: $(uname -r)   node: $(node -v)   k6: $(k6 version | head -1)   cpus: $(nproc)"
  echo "pinning: server=$SERVER_CPUS brokers=$KAFKA_CPUS k6=$K6_CPUS monitoring+postgres=$MON_CPUS ($MON_CONTAINERS $PG_CONTAINER)"
  echo "settings: RATES=[$RATES] REPEATS=$REPEATS DURATION=$DURATION TOPIC=$TOPIC PARTITIONS=$PARTITIONS"
  echo "api: KAFKA_BROKERS=$K_BROKERS KAFKA_KEY_MODE=$K_KEY_MODE; exported knobs: $(for k in "${API_KNOBS[@]}"; do [ -n "${!k:-}" ] && printf '%s=%s ' "$k" "${!k}"; done)(others = API defaults)"
  echo "ceiling: RECORD_SIZE=$RECORD_SIZE NUM_RECORDS=$NUM_RECORDS LINGER_MS=$LINGER_MS PROFILES=[$PROFILES]"
  echo "--- brokers ---"
  echo "image: $KIMAGE   network: $KNET"
  for c in $KAFKA_CONTAINERS; do docker inspect -f '{{.Name}} nanocpus={{.HostConfig.NanoCpus}} memory={{.HostConfig.Memory}} cpuset={{.HostConfig.CpusetCpus}}' "$c"; done
  kt --describe --topic "$TOPIC" 2>/dev/null | head -2 || true
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
  # /var/lib/docker is root-only on the host, so I ask df from inside the broker (its data volume is /var/lib/kafka/data)
  echo "--- disk of the broker volume ---"; docker exec "$KC1" df -h /var/lib/kafka/data 2>/dev/null | tail -1 || true
} >"$OUT/env.txt"

if [ "$MODE" = ceiling ]; then
  printf 'testid\tprofile\tbatch_size\trecords\trecords_per_s\tmb_per_s\tavg_ms\tp50_ms\tp95_ms\tp99_ms\tp999_ms\n' >"$CEIL_TSV"
  ceiling_one warmup 262144 0 "$WARM_RECORDS"
  for rep in $(seq 1 "$REPEATS"); do          # repeat is the OUTER loop, profiles alternate inside it
    for p in $PROFILES; do ceiling_one "${p%%:*}" "${p##*:}" "$rep" "$NUM_RECORDS"; done
  done
  echo; column -t -s $'\t' "$CEIL_TSV"
  echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Raw output: $CAP  env: $OUT/env.txt"
  exit 0
fi

# measured runs: repeat is the OUTER loop, so every repeat sweeps all rates (drift hits every rate the same way)
for rep in $(seq 1 "$REPEATS"); do
  for rate in $RATES; do one_run measured "$rate" "$DURATION" "$rep"; done
done
# probe runs: short, separate
if [ "$PROBES" = 1 ]; then for rate in $PROBE_RATES; do one_run probe "$rate" "$PROBE_DURATION" 1; done; fi

echo; python3 "$ANALYZE" table "$SUMMARY"
if [ "$MODE" = coarse ]; then echo; python3 "$ANALYZE" suggest "$SUMMARY" "$SUGGEST"; echo "Next: ./run.sh fine   (it reads $SUGGEST)"; fi
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Summary: $SUMMARY  captures: $CAP  env: $OUT/env.txt"
