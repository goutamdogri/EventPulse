#!/usr/bin/env bash
# K6: Poison message
# ONE command runs everything:
#     ./run.sh            8 cells (DLQ off/on x poison kind invalid_json/bad_uuid/oversized/constraint) x 3 repeats,
#                         about 2.5 min each, about 1 hour in total
#
# What one run does:
#   1. fresh topics k6-events (6 partitions, RF 3, min ISR 2) and k6-dlq (1 partition); the real ingest-api is started
#      with KAFKA_KEY_MODE=event, so the load spreads over all 6 partitions (every partition is busy)
#   2. one consumer-sink is started with the cell's setting: SINK_DLQ_ENABLED=false (the old behaviour) or true (the fix)
#   3. k6 sends RATE req/s for DURATION through the ingest-api
#   4. POISON_AT_S seconds into the load, ONE bad record is written straight into k6-events (bypassing the API) with
#      scripts/produce-poison.sh. Its key always hashes to the same partition (POISON_PARTITION)
#   5. a sampler asks Kafka for the group's committed offset and lag of every partition about once a second
#   6. after the load, wait OBSERVE_S more seconds (the stuck partition stays stuck when the DLQ is off)
#   7. audit: rows of this run in Postgres vs 202 answers, lag per partition, records in k6-dlq and their headers
# Cells are <dlq>-<kind>:  dlq off|on  x  kind invalid_json | bad_uuid | oversized | constraint
#   invalid_json  value is not JSON (parser error, permanent)         bad_uuid  event_id is not a UUID (parser error)
#   oversized     type has 200 chars, API max is 100 (parser error)   constraint  parses fine, Postgres refuses it (jsonb and \u0000)
# Repeat is the OUTER loop; repeat 2 runs the cells in reverse order so drift hits all of them the same way.
#
# Usage:  DRY_RUN=1 ./run.sh                                                print the plan, run nothing
#         CELLS="on-invalid_json" REPEATS=1 RESULTS_NAME=k6-trial ./run.sh     small trial (never quote its numbers)
#
# Env knobs: RATE, DURATION_S, POISON_AT_S, OBSERVE_S, CELLS, REPEATS, RESULTS_NAME, PARTITIONS, POISON_KEY, POISON_PARTITION, DRY_RUN.
set -euo pipefail

EXP_ID="K6"
# ---- 1. Settings (override from the environment) ------------------------------
RATE="${RATE:-2000}"                            # req/s; 6 partitions = about 333 per partition. Same as K5
DURATION_S="${DURATION_S:-60}"
DURATION="${DURATION_S}s"
POISON_AT_S="${POISON_AT_S:-10}"                # the poison goes in this many seconds after the load started
OBSERVE_S="${OBSERVE_S:-30}"                    # how long I keep watching after the load stopped
CELLS="${CELLS:-off-invalid_json off-bad_uuid off-oversized off-constraint on-invalid_json on-bad_uuid on-oversized on-constraint}"
REPEATS="${REPEATS:-3}"
RESULTS_NAME="${RESULTS_NAME:-k6-poison-message}"
PARTITIONS="${PARTITIONS:-6}"
TOPIC="${TOPIC:-k6-events}"
DLQ_TOPIC="${DLQ_TOPIC:-k6-dlq}"
# A key is hashed (murmur2) to a partition. This one went to partition 5 of a 6-partition topic in the manual
# observation (notebook/stage-2/s2-f4-poison-before-fix.md). run.sh checks it again with the DLQ header of the "on" cells.
POISON_KEY="${POISON_KEY:-poison-observe-1}"
POISON_PARTITION="${POISON_PARTITION:-5}"
SINK_PORT="${SINK_PORT:-8001}"
DRAIN_TIMEOUT_S="${DRAIN_TIMEOUT_S:-120}"
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
PG_CONTAINER="${PG_CONTAINER:-ep-postgres}"; PG_USER="${PG_USER:-eventpulse}"; PG_DB="${PG_DB:-eventpulse}"
# Pinning: server 0,6 | consumer-sink 1,7 | brokers 2,8,3,9 | k6 4,10 | monitoring + Postgres 5,11 (same as K5).
SERVER_CPUS="${SERVER_CPUS:-0,6}"
CONSUMER_CPUS="${CONSUMER_CPUS:-1,7}"
KAFKA_CPUS="${KAFKA_CPUS:-2,8,3,9}"
export K6_CPUS="${K6_CPUS:-4,10}"
MON_CPUS="${MON_CPUS:-5,11}"
KEEP_PINNING="${KEEP_PINNING:-0}"
DRY_RUN="${DRY_RUN:-0}"
RETENTION_MS="${RETENTION_MS:-86400000}"
K_BROKERS="${KAFKA_BROKERS:-localhost:9092,localhost:9094,localhost:9096}"
BOOTSTRAP_INT="kafka1:19092,kafka2:19092,kafka3:19092"        # for the CLI tools inside the containers

# The API settings, written out so a run never depends on my shell or on services/ingest-api/.env. Same for every cell.
API_ENV="KAFKA_ACKS=-1 KAFKA_IDEMPOTENT=true KAFKA_COMPRESSION=none KAFKA_LINGER_MS=0 BATCH_MAX_MESSAGES=500 BATCH_MAX_WAIT_MS=5 BATCH_MAX_BYTES=900000 BATCH_MAX_IN_FLIGHT=1 KAFKA_KEY_MODE=event"
# The consumer-sink settings that are NOT the variable. The retry settings are left at the sink defaults on purpose
# (SINK_MAX_RETRIES=3, backoff 100 ms to 5 s): they are what the fix ships with.
SINK_ENV="CONSUMER_FROM_BEGINNING=true CONSUMER_ASSIGNOR=roundRobin CONSUMER_PARTITIONS_CONCURRENTLY=1 CONSUMER_MAX_BATCH_SIZE=1000 SINK_MAX_ROWS=1000 PG_POOL_MAX=5 LAG_POLL_MS=1000 LOG_ENABLED=true SINK_DLQ_TOPIC=$DLQ_TOPIC"
for k in KAFKA_ACKS KAFKA_IDEMPOTENT KAFKA_COMPRESSION KAFKA_LINGER_MS BATCH_MAX_MESSAGES BATCH_MAX_WAIT_MS BATCH_MAX_BYTES BATCH_MAX_IN_FLIGHT BATCH_MAX_QUEUE KAFKA_TOPIC KAFKA_KEY_MODE \
         CONSUMER_TOPIC CONSUMER_GROUP_ID CONSUMER_FROM_BEGINNING CONSUMER_ASSIGNOR CONSUMER_PARTITIONS_CONCURRENTLY CONSUMER_MAX_BATCH_SIZE \
         CONSUMER_SESSION_TIMEOUT_MS CONSUMER_HEARTBEAT_INTERVAL_MS SINK_MAX_ROWS SINK_PROCESSING_DELAY_MS SINK_DEDUP SINK_NODEDUP_TABLE \
         SINK_DLQ_ENABLED SINK_DLQ_TOPIC SINK_MAX_RETRIES SINK_RETRY_BASE_MS SINK_RETRY_MAX_MS SINK_TRANSIENT_RETRY_WINDOW_MS \
         CRASH_AT CRASH_AFTER_BATCHES DEBUG_FAULTS PG_POOL_MAX LAG_POLL_MS METRICS_PORT KAFKA_CLIENT_ID KAFKA_HEALTH_MIN_ISR; do unset "$k" || true; done

ALL_CELLS=(off-invalid_json off-bad_uuid off-oversized off-constraint on-invalid_json on-bad_uuid on-oversized on-constraint)
cell_no() { local i; for i in "${!ALL_CELLS[@]}"; do [ "${ALL_CELLS[$i]}" = "$1" ] && { echo $((i+1)); return; }; done; echo 0; }
for c in $CELLS; do [ "$(cell_no "$c")" != 0 ] || { echo "ERROR: unknown cell '$c'. Known: ${ALL_CELLS[*]}" >&2; exit 1; }; done
# the kind name of produce-poison.sh for each kind of the cell name
poison_kind() { case "$1" in invalid_json) echo invalid-json ;; bad_uuid) echo bad-uuid ;; oversized) echo oversized-field ;; constraint) echo constraint ;; esac; }
# Run id of one run: 8 hex chars = a6 + cell number + repeat + 00. Every event id of the run starts with it.
run_id() { printf 'a6%02x%02x00' "$1" "$2"; }       # run_id <cell number> <repeat>

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-2/K6
STAGE_DIR="$(dirname "$HERE")"
RUNNER_DIR="$(dirname "$STAGE_DIR")"
ANALYZE="$RUNNER_DIR/analyze.py"
REPO="$(cd "$RUNNER_DIR/../.." && pwd)"
API_DIR="$REPO/services/ingest-api"
SINK_DIR="$REPO/services/consumer-sink"
POISON_SH="$REPO/scripts/produce-poison.sh"
TEXTFILE_DIR="${TEXTFILE_DIR:-$REPO/infra/compose/textfile}"
OUT="$HERE/results/$RESULTS_NAME"
CAP="$OUT/captures"; RUNS_TSV="$OUT/runs.tsv"; SUMMARY="$OUT/summary.tsv"
PHASE_PROM="$TEXTFILE_DIR/ep_k6.prom"

die() { echo "ERROR: $*" >&2; exit 1; }

# ---- 3. Plan / dry run ---------------------------------------------------------
n_cell=$(wc -w <<<"$CELLS")
per_run_s=$(( DURATION_S + OBSERVE_S + 60 ))   # setup (topics, API, sink, settle) + load + observation + audit and cleanup (rough)
echo "== $EXP_ID: $n_cell cells x $REPEATS repeats = $((n_cell*REPEATS)) measured runs, $RATE req/s for ${DURATION_S}s (about $((RATE*DURATION_S)) events), results in $OUT =="
echo "== expected total about $(( n_cell*REPEATS*per_run_s/60 )) min =="
if [ "$DRY_RUN" = "1" ]; then
  echo "  per run: topics $TOPIC + $DLQ_TOPIC recreated -> API + one consumer-sink -> k6 load -> ONE poison record at t=${POISON_AT_S}s (key $POISON_KEY, expected partition $POISON_PARTITION)"
  echo "           -> lag sampler (about 1/s) -> ${OBSERVE_S}s of observation after the load -> audit (Postgres rows vs 202s, lag per partition, $DLQ_TOPIC records and headers)"
  for rep in $(seq 1 "$REPEATS"); do
    order="$CELLS"; [ $((rep % 2)) = 0 ] && order=$(tr ' ' '\n' <<<"$CELLS" | tac | tr '\n' ' ')
    echo "  repeat $rep order: $order"
    echo "     run ids: $(for v in $order; do printf '%s ' "$(run_id "$(cell_no "$v")" "$rep")"; done)"
  done
  echo "  cells: dlq=off -> SINK_DLQ_ENABLED=false (a poison record blocks its partition)"
  echo "         dlq=on  -> SINK_DLQ_ENABLED=true (classify, bounded retries with backoff, dead-letter, move on)"
  echo "  API: KAFKA_BROKERS=$K_BROKERS KAFKA_TOPIC=$TOPIC $API_ENV"
  echo "  sink: $SINK_ENV  (+ SINK_DLQ_ENABLED=<cell>)"
  echo "  CPU plan: server=$SERVER_CPUS consumer=$CONSUMER_CPUS brokers=$KAFKA_CPUS k6=$K6_CPUS monitoring+postgres=$MON_CPUS"
  echo "  changes the machine: docker update --cpuset-cpus (restored), topics $TOPIC and $DLQ_TOPIC recreated, one record written into $TOPIC per run,"
  echo "    rows of each run deleted from events, consumer groups k6-* deleted, textfile $PHASE_PROM (removed on exit)"
  exit 0
fi

# ---- 4. Helpers -----------------------------------------------------------------
KAFKA_CONTAINERS=""; MON_CONTAINERS=""
live_broker() { docker ps --format '{{.Names}}' | grep -E '^ep-kafka[0-9]+$' | sort | head -1; }
kt()  { timeout "${TMO:-20}" docker exec "$(live_broker)" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INT" "$@"; }
kg()  { timeout "${TMO:-20}" docker exec "$(live_broker)" /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_INT" "$@"; }
pgq() { docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -tAc "$1"; }
RANGE() { echo "event_id BETWEEN '$1-0000-0000-0000-000000000000' AND '$1-ffff-ffff-ffff-ffffffffffff'"; }
now() { date +%s.%N; }
to_n() { awk -v x="$1" 'BEGIN{printf "%d", x}'; }
alive() { [ -r "/proc/$1/stat" ] && ! awk '{exit ($3=="Z")?0:1}' "/proc/$1/stat"; }   # a killed child waiting to be reaped is not alive
# A counter of the sink without labels, from its own /metrics (0 if the sink does not answer)
smetric() { curl -fsS --max-time 0.5 "localhost:$SINK_PORT/metrics" 2>/dev/null | awk -v n="$1" '$1==n {s+=$2} END{printf "%d", s+0}'; }

detect_containers() {
  KAFKA_CONTAINERS=$(docker ps -a --format '{{.Names}}' | grep -E '^ep-kafka[0-9]+$' | sort | tr '\n' ' ')
  [ "$(wc -w <<<"$KAFKA_CONTAINERS")" = 3 ] || die "expected 3 broker containers, found [$KAFKA_CONTAINERS]"
  MON_CONTAINERS=$(docker ps --format '{{.Names}} {{.Image}}' | awk '$2 ~ /prometheus|grafana|node-exporter|node_exporter|kafka-exporter|postgres-exporter/ {print $1}' | tr '\n' ' ')
}

preflight() {
  for c in k6 taskset curl docker python3 awk node jq timeout; do command -v "$c" >/dev/null || die "'$c' not found in PATH"; done
  [ -x "$RUNNER_DIR/runner.sh" ] || die "$RUNNER_DIR/runner.sh missing or not executable"
  [ -f "$ANALYZE" ] || die "$ANALYZE missing"
  curl -fsS --max-time 3 "$PROM_URL/-/ready" >/dev/null || die "Prometheus not ready at $PROM_URL"
  detect_containers
  for c in $KAFKA_CONTAINERS; do [ "$(docker inspect -f '{{.State.Health.Status}}' "$c")" = healthy ] || die "$c is not healthy"; done
  kt --list >/dev/null || die "kafka-topics.sh inside a broker failed"
  pgq "select 1" >/dev/null || die "cannot run psql in container $PG_CONTAINER"
  local p
  for p in 8000 "$SINK_PORT"; do ss -ltn "sport = :$p" | grep -q LISTEN && die "something already listens on :$p (an old ingest-api or consumer-sink?). Stop it first"; done
  [ -f "$SINK_DIR/dist/main.js" ] || die "$SINK_DIR/dist/main.js missing: run 'pnpm build' in services/consumer-sink"
  [ -f "$SINK_DIR/.env" ] || die "$SINK_DIR/.env missing"
  [ -f "$API_DIR/dist/main.js" ] || die "$API_DIR/dist/main.js missing: run 'pnpm build' in services/ingest-api"
  [ -f "$API_DIR/.env" ] || die "$API_DIR/.env missing"
  [ -d "$TEXTFILE_DIR" ] || die "textfile directory $TEXTFILE_DIR not found"
  if [ -z "${API_KEY:-}" ]; then
    API_KEY=$(grep -E '^API_KEYS=' "$API_DIR/.env" 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"' | cut -d, -f1 | cut -d: -f1 || true)
    [ -n "$API_KEY" ] || die "API_KEY not set and not found in $API_DIR/.env (API_KEYS=key:tenant)"
  fi
  export API_KEY
}

declare -A OLD_CPUSET=()
pin_container() {
  local c="$1" cpus="$2"
  OLD_CPUSET["$c"]="$(docker inspect -f '{{.HostConfig.CpusetCpus}}' "$c")"
  docker update --cpuset-cpus "$cpus" "$c" >/dev/null && echo "pinned $c -> $cpus (was '${OLD_CPUSET[$c]}')"
}
restore_pinning() {
  [ "$KEEP_PINNING" = 1 ] && return 0
  for c in "${!OLD_CPUSET[@]}"; do
    local old="${OLD_CPUSET[$c]}"; [ -n "$old" ] || old="0-$(( $(nproc) - 1 ))"
    docker update --cpuset-cpus "$old" "$c" >/dev/null 2>&1 || true
  done
}

full_isr_count() {
  kt --describe --topic "$1" 2>/dev/null | awk '/Partition:/ { r=""; s=""; l=-1; for(k=1;k<=NF;k++){ if($k=="Replicas:") r=$(k+1); if($k=="Isr:") s=$(k+1); if($k=="Leader:") l=$(k+1) } if (l>=0 && split(r,a,",")==split(s,b,",")) n++ } END{print n+0}'
}
wait_full_isr() { local t="$1" lim="$2" i; for i in $(seq 1 "$lim"); do [ "$(full_isr_count "$t")" = "$PARTITIONS" ] && return 0; sleep 1; done; return 1; }
recreate_topic() {
  local t="$1" i
  kt --delete --topic "$t" >/dev/null 2>&1 || true
  for i in $(seq 1 60); do kt --list 2>/dev/null | grep -qx "$t" || break; sleep 1; done
  for i in $(seq 1 30); do
    kt --create --topic "$t" --partitions "$PARTITIONS" --replication-factor 3 \
       --config min.insync.replicas=2 --config retention.ms="$RETENTION_MS" >/dev/null 2>&1 && break
    [ "$i" = 30 ] && die "could not create topic $t"; sleep 1
  done
  wait_full_isr "$t" 30 || die "topic $t: not all partitions have a full ISR after 30 s"
}
topic_total() {   # sum of the end offsets of every partition = records in the topic (retention is 1 day, nothing is deleted)
  timeout 20 docker exec "$(live_broker)" /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server "$BOOTSTRAP_INT" --topic "$1" --time -1 2>/dev/null \
    | awk -F: '{s+=$3} END{printf "%d", s+0}'
}

SERVER_PID=""
start_server() {
  stop_server
  # shellcheck disable=SC2086
  ( cd "$API_DIR" && exec env KAFKA_BROKERS="$K_BROKERS" KAFKA_TOPIC="$TOPIC" $API_ENV \
      taskset -c "$SERVER_CPUS" node --env-file=.env dist/main.js ) >"$CAP/$testid.server.out" 2>&1 &
  SERVER_PID=$!
  local _
  for _ in $(seq 1 30); do curl -fsS --max-time 1 "$BASE_URL/healthz" >/dev/null 2>&1 && break; sleep 1; done
  for _ in $(seq 1 20); do curl -fsS --max-time 2 "$BASE_URL/readyz" >/dev/null 2>&1 && return 0; sleep 1; done
  die "ingest-api not ready (see $CAP/$testid.server.out)"
}
stop_server() {
  if [ -n "${SERVER_PID:-}" ] && kill -0 "$SERVER_PID" 2>/dev/null; then kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; fi
  SERVER_PID=""
}

# The consumer-sink of one cell. dlq is on|off.
SINK_PID=""
start_sink() {   # start_sink <group> <dlq on|off>
  local group="$1" dlq="$2" flag=true
  [ "$dlq" = off ] && flag=false
  # shellcheck disable=SC2086
  ( cd "$SINK_DIR" && exec env KAFKA_BROKERS="$K_BROKERS" CONSUMER_TOPIC="$TOPIC" CONSUMER_GROUP_ID="$group" \
      METRICS_PORT="$SINK_PORT" KAFKA_CLIENT_ID="k6-sink" $SINK_ENV SINK_DLQ_ENABLED="$flag" \
      taskset -c "$CONSUMER_CPUS" node --env-file=.env dist/main.js ) >"$CAP/$testid.sink.out" 2>&1 &
  SINK_PID=$!
}
stop_sink() {
  if [ -n "${SINK_PID:-}" ] && kill -0 "$SINK_PID" 2>/dev/null; then kill "$SINK_PID" 2>/dev/null || true; wait "$SINK_PID" 2>/dev/null || true; fi
  SINK_PID=""
}

# One gauge for Grafana: 0 load running, no poison yet, 1 poison written, 2 load finished (observation), -1 not in a run.
set_phase() {
  { echo "# HELP ep_k6_phase K6 run phase: 0 load running, 1 poison written, 2 observation after the load, -1 no run."
    echo "# TYPE ep_k6_phase gauge"
    echo "ep_k6_phase{testid=\"${testid:-none}\",cell=\"${cell:-none}\"} $1"; } >"$PHASE_PROM.tmp" && mv "$PHASE_PROM.tmp" "$PHASE_PROM"
}
SAMPLER_PID=""
stop_sampler() { [ -n "${SAMPLER_PID:-}" ] && { kill "$SAMPLER_PID" 2>/dev/null || true; wait "$SAMPLER_PID" 2>/dev/null || true; }; SAMPLER_PID=""; }
cleanup() { stop_sampler; stop_sink; stop_server; rm -f "$PHASE_PROM" "$PHASE_PROM.tmp"; restore_pinning; }
trap cleanup EXIT

acked_total() { curl -fsS --max-time 2 "$BASE_URL/metrics" 2>/dev/null | awk '/^ep_kafka_messages_acked_total/ {s+=$NF} END{printf "%d", s+0}'; }
record() { printf '%s\t%s\t%s\t%s\t%s\n' "$testid" "$1" "$2" "$3" "$4" >>"$RUNS_TSV"; }

run_k6() {   # run_k6 <testid> <run id> <cell>   (extra env names are forwarded to k6 by runner.sh)
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$1" SCRIPT="stage-2/K6/script.js" \
      RUN_ID="$2" CELL="$3" RATE="$RATE" DURATION="$DURATION" PHASE=measured \
      API_KEY="$API_KEY" BASE_URL="$BASE_URL" ROUTE="/healthz" ./runner.sh )
}

# The group's lag per partition, about once a second, into a TSV: epoch partition committed end lag.
# I use the Kafka tool (not Prometheus) so the numbers are the broker's own and I can post-process them here.
sample_lag() {   # sample_lag <group> <file>
  while true; do
    local t; t=$(date +%s)
    kg --describe --group "$1" 2>/dev/null | awk -v t="$t" -v tp="$TOPIC" '$2==tp && $3 ~ /^[0-9]+$/ && $5 ~ /^[0-9]+$/ {print t "\t" $3 "\t" $4 "\t" $5 "\t" $6}' >>"$2" || true
    sleep 1
  done
}

# Wait until the group's total lag is 0 (every record of the topic is handled). Returns 1 on timeout.
drain_topic() {   # drain_topic <group> <limit s>
  local g="$1" lim="$2" i lag parts
  for i in $(seq 1 "$lim"); do
    sleep 1
    kg --describe --group "$g" 2>/dev/null | awk -v t="$TOPIC" '$2==t && $6 ~ /^[0-9]+$/ {l+=$6; p++} END{print l+0, p+0}' >"$CAP/.lag.$$" || true
    read -r lag parts <"$CAP/.lag.$$" || { lag=-1; parts=0; }
    [ "${parts:-0}" -ge "$PARTITIONS" ] && [ "${lag:-1}" = 0 ] && { rm -f "$CAP/.lag.$$"; return 0; }
  done
  rm -f "$CAP/.lag.$$"; return 1
}

one_run() {  # one_run <cell> <rep>
  cell="$1"; local rep="$2" cno dlq kind
  dlq="${cell%%-*}"; kind="${cell#*-}"; cno=$(cell_no "$cell")
  testid="k6-${cell//_/-}-n${rep}"
  local rid; rid=$(run_id "$cno" "$rep")
  echo; echo "== cell=$cell (dlq=$dlq poison=$kind) repeat=$rep ($testid) run id $rid =="
  set_phase -1
  stop_server; stop_sink
  for c in $KAFKA_CONTAINERS; do for _ in $(seq 1 60); do [ "$(docker inspect -f '{{.State.Health.Status}}' "$c")" = healthy ] && break; sleep 1; done; done
  recreate_topic "$TOPIC"
  # the DLQ topic: one partition is enough, it only receives the poison records
  kt --delete --topic "$DLQ_TOPIC" >/dev/null 2>&1 || true
  for _ in $(seq 1 60); do kt --list 2>/dev/null | grep -qx "$DLQ_TOPIC" || break; sleep 1; done
  kt --create --topic "$DLQ_TOPIC" --partitions 1 --replication-factor 3 --config min.insync.replicas=2 --config retention.ms="$RETENTION_MS" >/dev/null 2>&1 || die "could not create topic $DLQ_TOPIC"
  pgq "DELETE FROM events WHERE $(RANGE "$rid")" >/dev/null
  start_server
  echo "   settling 5 s"; sleep 5
  local group="k6-$testid-$$" status=ok i
  local base_acked; base_acked=$(acked_total)

  # --- the consumer of this cell ---
  start_sink "$group" "$dlq"
  local pid=$SINK_PID
  for i in $(seq 1 300); do   # joined the group when it was assigned a partition (0.1 s polls, 30 s at most)
    [ "$(smetric ep_consumer_rebalances_total)" -ge 1 ] && break
    alive "$pid" || die "the consumer died before the load started (see $CAP/$testid.sink.out)"; sleep 0.1
  done
  [ "$i" = 300 ] && die "the consumer was not assigned a partition in 30 s (see $CAP/$testid.sink.out)"

  # --- the load, in the background ---
  run_k6 "$testid" "$rid" "$cell" &
  local k6pid=$!
  for i in $(seq 1 600); do   # k6 has started when the API's acked counter moves
    [ "$(( $(acked_total) - base_acked ))" -ge 20 ] && break
    kill -0 "$k6pid" 2>/dev/null || break; sleep 0.1
  done
  local t0 t_poison="" LAGF="$CAP/$testid.lag.tsv"
  t0=$(now); set_phase 0; : >"$LAGF"
  sample_lag "$group" "$LAGF" & SAMPLER_PID=$!
  echo "   load started (t=0), $RATE req/s for ${DURATION_S}s; consumer pid $pid, DLQ $dlq"

  # --- the poison ---
  sleep "$POISON_AT_S"
  "$POISON_SH" --kind "$(poison_kind "$kind")" --key "$POISON_KEY" --topic "$TOPIC" >"$CAP/$testid.poison.out" 2>&1 || { status=POISON_FAILED; echo "!! could not write the poison record (see $CAP/$testid.poison.out)"; }
  t_poison=$(now); set_phase 1; record poison "$(to_n "$t_poison")" "$(to_n "$t_poison")" "$kind"
  echo "   t=$(awk -v n="$t_poison" -v t="$t0" 'BEGIN{printf "%.1f", n-t}')s: poison record ($kind) written, key $POISON_KEY"

  wait "$k6pid" || { status=FAILED; echo "!! $testid: k6 failed, see its manifest.md"; }
  local t_load_end; t_load_end=$(now); set_phase 2
  echo "   load done, observing ${OBSERVE_S}s more"
  sleep "$OBSERVE_S"
  record measured "$(to_n "$t0")" "$(date +%s)" "$status"
  stop_sampler

  # --- audit ---
  # Counters of the consumer, read while it is still running.
  local m_consumed m_inserted m_dlq m_retries m_blocked m_errors
  m_consumed=$(smetric ep_consumer_messages_consumed_total); m_inserted=$(smetric ep_consumer_rows_inserted_total)
  m_dlq=$(smetric ep_consumer_dlq_total); m_retries=$(smetric ep_consumer_retries_total)
  m_blocked=$(curl -fsS --max-time 1 "localhost:$SINK_PORT/metrics" 2>/dev/null | awk '/^ep_consumer_chunk_failures_total\{reason="poison_blocked"\}/ {s+=$2} END{printf "%d", s+0}')
  m_errors=$(smetric ep_consumer_errors_total)
  # Lag and committed offsets per partition at the end (last sample), and how long the poison partition stood still.
  local last_t stuck_lag other_lag stall_s
  last_t=$(tail -n 1 "$LAGF" 2>/dev/null | cut -f1 || true)
  stuck_lag=$(awk -F'\t' -v t="$last_t" -v p="$POISON_PARTITION" '$1==t && $2==p {print $5}' "$LAGF" | tail -1)
  other_lag=$(awk -F'\t' -v t="$last_t" -v p="$POISON_PARTITION" '$1==t && $2!=p && $5>m {m=$5} END{print m+0}' "$LAGF")
  # stall_s: the longest time after the poison during which the committed offset of the poison partition did not
  # move while it had lag. Resolution is the sampling step (a bit over 1 s).
  stall_s=$(awk -F'\t' -v p="$POISON_PARTITION" -v t0="$(to_n "$t_poison")" '
    $2==p && $1>=t0 { if ($3==prev && $5>0) { if (start=="") start=$1; cur=$1-start } else { start=""; cur=0 } if (cur>best) best=cur; prev=$3 }
    END{printf "%d", best+0}' "$LAGF")
  stuck_lag="${stuck_lag:-n/a}"
  # For the on cells let the partition finish draining before the audit (it should be 0 already).
  local drained=yes; drain_topic "$group" 5 || drained=no
  local total uniq tt dlqn dlq_hdr="none" dlq_part="n/a"
  total=$(pgq "SELECT count(*) FROM events WHERE $(RANGE "$rid")" | tr -d ' \r')
  uniq=$(pgq "SELECT count(DISTINCT event_id) FROM events WHERE $(RANGE "$rid")" | tr -d ' \r')
  tt=$(topic_total "$TOPIC"); dlqn=$(topic_total "$DLQ_TOPIC")
  if [ "$dlqn" -ge 1 ]; then
    # headers of the dead letter: dlq.error_class, dlq.error_code, dlq.attempts, dlq.original_partition
    timeout 20 docker exec "$(live_broker)" /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP_INT" --topic "$DLQ_TOPIC" \
      --from-beginning --max-messages 1 --timeout-ms 8000 --formatter-property print.headers=true --formatter-property print.key=true \
      >"$CAP/$testid.dlq.txt" 2>/dev/null || true
    dlq_hdr=$(grep -o 'dlq.error_class:[A-Z_]*' "$CAP/$testid.dlq.txt" | head -1 | cut -d: -f2)/$(grep -o 'dlq.error_code:[a-z_0-9A-Z]*' "$CAP/$testid.dlq.txt" | head -1 | cut -d: -f2)/$(grep -o 'dlq.attempts:[0-9]*' "$CAP/$testid.dlq.txt" | head -1 | cut -d: -f2)
    dlq_part=$(grep -o 'dlq.original_partition:[0-9]*' "$CAP/$testid.dlq.txt" | head -1 | cut -d: -f2 || true); dlq_part="${dlq_part:-n/a}"
    [ "$dlq_part" != n/a ] && [ "$dlq_part" != "$POISON_PARTITION" ] && { status=WRONG_PARTITION; echo "!! the DLQ record came from partition $dlq_part, not $POISON_PARTITION: the stuck-partition numbers are not about the poison"; }
  fi
  echo "   audit: rows $uniq (stored $total), topic records $tt, DLQ records $dlqn [$dlq_hdr], poison partition lag $stuck_lag, other partitions max lag $other_lag, stall ${stall_s}s"
  echo "   consumer: consumed $m_consumed inserted $m_inserted dlq $m_dlq retries $m_retries poison_blocked attempts $m_blocked errors $m_errors"
  pgq "DELETE FROM events WHERE $(RANGE "$rid")" >/dev/null
  stop_sink
  kg --delete --group "$group" >/dev/null 2>&1 || true
  stop_server; set_phase -1

  local js; js=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  if [ -n "$js" ]; then
    local ok202 missing
    ok202=$(jq -r '.metrics.resp_202 | (.count // .values.count // 0)' "$js")
    missing=$(( ok202 - uniq ))
    python3 "$ANALYZE" row --json "$js" --testid "$testid" --phase measured --rate "$RATE" --rows "$uniq" \
      --ok-code 202 --fail-code 503 --p99-limit-ms 1000000 --err-limit-pct 100 \
      --var "dlq=$dlq" --var "poison=$kind" \
      --extra "acked=$ok202" --extra "missing=$missing" --extra "stuck_lag=$stuck_lag" --extra "other_lag_max=$other_lag" \
      --extra "stall_s=$stall_s" --extra "dlq_records=$dlqn" --extra "dlq_hdr=$dlq_hdr" --extra "retries=$m_retries" \
      --extra "blocked_attempts=$m_blocked" --extra "drained=$drained" --extra "status=$status" --out "$SUMMARY"
    echo "   -> acked $ok202, rows $uniq (missing $missing), poison partition lag $stuck_lag, DLQ records $dlqn, stall ${stall_s}s"
  else
    echo "   !! no summary row for $testid (k6 failed)"
  fi
}

# ---- 6. Main ----------------------------------------------------------------------
mkdir -p "$CAP"
preflight
[ -x "$POISON_SH" ] || die "$POISON_SH missing or not executable"
grep -q "SINK_DLQ_ENABLED" "$SINK_DIR/dist/config.js" || die "consumer-sink dist has no SINK_DLQ_ENABLED: run 'pnpm --filter @eventpulse/consumer-sink build'"
printf 'testid\tphase\tstart_epoch\tend_epoch\tstatus\n' >"$RUNS_TSV"
for c in $KAFKA_CONTAINERS; do pin_container "$c" "$KAFKA_CPUS"; done
pin_container "$PG_CONTAINER" "$MON_CPUS"
for c in $MON_CONTAINERS; do pin_container "$c" "$MON_CPUS"; done

{
  echo "experiment: $EXP_ID   date: $(date -Is)"
  echo "git: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo n/a), $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l) dirty files"
  echo "kernel: $(uname -r)   node: $(node -v)   k6: $(k6 version | head -1)   cpus: $(nproc)"
  echo "pinning: server=$SERVER_CPUS consumer=$CONSUMER_CPUS brokers=$KAFKA_CPUS k6=$K6_CPUS monitoring+postgres=$MON_CPUS ($MON_CONTAINERS $PG_CONTAINER)"
  echo "settings: RATE=$RATE DURATION_S=$DURATION_S POISON_AT_S=$POISON_AT_S OBSERVE_S=$OBSERVE_S REPEATS=$REPEATS CELLS=[$CELLS] TOPIC=$TOPIC DLQ_TOPIC=$DLQ_TOPIC PARTITIONS=$PARTITIONS POISON_KEY=$POISON_KEY POISON_PARTITION=$POISON_PARTITION"
  echo "api: KAFKA_BROKERS=$K_BROKERS $API_ENV"
  echo "sink: $SINK_ENV  (+ SINK_DLQ_ENABLED=<cell>; retry settings = sink defaults: SINK_MAX_RETRIES=3, SINK_RETRY_BASE_MS=100, SINK_RETRY_MAX_MS=5000)"
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
} >"$OUT/env.txt"

for rep in $(seq 1 "$REPEATS"); do
  order="$CELLS"; [ $((rep % 2)) = 0 ] && order=$(tr ' ' '\n' <<<"$CELLS" | tac | tr '\n' ' ')
  for v in $order; do one_run "$v" "$rep"; done
done

echo; python3 "$ANALYZE" table "$SUMMARY"
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Summary: $SUMMARY  captures: $CAP  env: $OUT/env.txt"
