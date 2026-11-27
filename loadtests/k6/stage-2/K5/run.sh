#!/usr/bin/env bash
# K5: Kill a consumer at every step
# ONE command runs everything:
#     ./run.sh            8 cells (dedup on/off x crash point none/before_parse/after_parse/after_db_commit) x 3 repeats,
#                         about 2.5 min each, about 1 hour in total
#
# What one run does:
#   1. fresh topic k5-events (6 partitions, RF 3, min ISR 2); the real ingest-api (acks=all, idempotent) is started
#   2. consumer-sink #1 is started with a crash point armed (CRASH_AT, on its CRASH_BATCH-th batch). A new group id
#   3. k6 sends RATE req/s for DURATION through the ingest-api (about RATE x DURATION events, all with this run's id prefix)
#   4. the sink kills itself with SIGKILL at the crash point (no cleanup, no offset commit). The script notices, and starts
#      consumer-sink #2 with the SAME group id and no crash point. It measures the time until #2 processes its first chunk
#   5. when k6 is done, wait until the group's lag is 0 (everything read and committed)
#   6. audit in Postgres: rows of this run (count(*)), distinct event ids, compare with the number of 202 answers
# The cell is made of two things (both are consumer-sink settings, the API and the load are identical):
#   dedup  on   the normal sink: INSERT ... ON CONFLICT (event_id) DO NOTHING into events (event_id is the primary key)
#   dedup  off  SINK_DEDUP=false: plain INSERT into events_nodedup, a copy of events with no primary key (debug only)
#   crash  none | before_parse | after_parse | after_db_commit   (the control run is "none": nobody is killed)
# Repeat is the OUTER loop; repeat 2 runs the cells in reverse order so drift hits all of them the same way.
#
# Usage:  DRY_RUN=1 ./run.sh                                             print the plan, run nothing
#         CELLS="on-after_db_commit" REPEATS=1 RESULTS_NAME=k5-trial ./run.sh     small trial (never quote its numbers)
#
# Env knobs: RATE, DURATION_S, CRASH_BATCH (the sink kills itself on its Nth batch), CELLS, REPEATS, RESULTS_NAME, SESSION_TIMEOUT_MS, PARTITIONS, DRY_RUN.
set -euo pipefail

EXP_ID="K5"
# ---- 1. Settings (override from the environment) ------------------------------
RATE="${RATE:-2000}"                            # req/s; about 60,000 events in 30 s. Far below the K1 knee (about 7,500)
DURATION_S="${DURATION_S:-30}"
DURATION="${DURATION_S}s"
CRASH_BATCH="${CRASH_BATCH:-5}" # the sink kills itself while handling its 5th batch (about 1 s into the load)
CELLS="${CELLS:-on-none on-before_parse on-after_parse on-after_db_commit off-none off-before_parse off-after_parse off-after_db_commit}"
REPEATS="${REPEATS:-3}"
RESULTS_NAME="${RESULTS_NAME:-k5-kill-consumer}"
PARTITIONS="${PARTITIONS:-6}"
TOPIC="${TOPIC:-k5-events}"
# Consumer group timing. The default session timeout (45 s) would make every run wait 45 s for the dead member to be
# dropped; 10 s keeps the experiment short. It is also what the time-to-resume prediction is built on.
SESSION_TIMEOUT_MS="${SESSION_TIMEOUT_MS:-10000}"
HEARTBEAT_MS="${HEARTBEAT_MS:-1000}"
SINK_PORT="${SINK_PORT:-8001}"
RESUME_TIMEOUT_S="${RESUME_TIMEOUT_S:-120}"
CRASH_TIMEOUT_S="${CRASH_TIMEOUT_S:-60}"
DRAIN_TIMEOUT_S="${DRAIN_TIMEOUT_S:-300}"
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
PG_CONTAINER="${PG_CONTAINER:-ep-postgres}"; PG_USER="${PG_USER:-eventpulse}"; PG_DB="${PG_DB:-eventpulse}"
# Pinning: server 0,6 | consumer-sink 1,7 | brokers 2,8,3,9 | k6 4,10 | monitoring + Postgres 5,11.
# The container cpusets are moved with docker update (and put back on exit).
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
NODEDUP_TABLE="events_nodedup"

# The API settings, written out so a run never depends on my shell or on services/ingest-api/.env. Same for every cell.
API_ENV="KAFKA_ACKS=-1 KAFKA_IDEMPOTENT=true KAFKA_COMPRESSION=none KAFKA_LINGER_MS=0 BATCH_MAX_MESSAGES=500 BATCH_MAX_WAIT_MS=5 BATCH_MAX_BYTES=900000 BATCH_MAX_IN_FLIGHT=1 KAFKA_KEY_MODE=event"
# The consumer-sink settings that are NOT the variable. One consumer, one partition at a time, chunks of up to 1000 rows.
SINK_ENV="CONSUMER_FROM_BEGINNING=true CONSUMER_ASSIGNOR=roundRobin CONSUMER_PARTITIONS_CONCURRENTLY=1 CONSUMER_MAX_BATCH_SIZE=1000 SINK_MAX_ROWS=1000 PG_POOL_MAX=5 LAG_POLL_MS=1000 LOG_ENABLED=true CONSUMER_SESSION_TIMEOUT_MS=$SESSION_TIMEOUT_MS CONSUMER_HEARTBEAT_INTERVAL_MS=$HEARTBEAT_MS"
for k in KAFKA_ACKS KAFKA_IDEMPOTENT KAFKA_COMPRESSION KAFKA_LINGER_MS BATCH_MAX_MESSAGES BATCH_MAX_WAIT_MS BATCH_MAX_BYTES BATCH_MAX_IN_FLIGHT BATCH_MAX_QUEUE KAFKA_TOPIC KAFKA_KEY_MODE \
         CONSUMER_TOPIC CONSUMER_GROUP_ID CONSUMER_FROM_BEGINNING CONSUMER_ASSIGNOR CONSUMER_PARTITIONS_CONCURRENTLY CONSUMER_MAX_BATCH_SIZE \
         CONSUMER_SESSION_TIMEOUT_MS CONSUMER_HEARTBEAT_INTERVAL_MS SINK_MAX_ROWS SINK_PROCESSING_DELAY_MS SINK_DEDUP SINK_NODEDUP_TABLE \
         CRASH_AT CRASH_AFTER_BATCHES DEBUG_FAULTS PG_POOL_MAX LAG_POLL_MS METRICS_PORT KAFKA_CLIENT_ID KAFKA_HEALTH_MIN_ISR; do unset "$k" || true; done

ALL_CELLS=(on-none on-before_parse on-after_parse on-after_db_commit off-none off-before_parse off-after_parse off-after_db_commit)
cell_no() { local i; for i in "${!ALL_CELLS[@]}"; do [ "${ALL_CELLS[$i]}" = "$1" ] && { echo $((i+1)); return; }; done; echo 0; }
for c in $CELLS; do [ "$(cell_no "$c")" != 0 ] || { echo "ERROR: unknown cell '$c'. Known: ${ALL_CELLS[*]}" >&2; exit 1; }; done
# Run id of one run: 8 hex chars = a5 + cell number + repeat + 00. Every event id of the run starts with it.
run_id() { printf 'a5%02x%02x00' "$1" "$2"; }       # run_id <cell number> <repeat>

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-2/K5
STAGE_DIR="$(dirname "$HERE")"
RUNNER_DIR="$(dirname "$STAGE_DIR")"
ANALYZE="$RUNNER_DIR/analyze.py"
REPO="$(cd "$RUNNER_DIR/../.." && pwd)"
API_DIR="$REPO/services/ingest-api"
SINK_DIR="$REPO/services/consumer-sink"
TEXTFILE_DIR="${TEXTFILE_DIR:-$REPO/infra/compose/textfile}"
OUT="$HERE/results/$RESULTS_NAME"
CAP="$OUT/captures"; RUNS_TSV="$OUT/runs.tsv"; SUMMARY="$OUT/summary.tsv"
PHASE_PROM="$TEXTFILE_DIR/ep_k5.prom"

die() { echo "ERROR: $*" >&2; exit 1; }

# ---- 3. Plan / dry run ---------------------------------------------------------
n_cell=$(wc -w <<<"$CELLS")
per_run_s=$(( DURATION_S + 100 ))   # setup + load + outage until the dead member is dropped + drain + audit (rough)
echo "== $EXP_ID: $n_cell cells x $REPEATS repeats = $((n_cell*REPEATS)) measured runs, $RATE req/s for ${DURATION_S}s (about $((RATE*DURATION_S)) events), results in $OUT =="
echo "== expected total about $(( n_cell*REPEATS*per_run_s/60 )) min =="
if [ "$DRY_RUN" = "1" ]; then
  echo "  per run: sink #1 armed (CRASH_AT, batch #$CRASH_BATCH) -> k6 load -> SIGKILL -> sink #2 started at once (same group) -> drain to lag 0 -> audit in Postgres"
  echo "  group timing: session timeout ${SESSION_TIMEOUT_MS} ms, heartbeat ${HEARTBEAT_MS} ms (the dead member is dropped after the session timeout)"
  for rep in $(seq 1 "$REPEATS"); do
    order="$CELLS"; [ $((rep % 2)) = 0 ] && order=$(tr ' ' '\n' <<<"$CELLS" | tac | tr '\n' ' ')
    echo "  repeat $rep order: $order"
    echo "     run ids: $(for v in $order; do printf '%s ' "$(run_id "$(cell_no "$v")" "$rep")"; done)"
  done
  echo "  cells: dedup=on  -> SINK_DEDUP default, table events (primary key event_id, ON CONFLICT DO NOTHING)"
  echo "         dedup=off -> SINK_DEDUP=false DEBUG_FAULTS=true, plain INSERT into $NODEDUP_TABLE (no primary key)"
  echo "  API: KAFKA_BROKERS=$K_BROKERS KAFKA_TOPIC=$TOPIC $API_ENV"
  echo "  sink: $SINK_ENV"
  echo "  topic: $TOPIC deleted + recreated before every run ($PARTITIONS partitions, RF 3, min.insync.replicas=2)"
  echo "  CPU plan: server=$SERVER_CPUS consumer=$CONSUMER_CPUS brokers=$KAFKA_CPUS k6=$K6_CPUS monitoring+postgres=$MON_CPUS"
  echo "  changes the machine: SIGKILL of the consumer-sink the script started (by design), docker update --cpuset-cpus (restored),"
  echo "    table $NODEDUP_TABLE + index in Postgres (created if missing, dropped on exit), rows of each run deleted from events and $NODEDUP_TABLE,"
  echo "    topic $TOPIC recreated, consumer groups k5-* deleted, textfile $PHASE_PROM (removed on exit)"
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
  grep -q "SINK_DEDUP" "$SINK_DIR/dist/config.js" || die "consumer-sink dist is older than the K5 change (no SINK_DEDUP): run 'pnpm --filter @eventpulse/consumer-sink build'"
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

SINK_PID=""
start_sink() {   # start_sink <n> <group> <dedup on|off> <crash point>
  local n="$1" group="$2" dedup="$3" crash="$4" extra=""
  [ "$dedup" = off ] && extra="SINK_DEDUP=false SINK_NODEDUP_TABLE=$NODEDUP_TABLE"
  # DEBUG_FAULTS is needed for CRASH_AT and for SINK_DEDUP=false. It does nothing by itself.
  # shellcheck disable=SC2086
  ( cd "$SINK_DIR" && exec env KAFKA_BROKERS="$K_BROKERS" CONSUMER_TOPIC="$TOPIC" CONSUMER_GROUP_ID="$group" \
      METRICS_PORT="$SINK_PORT" KAFKA_CLIENT_ID="k5-sink-$n" $SINK_ENV DEBUG_FAULTS=true CRASH_AT="$crash" CRASH_AFTER_BATCHES="$CRASH_BATCH" $extra \
      taskset -c "$CONSUMER_CPUS" node --env-file=.env dist/main.js ) >"$CAP/$testid.sink$n.out" 2>&1 &
  SINK_PID=$!
}
stop_sink() {
  if [ -n "${SINK_PID:-}" ] && kill -0 "$SINK_PID" 2>/dev/null; then kill "$SINK_PID" 2>/dev/null || true; wait "$SINK_PID" 2>/dev/null || true; fi
  SINK_PID=""
}

# One gauge for Grafana: 0 consumer running (load on), 1 consumer dead, 2 restarted but not processing yet, 3 processing again / draining, -1 not in a run.
set_phase() {
  { echo "# HELP ep_k5_phase K5 run phase: 0 consumer running, 1 consumer dead, 2 restarted not yet processing, 3 processing again, -1 no run."
    echo "# TYPE ep_k5_phase gauge"
    echo "ep_k5_phase{testid=\"${testid:-none}\",cell=\"${cell:-none}\"} $1"; } >"$PHASE_PROM.tmp" && mv "$PHASE_PROM.tmp" "$PHASE_PROM"
}
CREATED_TABLE=0
cleanup() {
  stop_sink; stop_server
  [ "$CREATED_TABLE" = 1 ] && pgq "DROP TABLE IF EXISTS $NODEDUP_TABLE" >/dev/null 2>&1 || true
  rm -f "$PHASE_PROM" "$PHASE_PROM.tmp"; restore_pinning
}
trap cleanup EXIT

acked_total() { curl -fsS --max-time 2 "$BASE_URL/metrics" 2>/dev/null | awk '/^ep_kafka_messages_acked_total/ {s+=$NF} END{printf "%d", s+0}'; }
record() { printf '%s\t%s\t%s\t%s\t%s\n' "$testid" "$1" "$2" "$3" "$4" >>"$RUNS_TSV"; }

run_k6() {   # run_k6 <testid> <run id> <cell>   (extra env names are forwarded to k6 by runner.sh)
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$1" SCRIPT="stage-2/K5/script.js" \
      RUN_ID="$2" CELL="$3" RATE="$RATE" DURATION="$DURATION" PHASE=measured \
      API_KEY="$API_KEY" BASE_URL="$BASE_URL" ROUTE="/healthz" ./runner.sh )
}

# Wait until the group's total lag is 0 (every record of the topic is inserted and committed).
drain_topic() {   # drain_topic <group>; returns 1 on timeout
  local g="$1" i lag parts
  for i in $(seq 1 "$DRAIN_TIMEOUT_S"); do
    sleep 1
    kg --describe --group "$g" 2>/dev/null | awk -v t="$TOPIC" '$2==t && $6 ~ /^[0-9]+$/ {l+=$6; p++} END{print l+0, p+0}' >"$CAP/.lag.$$" || true
    read -r lag parts <"$CAP/.lag.$$" || { lag=-1; parts=0; }
    [ "${parts:-0}" -ge "$PARTITIONS" ] && [ "${lag:-1}" = 0 ] && { rm -f "$CAP/.lag.$$"; sleep 3; return 0; }
  done
  rm -f "$CAP/.lag.$$"; return 1
}

one_run() {  # one_run <cell> <rep>
  cell="$1"; local rep="$2" cno dedup crash
  dedup="${cell%%-*}"; crash="${cell#*-}"; cno=$(cell_no "$cell")
  testid="k5-${cell//_/-}-n${rep}"
  local rid; rid=$(run_id "$cno" "$rep")
  local table="events"; [ "$dedup" = off ] && table="$NODEDUP_TABLE"
  echo; echo "== cell=$cell (dedup=$dedup crash=$crash) repeat=$rep ($testid) run id $rid =="
  set_phase -1
  stop_server; stop_sink
  for c in $KAFKA_CONTAINERS; do for _ in $(seq 1 60); do [ "$(docker inspect -f '{{.State.Health.Status}}' "$c")" = healthy ] && break; sleep 1; done; done
  recreate_topic "$TOPIC"
  pgq "DELETE FROM events WHERE $(RANGE "$rid")" >/dev/null
  pgq "DELETE FROM $NODEDUP_TABLE WHERE $(RANGE "$rid")" >/dev/null
  start_server
  echo "   settling 5 s"; sleep 5
  local group="k5-$testid-$$" status=ok
  local base_acked; base_acked=$(acked_total)

  # --- consumer #1, armed ---
  start_sink 1 "$group" "$dedup" "$crash"
  local pid1=$SINK_PID i
  for i in $(seq 1 300); do   # joined the group when it was assigned a partition (0.1 s polls, 30 s at most)
    [ "$(smetric ep_consumer_rebalances_total)" -ge 1 ] && break
    alive "$pid1" || die "consumer #1 died before the load started (see $CAP/$testid.sink1.out)"; sleep 0.1
  done
  [ "$i" = 300 ] && die "consumer #1 was not assigned a partition in 30 s (see $CAP/$testid.sink1.out)"

  # --- the load, in the background ---
  run_k6 "$testid" "$rid" "$cell" &
  local k6pid=$!
  for i in $(seq 1 600); do   # k6 has started when the API's acked counter moves
    [ "$(( $(acked_total) - base_acked ))" -ge 20 ] && break
    kill -0 "$k6pid" 2>/dev/null || break; sleep 0.1
  done
  local t0 t_kill="" t_start2="" t_resume="" t_assign="" c1=0 crash_rows="n/a" c
  t0=$(now); set_phase 0
  echo "   load started (t=0), $RATE req/s for ${DURATION_S}s; consumer #1 pid $pid1 armed with CRASH_AT=$crash on batch $CRASH_BATCH"

  if [ "$crash" != none ]; then
    # Poll the consumer every 0.1 s: remember its last consumed counter (the metrics die with the process), notice the SIGKILL.
    for i in $(seq 1 $((CRASH_TIMEOUT_S*10))); do
      alive "$pid1" || break
      c=$(smetric ep_consumer_messages_consumed_total); [ "$c" -gt 0 ] && c1=$c
      sleep 0.1
    done
    t_kill=$(now)
    if alive "$pid1"; then
      status=NO_CRASH; echo "!! consumer #1 did not crash within ${CRASH_TIMEOUT_S}s"
    else
      wait "$pid1" 2>/dev/null || true; SINK_PID=""
      crash_rows=$(grep -o 'rows=[0-9]*' "$CAP/$testid.sink1.out" | tail -1 | cut -d= -f2 || true); crash_rows="${crash_rows:-n/a}"
      set_phase 1; record crash "$(to_n "$t_kill")" "$(to_n "$t_kill")" "rows=$crash_rows consumed1=$c1"
      echo "   t=$(awk -v n="$t_kill" -v t="$t0" 'BEGIN{printf "%.1f", n-t}')s: consumer #1 was killed at $crash (rows in the chunk: $crash_rows, consumed before: $c1)"
      # --- consumer #2: same group, no crash point, started at once ---
      t_start2=$(now); set_phase 2
      start_sink 2 "$group" "$dedup" none
      for i in $(seq 1 $((RESUME_TIMEOUT_S*10))); do
        alive "$SINK_PID" || { status=SINK2_DIED; break; }
        [ -z "$t_assign" ] && [ "$(smetric ep_consumer_rebalances_total)" -ge 1 ] && t_assign=$(now)
        [ "$(smetric ep_consumer_messages_consumed_total)" -gt 0 ] && { t_resume=$(now); break; }
        sleep 0.1
      done
      if [ -n "$t_resume" ]; then
        set_phase 3; record resume "$(to_n "$t_resume")" "$(to_n "$t_resume")" "after $(awk -v a="$t_resume" -v b="$t_kill" 'BEGIN{printf "%.1f", a-b}') s"
        echo "   t=$(awk -v n="$t_resume" -v t="$t0" 'BEGIN{printf "%.1f", n-t}')s: consumer #2 processed its first chunk ($(awk -v a="$t_resume" -v b="$t_kill" 'BEGIN{printf "%.1f", a-b}') s after the kill)"
      else
        [ "$status" = ok ] && status=NO_RESUME; echo "!! consumer #2 did not resume ($status)"
      fi
    fi
  fi
  wait "$k6pid" || { status=FAILED; echo "!! $testid: k6 failed, see its manifest.md"; }
  record measured "$(to_n "$t0")" "$(date +%s)" "$status"

  # --- drain and audit ---
  echo "   load done, waiting for group lag 0"
  drain_topic "$group" || { echo "!! drain timeout after ${DRAIN_TIMEOUT_S}s: the audit of $testid is incomplete"; status=DRAIN_TIMEOUT; }
  local c2 ins2 dup2 total uniq tt
  c2=$(smetric ep_consumer_messages_consumed_total); ins2=$(smetric ep_consumer_rows_inserted_total); dup2=$(smetric ep_consumer_duplicates_total)
  [ "$crash" = none ] && [ -n "$SINK_PID" ] && c1=0   # control: only one consumer, its counters are in c2
  stop_sink
  total=$(pgq "SELECT count(*) FROM $table WHERE $(RANGE "$rid")" | tr -d ' \r')
  uniq=$(pgq "SELECT count(DISTINCT event_id) FROM $table WHERE $(RANGE "$rid")" | tr -d ' \r')
  tt=$(topic_total "$TOPIC")
  echo "   audit: rows $total, distinct $uniq, topic records $tt, consumer #1 consumed $c1, consumer #2 consumed $c2 (inserted $ins2, absorbed $dup2)"
  pgq "DELETE FROM $table WHERE $(RANGE "$rid")" >/dev/null
  kg --delete --group "$group" >/dev/null 2>&1 || true
  stop_server; set_phase -1

  local js; js=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  if [ -n "$js" ] && [ "$status" != DRAIN_TIMEOUT ]; then
    local ok202 missing dup_rows reread resume_s assign_s
    ok202=$(jq -r '.metrics.resp_202 | (.count // .values.count // 0)' "$js")
    missing=$(( ok202 - uniq ))
    dup_rows=$(( total - uniq ))
    # Reads = every record a handler pulled out of Kafka, the chunk that was in flight when the process died included.
    # reread = reads - records in the topic: records that were handed to a handler more than once (all crash points).
    if [[ "$crash_rows" =~ ^[0-9]+$ ]]; then reread=$(( c1 + crash_rows + c2 - tt )); else reread=$(( c1 + c2 - tt )); fi
    resume_s="n/a"; assign_s="n/a"
    [ -n "$t_resume" ] && resume_s=$(awk -v a="$t_resume" -v b="$t_kill" 'BEGIN{printf "%.1f", a-b}')
    [ -n "$t_assign" ] && assign_s=$(awk -v a="$t_assign" -v b="$t_kill" 'BEGIN{printf "%.1f", a-b}')
    python3 "$ANALYZE" row --json "$js" --testid "$testid" --phase measured --rate "$RATE" --rows "$uniq" \
      --ok-code 202 --fail-code 503 --p99-limit-ms 1000000 --err-limit-pct 100 \
      --var "dedup=$dedup" --var "crash=$crash" \
      --extra "acked=$ok202" --extra "topic=$tt" --extra "rows_total=$total" --extra "missing=$missing" \
      --extra "dup_rows=$dup_rows" --extra "absorbed=$dup2" --extra "crash_chunk=$crash_rows" --extra "reread=$reread" \
      --extra "assign_s=$assign_s" --extra "resume_s=$resume_s" --extra "status=$status" --out "$SUMMARY"
    echo "   -> acked $ok202, distinct rows $uniq (missing $missing), rows stored $total (stored twice: $dup_rows), absorbed by ON CONFLICT $dup2, re-read $reread, resume ${resume_s}s"
  else
    echo "   !! no summary row for $testid (k6 failed, or the drain timed out)"
  fi
}

# ---- 6. Main ----------------------------------------------------------------------
mkdir -p "$CAP"
preflight
printf 'testid\tphase\tstart_epoch\tend_epoch\tstatus\n' >"$RUNS_TSV"
for c in $KAFKA_CONTAINERS; do pin_container "$c" "$KAFKA_CPUS"; done
pin_container "$PG_CONTAINER" "$MON_CPUS"
for c in $MON_CONTAINERS; do pin_container "$c" "$MON_CPUS"; done
# The table of the dedup=off cells: same columns as events, no primary key, no unique index (a plain index on event_id for the audit).
if [ "$(pgq "SELECT to_regclass('$NODEDUP_TABLE') IS NOT NULL")" != t ]; then
  pgq "CREATE TABLE $NODEDUP_TABLE (LIKE events INCLUDING DEFAULTS)" >/dev/null; CREATED_TABLE=1
  pgq "CREATE INDEX ${NODEDUP_TABLE}_event_id_idx ON $NODEDUP_TABLE (event_id)" >/dev/null
fi

{
  echo "experiment: $EXP_ID   date: $(date -Is)"
  echo "git: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo n/a), $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l) dirty files"
  echo "kernel: $(uname -r)   node: $(node -v)   k6: $(k6 version | head -1)   cpus: $(nproc)"
  echo "pinning: server=$SERVER_CPUS consumer=$CONSUMER_CPUS brokers=$KAFKA_CPUS k6=$K6_CPUS monitoring+postgres=$MON_CPUS ($MON_CONTAINERS $PG_CONTAINER)"
  echo "settings: RATE=$RATE DURATION_S=$DURATION_S CRASH_BATCH=$CRASH_BATCH REPEATS=$REPEATS CELLS=[$CELLS] TOPIC=$TOPIC PARTITIONS=$PARTITIONS SESSION_TIMEOUT_MS=$SESSION_TIMEOUT_MS HEARTBEAT_MS=$HEARTBEAT_MS"
  echo "api: KAFKA_BROKERS=$K_BROKERS $API_ENV"
  echo "sink: $SINK_ENV  (+ DEBUG_FAULTS=true CRASH_AT=<point>; dedup off adds SINK_DEDUP=false SINK_NODEDUP_TABLE=$NODEDUP_TABLE)"
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
} >"$OUT/env.txt"

for rep in $(seq 1 "$REPEATS"); do
  order="$CELLS"; [ $((rep % 2)) = 0 ] && order=$(tr ' ' '\n' <<<"$CELLS" | tac | tr '\n' ' ')
  for v in $order; do one_run "$v" "$rep"; done
done

echo; python3 "$ANALYZE" table "$SUMMARY"
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Summary: $SUMMARY  captures: $CAP  env: $OUT/env.txt"
