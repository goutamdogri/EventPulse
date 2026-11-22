#!/usr/bin/env bash
# K11: Retention and lag
# ONE command runs everything:
#     ./run.sh            3 variants (earliest, latest, control) x 3 repeats, about 90 min in total
#
# What one run does (NOT a rate sweep: one load, and the consumer is taken away on a timetable):
#   1. fresh topic k11-events (6 partitions, RF 3, min ISR 2) with a SHORT retention: retention.ms=RETENTION_MS (3 min)
#      and segment.ms=SEGMENT_MS (20 s). The real ingest-api is started, then consumer-sink #1 (new group, earliest).
#   2. k6 sends RATE req/s for LOAD_S seconds (the "main" stream), then a small "trickle" of TRICKLE_RATE req/s until the restart.
#   3. at t=STOP_AT_S the consumer is stopped (SIGTERM). Its committed offsets stay in Kafka. The load keeps going.
#   4. the script waits OUTAGE_S, and then (except in the control) until the broker has really deleted data: the log
#      start offset ("oldest") of EVERY partition is past the committed offset of the group.
#   5. offsets are written down (oldest, end, committed per partition), then consumer-sink #2 is started with the SAME
#      group id and the reset policy of the variant. It is left to settle (lag 0, or nothing consumed for QUIET_S).
#   6. audit in Postgres: rows of this run (count(*), distinct ids) against the number of 202 answers from k6.
# The variants differ only in consumer #2 and in the length of the outage:
#   earliest   CONSUMER_FROM_BEGINNING=true   (auto.offset.reset=earliest), outage OUTAGE_S (longer than retention)
#   latest     CONSUMER_FROM_BEGINNING=false  (auto.offset.reset=latest),   outage OUTAGE_S (longer than retention)
#   control    earliest, outage CONTROL_OUTAGE_S (shorter than retention, so nothing is deleted)
# Consumer #1 is always earliest, so every variant starts the same way.
#
# Why the wait is not just "retention + a bit": Kafka checks retention only every log.retention.check.interval.ms
# (broker setting, 300000 = 5 min here, it cannot be set per topic) and only deletes CLOSED segments. So data older
# than retention can still be there for up to 5 more minutes. Step 4 waits for the deletion itself, not for a clock.
#
# Repeat is the OUTER loop; repeat 2 runs the variants in reverse order so drift hits all of them the same way.
#
# Usage:  DRY_RUN=1 ./run.sh                                               print the plan, run nothing
#         VARIANTS="control" REPEATS=1 RESULTS_NAME=k11-trial ./run.sh     small trial (never quote its numbers)
#
# Env knobs: RATE, LOAD_S, STOP_AT_S, OUTAGE_S, CONTROL_OUTAGE_S, TRICKLE_RATE, RETENTION_MS, SEGMENT_MS, VARIANTS, REPEATS,
#            RESULTS_NAME, PARTITIONS, WAIT_DELETE_S, QUIET_S, DRY_RUN.
set -euo pipefail

EXP_ID="K11"
# ---- 1. Settings (override from the environment) ------------------------------
RATE="${RATE:-1000}"                            # req/s of the main stream. About 60,000 events. Far below the K1 knee (about 7,500)
LOAD_S="${LOAD_S:-60}"                          # length of the main stream
STOP_AT_S="${STOP_AT_S:-15}"                    # consumer #1 is stopped this long after the load started
OUTAGE_S="${OUTAGE_S:-660}"                     # outage of earliest and latest. Retention 180 s + one broker check (up to 300 s) + margin
CONTROL_OUTAGE_S="${CONTROL_OUTAGE_S:-60}"      # outage of the control: shorter than retention, so nothing may be deleted
TRICKLE_RATE="${TRICKLE_RATE:-20}"              # req/s until the restart. Keeps the active segments rolling so retention can delete them
RETENTION_MS="${RETENTION_MS:-180000}"          # topic retention.ms (3 min)
SEGMENT_MS="${SEGMENT_MS:-20000}"               # topic segment.ms (a segment is closed 20 s after it was opened, when the next record arrives)
VARIANTS="${VARIANTS:-earliest latest control}"
REPEATS="${REPEATS:-3}"
RESULTS_NAME="${RESULTS_NAME:-k11-retention-lag}"
PARTITIONS="${PARTITIONS:-6}"
TOPIC="${TOPIC:-k11-events}"
WAIT_DELETE_S="${WAIT_DELETE_S:-420}"           # extra time after the planned outage to wait for the deletion (max)
QUIET_S="${QUIET_S:-20}"                        # consumer #2 is settled when it consumed nothing new for this long
DRAIN_TIMEOUT_S="${DRAIN_TIMEOUT_S:-300}"
SESSION_TIMEOUT_MS="${SESSION_TIMEOUT_MS:-10000}"
HEARTBEAT_MS="${HEARTBEAT_MS:-1000}"
SINK_PORT="${SINK_PORT:-8001}"
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
PG_CONTAINER="${PG_CONTAINER:-ep-postgres}"; PG_USER="${PG_USER:-eventpulse}"; PG_DB="${PG_DB:-eventpulse}"
# Pinning (same plan as K5): server 0,6 | consumer-sink 1,7 | brokers 2,8,3,9 | k6 4,10 | monitoring + Postgres 5,11.
# The container cpusets are moved with docker update (and put back on exit).
SERVER_CPUS="${SERVER_CPUS:-0,6}"
CONSUMER_CPUS="${CONSUMER_CPUS:-1,7}"
KAFKA_CPUS="${KAFKA_CPUS:-2,8,3,9}"
export K6_CPUS="${K6_CPUS:-4,10}"
MON_CPUS="${MON_CPUS:-5,11}"
KEEP_PINNING="${KEEP_PINNING:-0}"
DRY_RUN="${DRY_RUN:-0}"
K_BROKERS="${KAFKA_BROKERS:-localhost:9092,localhost:9094,localhost:9096}"
BOOTSTRAP_INT="kafka1:19092,kafka2:19092,kafka3:19092"        # for the CLI tools inside the containers

# The API settings, written out so a run never depends on my shell or on services/ingest-api/.env. Same for every variant.
API_ENV="KAFKA_ACKS=-1 KAFKA_IDEMPOTENT=true KAFKA_COMPRESSION=none KAFKA_LINGER_MS=0 BATCH_MAX_MESSAGES=500 BATCH_MAX_WAIT_MS=5 BATCH_MAX_BYTES=900000 BATCH_MAX_IN_FLIGHT=1 KAFKA_KEY_MODE=event"
# The consumer-sink settings that are NOT the variable. CONSUMER_FROM_BEGINNING is added per consumer (it IS the variable).
SINK_ENV="CONSUMER_ASSIGNOR=roundRobin CONSUMER_PARTITIONS_CONCURRENTLY=1 CONSUMER_MAX_BATCH_SIZE=1000 SINK_MAX_ROWS=1000 PG_POOL_MAX=5 LAG_POLL_MS=1000 LOG_ENABLED=true CONSUMER_SESSION_TIMEOUT_MS=$SESSION_TIMEOUT_MS CONSUMER_HEARTBEAT_INTERVAL_MS=$HEARTBEAT_MS"
# Unset anything from my shell that would silently change the API or the sink (the values above are the only source).
for k in KAFKA_ACKS KAFKA_IDEMPOTENT KAFKA_COMPRESSION KAFKA_LINGER_MS BATCH_MAX_MESSAGES BATCH_MAX_WAIT_MS BATCH_MAX_BYTES BATCH_MAX_IN_FLIGHT BATCH_MAX_QUEUE KAFKA_TOPIC KAFKA_KEY_MODE \
         CONSUMER_TOPIC CONSUMER_GROUP_ID CONSUMER_FROM_BEGINNING CONSUMER_ASSIGNOR CONSUMER_PARTITIONS_CONCURRENTLY CONSUMER_MAX_BATCH_SIZE \
         CONSUMER_SESSION_TIMEOUT_MS CONSUMER_HEARTBEAT_INTERVAL_MS SINK_MAX_ROWS SINK_PROCESSING_DELAY_MS SINK_DEDUP SINK_NODEDUP_TABLE \
         CRASH_AT CRASH_AFTER_BATCHES DEBUG_FAULTS PG_POOL_MAX LAG_POLL_MS METRICS_PORT KAFKA_CLIENT_ID KAFKA_HEALTH_MIN_ISR; do unset "$k" || true; done

ALL_VARIANTS=(earliest latest control)
vno() { local i; for i in "${!ALL_VARIANTS[@]}"; do [ "${ALL_VARIANTS[$i]}" = "$1" ] && { echo $((i+1)); return; }; done; echo 0; }
for v in $VARIANTS; do [ "$(vno "$v")" != 0 ] || { echo "ERROR: unknown variant '$v'. Known: ${ALL_VARIANTS[*]}" >&2; exit 1; }; done
# Run id of one run: 8 hex chars = e1 + variant number + repeat + 00. Every event id of the run starts with it.
run_id() { printf 'e1%02x%02x00' "$1" "$2"; }       # run_id <variant number> <repeat>
outage_of() { if [ "$1" = control ]; then echo "$CONTROL_OUTAGE_S"; else echo "$OUTAGE_S"; fi; }   # outage_of <variant>
fb_of()     { if [ "$1" = latest ];  then echo false; else echo true; fi; }                        # CONSUMER_FROM_BEGINNING of consumer #2

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-2/K11
STAGE_DIR="$(dirname "$HERE")"
RUNNER_DIR="$(dirname "$STAGE_DIR")"
ANALYZE="$RUNNER_DIR/analyze.py"
REPO="$(cd "$RUNNER_DIR/../.." && pwd)"
API_DIR="$REPO/services/ingest-api"
SINK_DIR="$REPO/services/consumer-sink"
TEXTFILE_DIR="${TEXTFILE_DIR:-$REPO/infra/compose/textfile}"
OUT="$HERE/results/$RESULTS_NAME"
CAP="$OUT/captures"; RUNS_TSV="$OUT/runs.tsv"; SUMMARY="$OUT/summary.tsv"
PHASE_PROM="$TEXTFILE_DIR/ep_k11.prom"

die() { echo "ERROR: $*" >&2; exit 1; }

# ---- 3. Plan / dry run ---------------------------------------------------------
n_var=$(wc -w <<<"$VARIANTS")
total_s=0
for v in $VARIANTS; do
  total_s=$(( total_s + REPEATS * ( STOP_AT_S + $(outage_of "$v") + 150 ) ))      # setup + outage + settle + audit (rough)
  [ "$v" = control ] || total_s=$(( total_s + REPEATS * 120 ))                    # the wait for the deletion adds about 2 min on average
done
echo "== $EXP_ID: $n_var variants x $REPEATS repeats = $((n_var*REPEATS)) measured runs, $RATE req/s for ${LOAD_S}s (about $((RATE*LOAD_S)) events) + trickle $TRICKLE_RATE req/s, results in $OUT =="
echo "== expected total about $(( total_s/60 )) min =="
if [ "$DRY_RUN" = "1" ]; then
  echo "  topic $TOPIC: $PARTITIONS partitions, RF 3, min.insync.replicas=2, retention.ms=$RETENTION_MS, segment.ms=$SEGMENT_MS, deleted + recreated before every run"
  echo "  per run: sink #1 (earliest) + load -> t=${STOP_AT_S}s sink #1 stopped (SIGTERM) -> outage -> wait until oldest offset > committed offset on every partition (not in the control) -> offsets written down -> sink #2 (same group) -> settle -> audit"
  for v in $VARIANTS; do
    echo "  variant $v: outage $(outage_of "$v") s, consumer #2 CONSUMER_FROM_BEGINNING=$(fb_of "$v"), restart at t=$(( STOP_AT_S + $(outage_of "$v") )) s, trickle for $(( STOP_AT_S + $(outage_of "$v") - LOAD_S )) s"
  done
  for rep in $(seq 1 "$REPEATS"); do
    order="$VARIANTS"; [ $((rep % 2)) = 0 ] && order=$(tr ' ' '\n' <<<"$VARIANTS" | tac | tr '\n' ' ')
    echo "  repeat $rep order: $order"
    echo "     run ids: $(for v in $order; do printf '%s ' "$(run_id "$(vno "$v")" "$rep")"; done)"
  done
  echo "  API: KAFKA_BROKERS=$K_BROKERS KAFKA_TOPIC=$TOPIC $API_ENV"
  echo "  sink: $SINK_ENV (+ CONSUMER_FROM_BEGINNING per consumer)"
  echo "  group timing: session timeout ${SESSION_TIMEOUT_MS} ms, heartbeat ${HEARTBEAT_MS} ms"
  echo "  CPU plan: server=$SERVER_CPUS consumer=$CONSUMER_CPUS brokers=$KAFKA_CPUS k6=$K6_CPUS monitoring+postgres=$MON_CPUS"
  echo "  changes the machine: SIGTERM of the consumer-sink the script started (by design), docker update --cpuset-cpus (restored),"
  echo "    rows of each run deleted from events, topic $TOPIC recreated (short retention, only on this topic), consumer groups k11-* deleted, textfile $PHASE_PROM (removed on exit)"
  exit 0
fi

# ---- 4. Helpers -----------------------------------------------------------------
KAFKA_CONTAINERS=""; MON_CONTAINERS=""
live_broker() { docker ps --format '{{.Names}}' | grep -E '^ep-kafka[0-9]+$' | sort | head -1; }
kt()  { timeout "${TMO:-20}" docker exec "$(live_broker)" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INT" "$@"; }
kg()  { timeout "${TMO:-20}" docker exec "$(live_broker)" /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_INT" "$@"; }
koff() { timeout 20 docker exec "$(live_broker)" /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server "$BOOTSTRAP_INT" --topic "$TOPIC" --time "$1" 2>/dev/null; }   # -2 = log start (oldest), -1 = log end. Lines: topic:partition:offset
pgq() { docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -tAc "$1"; }
RANGE() { echo "event_id BETWEEN '$1-0000-0000-0000-000000000000' AND '$1-ffff-ffff-ffff-ffffffffffff'"; }
now() { date +%s.%N; }
sleep_until() { local d; d=$(awk -v t="$1" -v n="$(now)" 'BEGIN{d=t-n; printf "%.3f", (d>0?d:0)}'); sleep "$d"; }
to_n() { awk -v x="$1" 'BEGIN{printf "%d", x}'; }
alive() { [ -r "/proc/$1/stat" ] && ! awk '{exit ($3=="Z")?0:1}' "/proc/$1/stat"; }
# A counter of the sink without labels, from its own /metrics (0 if the sink does not answer)
smetric() { curl -fsS --max-time 0.5 "localhost:$SINK_PORT/metrics" 2>/dev/null | awk -v n="$1" '$1==n {s+=$2} END{printf "%d", s+0}'; }

detect_containers() {
  KAFKA_CONTAINERS=$(docker ps -a --format '{{.Names}}' | grep -E '^ep-kafka[0-9]+$' | sort | tr '\n' ' ')
  [ "$(wc -w <<<"$KAFKA_CONTAINERS")" = 3 ] || die "expected 3 broker containers, found [$KAFKA_CONTAINERS]"
  MON_CONTAINERS=$(docker ps --format '{{.Names}} {{.Image}}' | awk '$2 ~ /prometheus|grafana|node-exporter|node_exporter|kafka-exporter|postgres-exporter/ {print $1}' | tr '\n' ' ')
}

CHECK_MS="unknown"
preflight() {
  for c in k6 taskset curl docker python3 awk node jq timeout; do command -v "$c" >/dev/null || die "'$c' not found in PATH"; done
  [ -x "$RUNNER_DIR/runner.sh" ] || die "$RUNNER_DIR/runner.sh missing or not executable"
  [ -f "$ANALYZE" ] || die "$ANALYZE missing"
  curl -fsS --max-time 3 "$PROM_URL/-/ready" >/dev/null || die "Prometheus not ready at $PROM_URL"
  detect_containers
  for c in $KAFKA_CONTAINERS; do [ "$(docker inspect -f '{{.State.Health.Status}}' "$c")" = healthy ] || die "$c is not healthy"; done
  docker ps --format '{{.Names}}' | grep -qx ep-kafka-exporter || echo "WARNING: ep-kafka-exporter is not running: the dashboard will miss the log start offset and the group offsets (docker compose -f infra/compose/docker-compose.kafka.yml up -d kafka-exporter)"
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
  # How often does the broker look for expired segments? This decides how long the outage must be.
  CHECK_MS=$(timeout 20 docker exec ep-kafka1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:19092 --entity-type brokers --entity-name 1 --describe --all 2>/dev/null \
             | awk '/log.retention.check.interval.ms=/ {split($1,a,"="); print a[2]; exit}' || true)
  CHECK_MS="${CHECK_MS:-unknown}"
  if [[ "$CHECK_MS" =~ ^[0-9]+$ ]]; then
    local need=$(( (RETENTION_MS + CHECK_MS)/1000 + SEGMENT_MS/1000 ))
    [ "$OUTAGE_S" -ge "$need" ] || echo "WARNING: OUTAGE_S=$OUTAGE_S is shorter than retention + check interval + segment time ($need s): the script will wait (up to WAIT_DELETE_S) for the deletion"
  else
    echo "WARNING: could not read log.retention.check.interval.ms from the broker"
  fi
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
  # retention.ms and segment.ms are set on THIS topic only. The brokers' own defaults are not touched.
  for i in $(seq 1 30); do
    kt --create --topic "$t" --partitions "$PARTITIONS" --replication-factor 3 \
       --config min.insync.replicas=2 --config retention.ms="$RETENTION_MS" --config segment.ms="$SEGMENT_MS" >/dev/null 2>&1 && break
    [ "$i" = 30 ] && die "could not create topic $t"; sleep 1
  done
  wait_full_isr "$t" 30 || die "topic $t: not all partitions have a full ISR after 30 s"
}

# Offsets of every partition, written to a file: partition, log start (oldest), log end, committed offset of the group.
# oldest and end come from kafka-get-offsets (-2 / -1). committed is CURRENT-OFFSET of kafka-consumer-groups ('-' = none, written as -1).
snapshot_offsets() {   # snapshot_offsets <group> <file>
  local g="$1" f="$2"
  { koff -2 | awk -F: '{print "O\t"$2"\t"$3}'
    koff -1 | awk -F: '{print "E\t"$2"\t"$3}'
    kg --describe --group "$g" 2>/dev/null | awk -v t="$TOPIC" '$2==t {c=($4 ~ /^[0-9]+$/)?$4:-1; print "C\t"$3"\t"c}'; } \
  | awk -F'\t' '{ if($1=="O")o[$2]=$3; if($1=="E")e[$2]=$3; if($1=="C")c[$2]=$3; p[$2]=1 }
                END{ for(k in p) printf "%s\t%s\t%s\t%s\n", k, (k in o)?o[k]:-1, (k in e)?e[k]:-1, (k in c)?c[k]:-1 }' | sort -n >"$f"
}
# deleted_unread = records between the committed offset and the log start (gone before anyone read them)
# retained_unread = records between max(committed, log start) and the log end (still in the topic, unread)
offset_sums() {   # offset_sums <file> -> "deleted_unread retained_unread n_partitions n_past (partitions whose log start is past the committed offset)"
  awk -F'\t' '{ n++; o=$2; e=$3; c=$4; if(c<0) next; if(o>c){ d+=o-c; past++; s=o } else s=c; r+=e-s } END{ printf "%d %d %d %d", d, r, n, past }' "$1"
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
start_sink() {   # start_sink <n> <group> <from beginning true|false>
  local n="$1" group="$2" fb="$3"
  # CONSUMER_FROM_BEGINNING is the consumer's reset policy: true = earliest, false = latest (see the client code).
  # It is written AFTER $SINK_ENV on purpose: with env, the last assignment wins.
  # shellcheck disable=SC2086
  ( cd "$SINK_DIR" && exec env KAFKA_BROKERS="$K_BROKERS" CONSUMER_TOPIC="$TOPIC" CONSUMER_GROUP_ID="$group" \
      METRICS_PORT="$SINK_PORT" KAFKA_CLIENT_ID="k11-sink-$n" $SINK_ENV CONSUMER_FROM_BEGINNING="$fb" \
      taskset -c "$CONSUMER_CPUS" node --env-file=.env dist/main.js ) >"$CAP/$testid.sink$n.out" 2>&1 &
  SINK_PID=$!
}
stop_sink() {
  if [ -n "${SINK_PID:-}" ] && kill -0 "$SINK_PID" 2>/dev/null; then kill "$SINK_PID" 2>/dev/null || true; wait "$SINK_PID" 2>/dev/null || true; fi
  SINK_PID=""
}

# One gauge for Grafana: 0 consumer running, 1 consumer stopped (outage), 2 consumer restarted, -1 not in a run.
set_phase() {
  { echo "# HELP ep_k11_phase K11 run phase: 0 consumer running, 1 consumer stopped, 2 consumer restarted, -1 no run."
    echo "# TYPE ep_k11_phase gauge"
    echo "ep_k11_phase{testid=\"${testid:-none}\",variant=\"${variant:-none}\"} $1"; } >"$PHASE_PROM.tmp" && mv "$PHASE_PROM.tmp" "$PHASE_PROM"
}
cleanup() {
  stop_sink; stop_server
  rm -f "$PHASE_PROM" "$PHASE_PROM.tmp"; restore_pinning
}
trap cleanup EXIT

acked_total() { curl -fsS --max-time 2 "$BASE_URL/metrics" 2>/dev/null | awk '/^ep_kafka_messages_acked_total/ {s+=$NF} END{printf "%d", s+0}'; }
record() { printf '%s\t%s\t%s\t%s\t%s\n' "$testid" "$1" "$2" "$3" "$4" >>"$RUNS_TSV"; }

run_k6() {   # run_k6 <testid> <run id> <variant> <trickle seconds>   (extra env names are forwarded to k6 by runner.sh)
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$1" SCRIPT="stage-2/K11/script.js" \
      RUN_ID="$2" CELL="$3" RATE="$RATE" LOAD_S="$LOAD_S" TRICKLE_RATE="$TRICKLE_RATE" TRICKLE_S="$4" PHASE=measured \
      API_KEY="$API_KEY" BASE_URL="$BASE_URL" ROUTE="/healthz" ./runner.sh )
}

# Wait until consumer #2 is settled: group lag 0, or (after at least 30 s) nothing newly consumed for QUIET_S.
# Under "latest" the group lag may NEVER reach 0: the consumer jumps to the end but commits nothing until it processes a chunk.
settle_consumer() {   # settle_consumer <group>; sets SETTLE_HOW
  local g="$1" i lag parts last=-1 c quiet=0
  SETTLE_HOW=timeout
  for i in $(seq 1 "$DRAIN_TIMEOUT_S"); do
    sleep 1
    c=$(smetric ep_consumer_messages_consumed_total)
    if [ "$c" = "$last" ]; then quiet=$((quiet+1)); else quiet=0; last=$c; fi
    kg --describe --group "$g" 2>/dev/null | awk -v t="$TOPIC" '$2==t && $6 ~ /^[0-9]+$/ {l+=$6; p++} END{print l+0, p+0}' >"$CAP/.lag.$$" || true
    read -r lag parts <"$CAP/.lag.$$" || { lag=-1; parts=0; }
    if [ "${parts:-0}" -ge "$PARTITIONS" ] && [ "${lag:-1}" = 0 ]; then SETTLE_HOW=lag0; break; fi
    if [ "$i" -ge 30 ] && [ "$quiet" -ge "$QUIET_S" ]; then SETTLE_HOW=quiet; break; fi
  done
  rm -f "$CAP/.lag.$$"
}

one_run() {  # one_run <variant> <rep>
  variant="$1"; local rep="$2" vn outage fb
  vn=$(vno "$variant"); outage=$(outage_of "$variant"); fb=$(fb_of "$variant")
  testid="k11-${variant}-n${rep}"
  local rid; rid=$(run_id "$vn" "$rep")
  local t_restart_s=$(( STOP_AT_S + outage )) trickle_s
  trickle_s=$(( t_restart_s - LOAD_S )); [ "$trickle_s" -gt 0 ] || trickle_s=0
  echo; echo "== variant=$variant (outage ${outage}s, consumer #2 from_beginning=$fb) repeat=$rep ($testid) run id $rid =="
  set_phase -1
  stop_server; stop_sink
  for c in $KAFKA_CONTAINERS; do for _ in $(seq 1 60); do [ "$(docker inspect -f '{{.State.Health.Status}}' "$c")" = healthy ] && break; sleep 1; done; done
  recreate_topic "$TOPIC"
  pgq "DELETE FROM events WHERE $(RANGE "$rid")" >/dev/null
  start_server
  echo "   settling 5 s"; sleep 5
  local group="k11-$testid-$$" status=ok
  local base_acked; base_acked=$(acked_total)

  # --- consumer #1: always earliest, new group ---
  start_sink 1 "$group" true
  local pid1=$SINK_PID i
  for i in $(seq 1 300); do   # joined the group when it was assigned a partition (0.1 s polls, 30 s at most)
    [ "$(smetric ep_consumer_rebalances_total)" -ge 1 ] && break
    alive "$pid1" || die "consumer #1 died before the load started (see $CAP/$testid.sink1.out)"; sleep 0.1
  done
  [ "$i" = 300 ] && die "consumer #1 was not assigned a partition in 30 s (see $CAP/$testid.sink1.out)"

  # --- the load, in the background (main stream, then the trickle until the restart) ---
  run_k6 "$testid" "$rid" "$variant" "$trickle_s" &
  local k6pid=$!
  for i in $(seq 1 600); do   # k6 has started when the API's acked counter moves
    [ "$(( $(acked_total) - base_acked ))" -ge 20 ] && break
    kill -0 "$k6pid" 2>/dev/null || break; sleep 0.1
  done
  local t0 t_stop t_restart
  t0=$(now); set_phase 0
  echo "   load started (t=0): $RATE req/s for ${LOAD_S}s, then $TRICKLE_RATE req/s until the restart at t=${t_restart_s}s"

  # --- stop consumer #1 (SIGTERM: a normal shutdown, not a crash; what it already committed stays in Kafka) ---
  sleep_until "$(awk -v t="$t0" -v s="$STOP_AT_S" 'BEGIN{printf "%.3f", t+s}')"
  local c1; c1=$(smetric ep_consumer_messages_consumed_total)
  stop_sink; t_stop=$(now); set_phase 1
  snapshot_offsets "$group" "$CAP/$testid.offsets.at-stop.tsv"
  echo "   t=$(awk -v n="$t_stop" -v t="$t0" 'BEGIN{printf "%.1f", n-t}')s: consumer #1 stopped after consuming $c1 records. Committed offsets per partition: $(cut -f4 "$CAP/$testid.offsets.at-stop.tsv" | tr '\n' ' ')"
  if awk -F'\t' '$4<0 {bad=1} END{exit bad?0:1}' "$CAP/$testid.offsets.at-stop.tsv"; then
    status=NO_COMMIT; echo "!! a partition had no committed offset when consumer #1 stopped (the reset policy would apply for another reason)"
  fi

  # --- the outage ---
  sleep_until "$(awk -v t="$t0" -v s="$t_restart_s" 'BEGIN{printf "%.3f", t+s}')"
  local deleted retained nparts npast waited=0
  if [ "$variant" != control ]; then
    # Planned outage is over. Is the data really gone? Wait (up to WAIT_DELETE_S) until the log start of EVERY partition is past the committed offset.
    while :; do
      snapshot_offsets "$group" "$CAP/$testid.offsets.at-restart.tsv"
      read -r deleted retained nparts npast <<<"$(offset_sums "$CAP/$testid.offsets.at-restart.tsv")"
      [ "$npast" -ge "$PARTITIONS" ] && break
      [ "$waited" -ge "$WAIT_DELETE_S" ] && { status=NOT_DELETED; echo "!! after ${WAIT_DELETE_S}s extra only $npast of $PARTITIONS partitions lost data: the run is not a valid outage-longer-than-retention run"; break; }
      [ $((waited % 30)) = 0 ] && echo "   waiting for the broker to delete expired segments ($npast of $PARTITIONS partitions past the committed offset, ${waited}s extra so far)"
      sleep 10; waited=$((waited+10))
    done
  else
    snapshot_offsets "$group" "$CAP/$testid.offsets.at-restart.tsv"
    read -r deleted retained nparts npast <<<"$(offset_sums "$CAP/$testid.offsets.at-restart.tsv")"
  fi
  t_restart=$(now)
  echo "   t=$(awk -v n="$t_restart" -v t="$t0" 'BEGIN{printf "%.1f", n-t}')s: restart. Records deleted before anyone read them: $deleted. Records still in the topic, unread: $retained ($npast of $PARTITIONS partitions lost data)"
  record outage "$(to_n "$t_stop")" "$(to_n "$t_restart")" "deleted_unread=$deleted retained_unread=$retained"

  # --- consumer #2: same group, the reset policy of the variant ---
  start_sink 2 "$group" "$fb"; set_phase 2
  local pid2=$SINK_PID
  for i in $(seq 1 300); do
    [ "$(smetric ep_consumer_rebalances_total)" -ge 1 ] && break
    alive "$pid2" || { status=SINK2_DIED; break; }; sleep 0.1
  done
  wait "$k6pid" || { status=FAILED; echo "!! $testid: k6 failed, see its manifest.md"; }
  record measured "$(to_n "$t0")" "$(date +%s)" "$status"
  settle_consumer "$group"
  local c2 ins2 lag_after oor total uniq
  c2=$(smetric ep_consumer_messages_consumed_total); ins2=$(smetric ep_consumer_rows_inserted_total)
  lag_after=$(kg --describe --group "$group" 2>/dev/null | awk -v t="$TOPIC" '$2==t && $6 ~ /^[0-9]+$/ {l+=$6} END{print l+0}')
  snapshot_offsets "$group" "$CAP/$testid.offsets.after.tsv"
  # Did the consumer say anything about the out-of-range offset? (0 = nothing in its output)
  oor=$(grep -ciE 'out of range|offset_out_of_range|reset' "$CAP/$testid.sink2.out" || true)
  stop_sink
  total=$(pgq "SELECT count(*) FROM events WHERE $(RANGE "$rid")" | tr -d ' \r')
  uniq=$(pgq "SELECT count(DISTINCT event_id) FROM events WHERE $(RANGE "$rid")" | tr -d ' \r')
  echo "   settled by $SETTLE_HOW. consumer #2 consumed $c2 (inserted $ins2). Group lag left: $lag_after. Lines about out-of-range/reset in its output: $oor. Rows: $total (distinct $uniq)"
  pgq "DELETE FROM events WHERE $(RANGE "$rid")" >/dev/null
  kg --delete --group "$group" >/dev/null 2>&1 || true
  stop_server; set_phase -1

  local js; js=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  if [ -n "$js" ]; then
    local ok202 missing
    ok202=$(jq -r '.metrics.resp_202 | (.count // .values.count // 0)' "$js")
    missing=$(( ok202 - uniq ))
    python3 "$ANALYZE" row --json "$js" --testid "$testid" --phase measured --rate "$RATE" --rows "$uniq" \
      --ok-code 202 --fail-code 503 --p99-limit-ms 1000000 --err-limit-pct 100 \
      --var "policy=$variant" \
      --extra "acked=$ok202" --extra "rows_total=$total" --extra "missing=$missing" \
      --extra "deleted_unread=$deleted" --extra "retained_unread=$retained" --extra "consumed2=$c2" \
      --extra "lag_after=$lag_after" --extra "oor_log_lines=$oor" --extra "settled=$SETTLE_HOW" --extra "extra_wait_s=$waited" \
      --extra "status=$status" --out "$SUMMARY"
    echo "   -> acked $ok202, distinct rows $uniq (missing $missing). Deleted before reading: $deleted. Still in the topic but skipped or read: $retained"
  else
    echo "   !! no summary row for $testid (k6 failed)"
  fi
}

# ---- 6. Main ----------------------------------------------------------------------
mkdir -p "$CAP"
preflight
printf 'testid\tphase\tstart_epoch\tend_epoch\tstatus\n' >"$RUNS_TSV"
for c in $KAFKA_CONTAINERS; do pin_container "$c" "$KAFKA_CPUS"; done
pin_container "$PG_CONTAINER" "$MON_CPUS"
for c in $MON_CONTAINERS; do pin_container "$c" "$MON_CPUS"; done

{
  echo "experiment: $EXP_ID   date: $(date -Is)"
  echo "git: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo n/a), $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l) dirty files"
  echo "kernel: $(uname -r)   node: $(node -v)   k6: $(k6 version | head -1)   cpus: $(nproc)"
  echo "pinning: server=$SERVER_CPUS consumer=$CONSUMER_CPUS brokers=$KAFKA_CPUS k6=$K6_CPUS monitoring+postgres=$MON_CPUS ($MON_CONTAINERS $PG_CONTAINER)"
  echo "settings: RATE=$RATE LOAD_S=$LOAD_S STOP_AT_S=$STOP_AT_S OUTAGE_S=$OUTAGE_S CONTROL_OUTAGE_S=$CONTROL_OUTAGE_S TRICKLE_RATE=$TRICKLE_RATE REPEATS=$REPEATS VARIANTS=[$VARIANTS] TOPIC=$TOPIC PARTITIONS=$PARTITIONS"
  echo "topic: retention.ms=$RETENTION_MS segment.ms=$SEGMENT_MS min.insync.replicas=2 RF=3"
  echo "broker: log.retention.check.interval.ms=$CHECK_MS"
  echo "api: KAFKA_BROKERS=$K_BROKERS $API_ENV"
  echo "sink: $SINK_ENV (+ CONSUMER_FROM_BEGINNING per consumer: true for #1 and for earliest/control, false for latest)"
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
} >"$OUT/env.txt"

for rep in $(seq 1 "$REPEATS"); do
  order="$VARIANTS"; [ $((rep % 2)) = 0 ] && order=$(tr ' ' '\n' <<<"$VARIANTS" | tac | tr '\n' ' ')
  for v in $order; do one_run "$v" "$rep"; done
done

echo; python3 "$ANALYZE" table "$SUMMARY"
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Summary: $SUMMARY  captures: $CAP  env: $OUT/env.txt"
