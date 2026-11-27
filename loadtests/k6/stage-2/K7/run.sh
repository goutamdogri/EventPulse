#!/usr/bin/env bash
# K7: Slow Postgres
# ONE command runs everything:
#     ./run.sh            4 cells (Postgres answers +0 / +50 / +200 / +500 ms slower) x 3 repeats, about 9 min each,
#                         about 1 h 50 min in total (the drain wait can make a run longer)
#
# What one run does:
#   1. fresh topic k7-events (6 partitions, RF 3, min ISR 2); the real ingest-api (acks=all, idempotent) is started.
#      The ingest-api never talks to Postgres in Stage 2, so it does not go through the proxy.
#   2. ONE consumer-sink is started. Its DATABASE_URL points at Toxiproxy (localhost:15434), so I can slow its database.
#   3. k6 sends RATE req/s for PRE_S + SLOW_S + POST_S seconds through the ingest-api (one steady rate, the whole run)
#   4. the script walks through three windows while the load never changes:
#        healthy   PRE_S s    no toxic
#        slow      SLOW_S s   a Toxiproxy "latency" toxic adds SLOW_MS to every answer of Postgres (cell s0 adds nothing: the control)
#        after     POST_S s   the toxic is deleted, the load is still on: the sink has to catch up while new events keep arriving
#   5. a sampler writes, every second, the consumer lag, the rows inserted and the bytes the topic uses on the brokers' disks
#   6. when k6 is done, wait until the group's lag is 0, then audit: rows in Postgres vs the 202 answers of k6
#   7. the numbers of the summary (lag growth, drain time, disk growth...) are computed from that sampler file
# Cells are s<ms>:  s0 s50 s200 s500   (ms of latency added to every answer Postgres sends to the sink)
# Repeat is the OUTER loop; repeat 2 runs the cells in reverse order so drift hits all of them the same way.
#
# Usage:  DRY_RUN=1 ./run.sh                                          print the plan, run nothing
#         CELLS="s200" REPEATS=1 RESULTS_NAME=k7-trial ./run.sh        small trial (never quote its numbers)
#         CALIBRATE=1 ./run.sh                                        separate probe: how fast can the sink drain when Postgres is healthy (never mixed with the runs)
#
# Env knobs: RATE, PRE_S, SLOW_S, POST_S, CELLS, REPEATS, RESULTS_NAME, PARTITIONS, DRAIN_LAG, DRAIN_TIMEOUT_S, CALIBRATE, DRY_RUN.
set -euo pipefail

EXP_ID="K7"
# ---- 1. Settings (override from the environment) ------------------------------
RATE="${RATE:-2000}"                            # req/s. Far below the K1 knee (about 7,500), the same as K5
PRE_S="${PRE_S:-60}"
SLOW_S="${SLOW_S:-120}"
POST_S="${POST_S:-120}"
DURATION_S=$(( PRE_S + SLOW_S + POST_S ))
DURATION="${DURATION_S}s"
CELLS="${CELLS:-s0 s50 s200 s500}"
REPEATS="${REPEATS:-3}"
RESULTS_NAME="${RESULTS_NAME:-k7-slow-postgres}"
PARTITIONS="${PARTITIONS:-6}"
TOPIC="${TOPIC:-k7-events}"
DRAIN_LAG="${DRAIN_LAG:-2000}"                  # lag at or below this counts as "caught up" (= 1 s of arrivals at RATE 2000)
DRAIN_TIMEOUT_S="${DRAIN_TIMEOUT_S:-900}"
CALIBRATE="${CALIBRATE:-0}"
SINK_PORT="${SINK_PORT:-8001}"
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
TOXI_URL="${TOXI_URL:-http://localhost:8474}"   # Toxiproxy's control API
TOXI_PORT="${TOXI_PORT:-15434}"                 # the proxied Postgres port (published by the compose service)
TOXI_UPSTREAM="${TOXI_UPSTREAM:-postgres:5432}"
PG_CONTAINER="${PG_CONTAINER:-ep-postgres}"; PG_USER="${PG_USER:-eventpulse}"; PG_DB="${PG_DB:-eventpulse}"
# Pinning: server 0,6 | consumer-sink 1,7 | brokers 2,8,3,9 | k6 4,10 | monitoring + Postgres + toxiproxy 5,11.
# The container cpusets are moved with docker update (and put back on exit).
SERVER_CPUS="${SERVER_CPUS:-0,6}"
CONSUMER_CPUS="${CONSUMER_CPUS:-1,7}"
KAFKA_CPUS="${KAFKA_CPUS:-2,8,3,9}"
export K6_CPUS="${K6_CPUS:-4,10}"
MON_CPUS="${MON_CPUS:-5,11}"
KEEP_PINNING="${KEEP_PINNING:-0}"
DRY_RUN="${DRY_RUN:-0}"
RETENTION_MS="${RETENTION_MS:-86400000}"        # 1 day: nothing is deleted during a run, so disk growth = what was written
K_BROKERS="${KAFKA_BROKERS:-localhost:9092,localhost:9094,localhost:9096}"
BOOTSTRAP_INT="kafka1:19092,kafka2:19092,kafka3:19092"        # for the CLI tools inside the containers

# The API settings, written out so a run never depends on my shell or on services/ingest-api/.env. Same for every cell.
API_ENV="KAFKA_ACKS=-1 KAFKA_IDEMPOTENT=true KAFKA_COMPRESSION=none KAFKA_LINGER_MS=0 BATCH_MAX_MESSAGES=500 BATCH_MAX_WAIT_MS=5 BATCH_MAX_BYTES=900000 BATCH_MAX_IN_FLIGHT=1 KAFKA_KEY_MODE=event"
# The consumer-sink settings. One consumer, one partition at a time, chunks of up to 1000 rows (same as K5). Not the variable.
SINK_ENV="CONSUMER_FROM_BEGINNING=true CONSUMER_ASSIGNOR=roundRobin CONSUMER_PARTITIONS_CONCURRENTLY=1 CONSUMER_MAX_BATCH_SIZE=1000 SINK_MAX_ROWS=1000 PG_POOL_MAX=5 LAG_POLL_MS=1000 LOG_ENABLED=true"
for k in KAFKA_ACKS KAFKA_IDEMPOTENT KAFKA_COMPRESSION KAFKA_LINGER_MS BATCH_MAX_MESSAGES BATCH_MAX_WAIT_MS BATCH_MAX_BYTES BATCH_MAX_IN_FLIGHT BATCH_MAX_QUEUE KAFKA_TOPIC KAFKA_KEY_MODE \
         CONSUMER_TOPIC CONSUMER_GROUP_ID CONSUMER_FROM_BEGINNING CONSUMER_ASSIGNOR CONSUMER_PARTITIONS_CONCURRENTLY CONSUMER_MAX_BATCH_SIZE \
         CONSUMER_SESSION_TIMEOUT_MS CONSUMER_HEARTBEAT_INTERVAL_MS SINK_MAX_ROWS SINK_PROCESSING_DELAY_MS SINK_DEDUP SINK_NODEDUP_TABLE \
         CRASH_AT CRASH_AFTER_BATCHES DEBUG_FAULTS PG_POOL_MAX LAG_POLL_MS METRICS_PORT KAFKA_CLIENT_ID KAFKA_HEALTH_MIN_ISR DATABASE_URL; do unset "$k" || true; done

ALL_CELLS=(s0 s50 s200 s500)
cell_no() { local i; for i in "${!ALL_CELLS[@]}"; do [ "${ALL_CELLS[$i]}" = "$1" ] && { echo $((i+1)); return; }; done; echo 0; }
for c in $CELLS; do [ "$(cell_no "$c")" != 0 ] || { echo "ERROR: unknown cell '$c'. Known: ${ALL_CELLS[*]}" >&2; exit 1; }; done
# Run id of one run: 8 hex chars = a7 + cell number + repeat + 00. Every event id of the run starts with it.
run_id() { printf 'a7%02x%02x00' "$1" "$2"; }       # run_id <cell number> <repeat>

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-2/K7
STAGE_DIR="$(dirname "$HERE")"
RUNNER_DIR="$(dirname "$STAGE_DIR")"
ANALYZE="$RUNNER_DIR/analyze.py"
REPO="$(cd "$RUNNER_DIR/../.." && pwd)"
API_DIR="$REPO/services/ingest-api"
SINK_DIR="$REPO/services/consumer-sink"
TEXTFILE_DIR="${TEXTFILE_DIR:-$REPO/infra/compose/textfile}"
OUT="$HERE/results/$RESULTS_NAME"
CAP="$OUT/captures"; RUNS_TSV="$OUT/runs.tsv"; SUMMARY="$OUT/summary.tsv"
PHASE_PROM="$TEXTFILE_DIR/ep_k7.prom"
DISK_PROM="$TEXTFILE_DIR/ep_kafka_disk.prom"
EXPORTER="$REPO/infra/scripts/kafka-disk-exporter.sh"

die() { echo "ERROR: $*" >&2; exit 1; }
slow_ms() { echo "${1#s}"; }

# ---- 3. Plan / dry run ---------------------------------------------------------
n_cell=$(wc -w <<<"$CELLS")
per_run_s=$(( DURATION_S + 150 ))   # setup + load + the extra drain after the load + audit (rough; a slow cell can need more)
echo "== $EXP_ID: $n_cell cells x $REPEATS repeats = $((n_cell*REPEATS)) measured runs, $RATE req/s for ${DURATION_S}s (about $((RATE*DURATION_S)) events), results in $OUT =="
echo "== expected total about $(( n_cell*REPEATS*per_run_s/60 )) min (plus the drain wait of the slow cells) =="
if [ "$DRY_RUN" = "1" ]; then
  echo "  windows per run: healthy ${PRE_S}s -> slow ${SLOW_S}s (toxic adds SLOW_MS to every Postgres answer) -> after ${POST_S}s (toxic removed, load still on) -> wait for lag 0 (at most ${DRAIN_TIMEOUT_S}s) -> audit"
  echo "  caught up = lag <= $DRAIN_LAG records. drain_s = seconds from removing the toxic until the first sample at or below that"
  for rep in $(seq 1 "$REPEATS"); do
    order="$CELLS"; [ $((rep % 2)) = 0 ] && order=$(tr ' ' '\n' <<<"$CELLS" | tac | tr '\n' ' ')
    echo "  repeat $rep order: $order"
    echo "     run ids: $(for v in $order; do printf '%s ' "$(run_id "$(cell_no "$v")" "$rep")"; done)"
  done
  echo "  cells: $(for v in $CELLS; do printf '%s=+%sms ' "$v" "$(slow_ms "$v")"; done)"
  echo "  API: KAFKA_BROKERS=$K_BROKERS KAFKA_TOPIC=$TOPIC $API_ENV"
  echo "  sink: DATABASE_URL=<.env URL with host:port replaced by localhost:$TOXI_PORT> $SINK_ENV"
  echo "  topic: $TOPIC deleted + recreated before every run ($PARTITIONS partitions, RF 3, min.insync.replicas=2)"
  echo "  CPU plan: server=$SERVER_CPUS consumer=$CONSUMER_CPUS brokers=$KAFKA_CPUS k6=$K6_CPUS monitoring+postgres+toxiproxy=$MON_CPUS"
  echo "  CALIBRATE=1 would instead: fill the topic with $((RATE*60)) events (60 s of load, no sink running), then start the sink and time the drain (healthy Postgres, no toxic)"
  echo "  changes the machine: Toxiproxy proxy 'pg' (created if missing) with a toxic 'lat' (deleted at the end of every window and on exit),"
  echo "    docker update --cpuset-cpus (restored), rows of each run deleted from events, topic $TOPIC recreated, consumer groups k7-* deleted,"
  echo "    background process $EXPORTER (docker exec du, stopped on exit), textfiles $PHASE_PROM and $DISK_PROM (removed on exit)"
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
# A counter / sum of the sink without labels, from its own /metrics (0 if the sink does not answer). %.6f: a _sum is in seconds
smetric() { curl -fsS --max-time 0.5 "localhost:$SINK_PORT/metrics" 2>/dev/null | awk -v n="$1" '$1==n {s+=$2} END{printf "%.6f", s+0}'; }
# Total lag = sum of the per-partition gauge lines ep_consumer_lag{partition="0"} ...
slag() { curl -fsS --max-time 0.5 "localhost:$SINK_PORT/metrics" 2>/dev/null | awk '/^ep_consumer_lag[{ ]/ {s+=$NF} END{printf "%d", s+0}'; }
# Bytes the topic uses on all brokers together (the exporter's textfile; 0 if it is not there yet)
topic_disk() { awk '/^ep_kafka_topic_disk_bytes/ {s+=$NF} END{printf "%d", s+0}' "$DISK_PROM" 2>/dev/null || echo 0; }

detect_containers() {
  KAFKA_CONTAINERS=$(docker ps -a --format '{{.Names}}' | grep -E '^ep-kafka[0-9]+$' | sort | tr '\n' ' ')
  [ "$(wc -w <<<"$KAFKA_CONTAINERS")" = 3 ] || die "expected 3 broker containers, found [$KAFKA_CONTAINERS]"
  MON_CONTAINERS=$(docker ps --format '{{.Names}} {{.Image}}' | awk '$2 ~ /prometheus|grafana|node-exporter|node_exporter|kafka-exporter|postgres-exporter|toxiproxy/ {print $1}' | tr '\n' ' ')
}

DIRECT_URL=""; PROXY_URL=""
preflight() {
  for c in k6 taskset curl docker python3 awk node jq timeout ss; do command -v "$c" >/dev/null || die "'$c' not found in PATH"; done
  [ -x "$RUNNER_DIR/runner.sh" ] || die "$RUNNER_DIR/runner.sh missing or not executable"
  [ -f "$ANALYZE" ] || die "$ANALYZE missing"
  [ -x "$EXPORTER" ] || die "$EXPORTER missing or not executable"
  curl -fsS --max-time 3 "$PROM_URL/-/ready" >/dev/null || die "Prometheus not ready at $PROM_URL"
  curl -fsS --max-time 3 "$TOXI_URL/version" >/dev/null || die "Toxiproxy API not answering at $TOXI_URL (is the ep-toxiproxy container up?)"
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
  # The sink's own .env URL, with only host:port replaced: same user, password and database, but through the proxy.
  DIRECT_URL=$(grep -E '^DATABASE_URL=' "$SINK_DIR/.env" | head -1 | cut -d= -f2- | tr -d '"')
  [ -n "$DIRECT_URL" ] || die "DATABASE_URL not found in $SINK_DIR/.env"
  PROXY_URL=$(sed -E "s#@[^/]+/#@localhost:${TOXI_PORT}/#" <<<"$DIRECT_URL")
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

# ---- Toxiproxy (same calls as Stage 1, B8 to B10) ---------------------------------
toxi() { curl -fsS --max-time 5 "$@"; }
setup_proxy() {
  # create the proxy if it does not exist yet (409 = exists, fine), then delete an old toxic of mine
  curl -sS --max-time 5 -o /dev/null -X POST "$TOXI_URL/proxies" -H 'Content-Type: application/json' \
    -d "{\"name\":\"pg\",\"listen\":\"0.0.0.0:${TOXI_PORT}\",\"upstream\":\"${TOXI_UPSTREAM}\",\"enabled\":true}" || true
  toxi "$TOXI_URL/proxies/pg" >/dev/null || die "proxy 'pg' does not exist and could not be created"
  clear_toxic
}
clear_toxic() { curl -sS --max-time 5 -o /dev/null -X DELETE "$TOXI_URL/proxies/pg/toxics/lat" || true; }
# latency toxic on the DOWNSTREAM (database -> sink) direction: every answer is delayed by <ms>. One transaction is BEGIN, INSERT and
# COMMIT = three answers, so one chunk write gets about 3 x ms slower. Existing connections are affected, so it can be switched mid-run.
set_latency() {   # set_latency <ms>
  clear_toxic
  toxi -X POST "$TOXI_URL/proxies/pg/toxics" -H 'Content-Type: application/json' \
    -d "{\"name\":\"lat\",\"type\":\"latency\",\"stream\":\"downstream\",\"toxicity\":1.0,\"attributes\":{\"latency\":$1,\"jitter\":0}}" >/dev/null \
    || die "could not add the latency toxic"
}

# ---- Topic ------------------------------------------------------------------------
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

# ---- Processes ----------------------------------------------------------------------
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
start_sink() {   # start_sink <group>
  # env vars beat --env-file, so DATABASE_URL here wins over .env
  # shellcheck disable=SC2086
  ( cd "$SINK_DIR" && exec env KAFKA_BROKERS="$K_BROKERS" CONSUMER_TOPIC="$TOPIC" CONSUMER_GROUP_ID="$1" \
      METRICS_PORT="$SINK_PORT" KAFKA_CLIENT_ID="k7-sink" DATABASE_URL="$PROXY_URL" $SINK_ENV \
      taskset -c "$CONSUMER_CPUS" node --env-file=.env dist/main.js ) >"$CAP/$testid.sink.out" 2>&1 &
  SINK_PID=$!
}
stop_sink() {
  if [ -n "${SINK_PID:-}" ] && kill -0 "$SINK_PID" 2>/dev/null; then kill "$SINK_PID" 2>/dev/null || true; wait "$SINK_PID" 2>/dev/null || true; fi
  SINK_PID=""
}
wait_sink_assigned() {   # joined the group when it was assigned a partition (0.1 s polls, 30 s at most)
  local i
  for i in $(seq 1 300); do
    [ "$(to_n "$(smetric ep_consumer_rebalances_total)")" -ge 1 ] && return 0
    kill -0 "$SINK_PID" 2>/dev/null || die "consumer-sink died before the load started (see $CAP/$testid.sink.out)"; sleep 0.1
  done
  die "consumer-sink was not assigned a partition in 30 s (see $CAP/$testid.sink.out)"
}

EXPORTER_PID=""
start_exporter() { KAFKA_TOPIC="$TOPIC" TEXTFILE_DIR="$TEXTFILE_DIR" INTERVAL=2 "$EXPORTER" >"$CAP/disk-exporter.out" 2>&1 & EXPORTER_PID=$!; }
stop_exporter() { [ -n "${EXPORTER_PID:-}" ] && { kill "$EXPORTER_PID" 2>/dev/null || true; wait "$EXPORTER_PID" 2>/dev/null || true; }; EXPORTER_PID=""; }

# The sampler: one line per second in <testid>.lag.tsv:  epoch  total_lag  rows_inserted  topic_disk_bytes
SAMPLER_PID=""
start_sampler() {   # start_sampler <file>
  ( while true; do printf '%s\t%s\t%s\t%s\n' "$(date +%s.%N)" "$(slag)" "$(to_n "$(smetric ep_consumer_rows_inserted_total)")" "$(topic_disk)"; sleep 1; done ) >"$1" 2>/dev/null &
  SAMPLER_PID=$!
}
stop_sampler() { [ -n "${SAMPLER_PID:-}" ] && { kill "$SAMPLER_PID" 2>/dev/null || true; wait "$SAMPLER_PID" 2>/dev/null || true; }; SAMPLER_PID=""; }

# Two gauges for Grafana. ep_k7_phase: 0 healthy, 1 slow, 2 after (toxic removed, load on), 3 load done and draining, -1 not in a run.
# ep_k7_slow_ms: the latency the toxic adds right now (0 when none).
set_phase() {   # set_phase <phase> <slow ms now>
  { echo "# HELP ep_k7_phase K7 run phase: 0 healthy, 1 slow, 2 after (load on), 3 draining after the load, -1 no run."
    echo "# TYPE ep_k7_phase gauge"
    echo "ep_k7_phase{testid=\"${testid:-none}\",cell=\"${cell:-none}\"} $1"
    echo "# HELP ep_k7_slow_ms Latency the Toxiproxy toxic adds to every Postgres answer right now."
    echo "# TYPE ep_k7_slow_ms gauge"
    echo "ep_k7_slow_ms{testid=\"${testid:-none}\",cell=\"${cell:-none}\"} $2"; } >"$PHASE_PROM.tmp" && mv "$PHASE_PROM.tmp" "$PHASE_PROM"
}
cleanup() {
  stop_sampler; stop_sink; stop_server; stop_exporter; clear_toxic
  rm -f "$PHASE_PROM" "$PHASE_PROM.tmp"; restore_pinning
}
trap cleanup EXIT

acked_total() { curl -fsS --max-time 2 "$BASE_URL/metrics" 2>/dev/null | awk '/^ep_kafka_messages_acked_total/ {s+=$NF} END{printf "%d", s+0}'; }
record() { printf '%s\t%s\t%s\t%s\t%s\n' "$testid" "$1" "$2" "$3" "$4" >>"$RUNS_TSV"; }

run_k6() {   # run_k6 <testid> <run id> <cell> <duration>   (extra env names are forwarded to k6 by runner.sh)
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$1" SCRIPT="stage-2/K7/script.js" \
      RUN_ID="$2" CELL="$3" RATE="$RATE" DURATION="$4" PRE_S="$PRE_S" SLOW_S="$SLOW_S" PHASE=measured \
      API_KEY="$API_KEY" BASE_URL="$BASE_URL" ROUTE="/healthz" ./runner.sh )
}

# Wait until the group's total lag is 0 (every record of the topic is inserted and committed). Same idea as K5.
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

# Average Postgres write time of the sink between two snapshots "sum count" of ep_consumer_db_write_seconds, in ms
snap() { echo "$(smetric ep_consumer_db_write_seconds_sum) $(smetric ep_consumer_db_write_seconds_count)"; }
avg_ms() { awk -v a="$1" -v b="$2" 'BEGIN{split(a,x," "); split(b,y," "); d=y[2]-x[2]; if (d>0) printf "%.0f", (y[1]-x[1])/d*1000; else print "n/a"}'; }

# Numbers from the sampler file: t_on and t_off are epoch seconds of the toxic going on and off (the same pair is used for s0, the control, so
# the windows are the same). Columns: lag_end_healthy lag_end_slow lag_peak growth_rps drain_s disk_mb_at_on disk_mb_peak
lag_stats() {   # lag_stats <file> <t_on> <t_off> <t_k6_end>
  awk -F'\t' -v on="$2" -v off="$3" -v kend="$4" -v dl="$DRAIN_LAG" '
    { t=$1; lag=$2; d=$4
      if (t < on) { h=lag; if (lag>hm) hm=lag }
      if (t <= off) { s=lag }
      if (lag>pk) pk=lag
      if (t >= on && t <= on+1 && !don) { don=d; ds=1 }
      if (d>dp) dp=d
      # least-squares slope of lag over the slow window, skipping its first 10 s (the ramp while the sink is still working through the old chunk sizes)
      if (t >= on+10 && t <= off) { n++; sx+=t; sy+=lag; sxx+=t*t; sxy+=t*lag }
      if (t >= off && !dr && lag <= dl) { dr=t }
    }
    END {
      g="n/a"; if (n>2 && (n*sxx-sx*sx)!=0) g=sprintf("%.0f", (n*sxy-sx*sy)/(n*sxx-sx*sx))
      dd="never"; dlv="n/a"; if (dr) { dd=sprintf("%.0f", dr-off); dlv=(dr<=kend)?"yes":"no" }
      printf "%d\t%d\t%d\t%s\t%s\t%s\t%.0f\t%.0f\n", h, s, pk, g, dd, dlv, don/1048576, dp/1048576
    }' "$1"
}

one_run() {  # one_run <cell> <rep>
  cell="$1"; local rep="$2" cno ms
  ms=$(slow_ms "$cell"); cno=$(cell_no "$cell")
  testid="k7-${cell}-n${rep}"
  local rid; rid=$(run_id "$cno" "$rep")
  echo; echo "== cell=$cell (+${ms} ms on every Postgres answer) repeat=$rep ($testid) run id $rid =="
  set_phase -1 0
  stop_server; stop_sink; clear_toxic
  for c in $KAFKA_CONTAINERS; do for _ in $(seq 1 60); do [ "$(docker inspect -f '{{.State.Health.Status}}' "$c")" = healthy ] && break; sleep 1; done; done
  recreate_topic "$TOPIC"
  pgq "DELETE FROM events WHERE $(RANGE "$rid")" >/dev/null
  start_server
  echo "   settling 5 s"; sleep 5
  local group="k7-$testid-$$" status=ok base_acked
  base_acked=$(acked_total)

  start_sink "$group"; wait_sink_assigned
  start_sampler "$CAP/$testid.lag.tsv"

  # --- the load, in the background ---
  run_k6 "$testid" "$rid" "$cell" "$DURATION" &
  local k6pid=$! i
  for i in $(seq 1 600); do   # k6 has started when the API's acked counter moves
    [ "$(( $(acked_total) - base_acked ))" -ge 20 ] && break
    kill -0 "$k6pid" 2>/dev/null || break; sleep 0.1
  done
  local t0 t_on t_off t_kend s_a s_b s_c s_d
  t0=$(now); set_phase 0 0
  echo "   load started (t=0), $RATE req/s for ${DURATION_S}s; healthy for ${PRE_S}s"

  # --- healthy window ---
  sleep $(( PRE_S > 20 ? 20 : 0 )); s_a=$(snap)
  sleep $(( PRE_S > 20 ? PRE_S - 20 : PRE_S )); s_b=$(snap)

  # --- slow window ---
  t_on=$(now)
  if [ "$ms" -gt 0 ]; then set_latency "$ms"; fi
  set_phase 1 "$ms"; record slow_on "$(to_n "$t_on")" "$(to_n "$t_on")" "+${ms}ms"
  echo "   t=$(awk -v n="$t_on" -v t="$t0" 'BEGIN{printf "%.1f", n-t}')s: Postgres slow (+${ms} ms) for ${SLOW_S}s"
  sleep 10; s_c=$(snap)
  sleep $(( SLOW_S - 10 )); s_d=$(snap)
  t_off=$(now)
  clear_toxic; set_phase 2 0; record slow_off "$(to_n "$t_off")" "$(to_n "$t_off")" "toxic removed"
  echo "   t=$(awk -v n="$t_off" -v t="$t0" 'BEGIN{printf "%.1f", n-t}')s: toxic removed, load still on for ${POST_S}s; lag now $(slag)"
  local dbw_h dbw_s; dbw_h=$(avg_ms "$s_a" "$s_b"); dbw_s=$(avg_ms "$s_c" "$s_d")
  # The toxic must really have slowed the sink: 3 answers per write, so a write takes at least about 3 x ms (90 % allowed)
  if [ "$ms" -gt 0 ] && { [ "$dbw_s" = "n/a" ] || [ "$dbw_s" -lt $(( ms * 3 * 9 / 10 )) ]; }; then
    status=TOXIC_NOT_EFFECTIVE; echo "!! average write took ${dbw_s} ms with +${ms} ms added: the sink is not going through the proxy (see $CAP/$testid.sink.out)"
  fi

  wait "$k6pid" || { status=FAILED; echo "!! $testid: k6 failed, see its manifest.md"; }
  t_kend=$(now)
  record measured "$(to_n "$t0")" "$(to_n "$t_kend")" "$status"

  # --- drain and audit ---
  set_phase 3 0
  echo "   load done, lag now $(slag); waiting for group lag 0"
  drain_topic "$group" || { echo "!! drain timeout after ${DRAIN_TIMEOUT_S}s: the audit of $testid is incomplete"; status=DRAIN_TIMEOUT; }
  local t_end; t_end=$(now)
  stop_sampler
  local total uniq tt
  total=$(pgq "SELECT count(*) FROM events WHERE $(RANGE "$rid")" | tr -d ' \r')
  uniq=$(pgq "SELECT count(DISTINCT event_id) FROM events WHERE $(RANGE "$rid")" | tr -d ' \r')
  tt=$(topic_total "$TOPIC")
  echo "   audit: rows $total, distinct $uniq, topic records $tt"
  pgq "DELETE FROM events WHERE $(RANGE "$rid")" >/dev/null
  kg --delete --group "$group" >/dev/null 2>&1 || true
  stop_sink; stop_server; set_phase -1 0
  record drained "$(to_n "$t_end")" "$(to_n "$t_end")" "status=$status"

  local st; st=$(lag_stats "$CAP/$testid.lag.tsv" "$t_on" "$t_off" "$t_kend")
  local lag_h lag_s lag_pk growth drain_s in_load disk_on disk_pk
  IFS=$'\t' read -r lag_h lag_s lag_pk growth drain_s in_load disk_on disk_pk <<<"$st"

  local js; js=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  if [ -n "$js" ] && [ "$status" != DRAIN_TIMEOUT ]; then
    local ok202 missing
    ok202=$(jq -r '.metrics.resp_202 | (.count // .values.count // 0)' "$js")
    missing=$(( ok202 - uniq ))
    python3 "$ANALYZE" row --json "$js" --testid "$testid" --phase measured --rate "$RATE" --rows "$uniq" \
      --ok-code 202 --fail-code 503 --p99-limit-ms 1000000 --err-limit-pct 100 --hook "$HERE/hook.py" \
      --var "slow_ms=$ms" \
      --extra "acked=$ok202" --extra "topic=$tt" --extra "rows_total=$total" --extra "missing=$missing" \
      --extra "dbw_healthy_ms=$dbw_h" --extra "dbw_slow_ms=$dbw_s" \
      --extra "lag_healthy_max=$lag_h" --extra "lag_end_slow=$lag_s" --extra "lag_peak=$lag_pk" --extra "growth_rps=$growth" \
      --extra "drain_s=$drain_s" --extra "drain_in_load=$in_load" --extra "disk_mb_at_on=$disk_on" --extra "disk_mb_peak=$disk_pk" \
      --extra "status=$status" --out "$SUMMARY"
    echo "   -> acked $ok202, distinct rows $uniq (missing $missing); write time healthy ${dbw_h} ms, slow ${dbw_s} ms; lag at toxic off $lag_s (peak $lag_pk), growth ${growth} rec/s, drain ${drain_s} s; topic disk ${disk_on} -> ${disk_pk} MB"
  else
    echo "   !! no summary row for $testid (k6 failed, or the drain timed out)"
  fi
}

# Separate probe, never part of the measured runs: how many records per second can the sink drain from a full topic when Postgres is healthy?
calibrate() {
  cell="cal"; testid="k7-calibrate"
  echo; echo "== CALIBRATE: fill $TOPIC with $((RATE*60)) events (no sink), then time one drain with healthy Postgres =="
  set_phase -1 0; stop_server; stop_sink; clear_toxic
  recreate_topic "$TOPIC"
  local rid="a7ca0000"; pgq "DELETE FROM events WHERE $(RANGE "$rid")" >/dev/null
  start_server; sleep 5
  run_k6 "$testid" "$rid" cal "60s" || echo "!! k6 failed"
  stop_server
  local tt group="k7-calibrate-$$"; tt=$(topic_total "$TOPIC")
  echo "   topic holds $tt records; starting the sink"
  start_sink "$group"; wait_sink_assigned
  local t1 t2; t1=$(now)
  drain_topic "$group" || echo "!! drain timeout"
  t2=$(now)
  local rows; rows=$(pgq "SELECT count(*) FROM events WHERE $(RANGE "$rid")" | tr -d ' \r')
  awk -v n="$rows" -v a="$t1" -v b="$t2" 'BEGIN{printf "   CALIBRATION: %d rows in %.1f s = %.0f rows/s (includes the 3 s pause at the end of drain_topic and up to 1 s polling; use as a rough ceiling)\n", n, b-a, n/(b-a)}' | tee -a "$OUT/calibration.txt"
  pgq "DELETE FROM events WHERE $(RANGE "$rid")" >/dev/null
  kg --delete --group "$group" >/dev/null 2>&1 || true
  stop_sink
}

# ---- 6. Main ----------------------------------------------------------------------
mkdir -p "$CAP"
preflight
printf 'testid\tphase\tstart_epoch\tend_epoch\tstatus\n' >"$RUNS_TSV"
for c in $KAFKA_CONTAINERS; do pin_container "$c" "$KAFKA_CPUS"; done
pin_container "$PG_CONTAINER" "$MON_CPUS"
for c in $MON_CONTAINERS; do pin_container "$c" "$MON_CPUS"; done
setup_proxy
start_exporter

{
  echo "experiment: $EXP_ID   date: $(date -Is)"
  echo "git: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo n/a), $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l) dirty files"
  echo "kernel: $(uname -r)   node: $(node -v)   k6: $(k6 version | head -1)   cpus: $(nproc)"
  echo "pinning: server=$SERVER_CPUS consumer=$CONSUMER_CPUS brokers=$KAFKA_CPUS k6=$K6_CPUS monitoring+postgres+toxiproxy=$MON_CPUS ($MON_CONTAINERS $PG_CONTAINER)"
  echo "settings: RATE=$RATE PRE_S=$PRE_S SLOW_S=$SLOW_S POST_S=$POST_S REPEATS=$REPEATS CELLS=[$CELLS] TOPIC=$TOPIC PARTITIONS=$PARTITIONS DRAIN_LAG=$DRAIN_LAG"
  echo "api: KAFKA_BROKERS=$K_BROKERS $API_ENV"
  echo "sink: DATABASE_URL via toxiproxy localhost:$TOXI_PORT -> $TOXI_UPSTREAM, $SINK_ENV"
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
} >"$OUT/env.txt"

if [ "$CALIBRATE" = 1 ]; then calibrate; exit 0; fi

for rep in $(seq 1 "$REPEATS"); do
  order="$CELLS"; [ $((rep % 2)) = 0 ] && order=$(tr ' ' '\n' <<<"$CELLS" | tac | tr '\n' ' ')
  for v in $order; do one_run "$v" "$rep"; done
done

echo; python3 "$ANALYZE" table "$SUMMARY"
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Summary: $SUMMARY  samples: $CAP/*.lag.tsv  env: $OUT/env.txt"
