#!/usr/bin/env bash
# K9: Hot key and skew
# ONE command runs everything:
#     ./run.sh            2 key modes x 4 rates x 3 repeats = 24 measured runs, about 2.5 to 3 min each, about 65 min in total
#
# What one run does:
#   1. fresh topic k9-events (6 partitions, RF 3, min ISR 2); the real ingest-api (acks=all, idempotent) is started with
#      TENANTS api keys and with KAFKA_KEY_MODE = the variable of this run (tenant or event)
#   2. 3 consumer-sinks are started in ONE new group (2 partitions each). Each one works on its 2 partitions at the same time
#      (CONSUMER_PARTITIONS_CONCURRENTLY=2) and every chunk gets DELAY_MS of artificial work, so ONE PARTITION can only be
#      drained at a limited speed (about BATCH rows per (DELAY_MS + insert time)). That limit is what the hot key runs into.
#   3. k6 sends RATE req/s for DURATION_S s. HOT_SHARE % of the requests use the key of the hot tenant, the rest is spread
#      over the other tenants.
#   4. right after the load: lag per partition and bytes per partition of the topic are captured
#   5. wait until the group's lag is 0
#   6. audit in Postgres (events_nodedup, plain INSERT, no primary key): rows of this run, distinct ids, rows of the hot tenant,
#      and how many events of a tenant were stored BEFORE an older event of the same tenant (the ordering I lose)
#
# The variable is the KEY the ingest-api puts on the Kafka record:
#   tenant  key = tenant_id  (all events of a tenant go to ONE partition, in order; the hot tenant makes one hot partition)
#   event   key = event_id   (events are spread over all partitions; no per-tenant order any more)
# Repeat is the OUTER loop; every even repeat runs the cells in reverse order so drift hits all of them the same way.
#
# Usage:  DRY_RUN=1 ./run.sh                                              print the plan, run nothing
#         CELLS="tenant-r1500 event-r1500" REPEATS=1 RESULTS_NAME=k9-trial ./run.sh      small trial (never quote its numbers)
#
# Env knobs: RATES, MODES, DURATION_S, HOT_SHARE, TENANTS, DELAY_MS, BATCH, CELLS, REPEATS, RESULTS_NAME, PARTITIONS, DRY_RUN.
set -euo pipefail

EXP_ID="K9"
# ---- 1. Settings (override from the environment) ------------------------------
RATES="${RATES:-1000 1500 2500 4000}"          # req/s. The hot partition gets HOT_SHARE % of it (see README for the arithmetic)
MODES="${MODES:-tenant event}"                  # the variable: Kafka key = tenant_id or event_id
DURATION_S="${DURATION_S:-60}"
DURATION="${DURATION_S}s"
HOT_SHARE="${HOT_SHARE:-80}"                    # % of the requests that belong to the one hot tenant
TENANTS="${TENANTS:-8}"                         # total number of tenants: 1 hot + (TENANTS-1) that share the rest
DELAY_MS="${DELAY_MS:-60}"                      # artificial work per chunk in the consumer-sink (the per-partition speed limit)
BATCH="${BATCH:-100}"                           # rows per chunk (CONSUMER_MAX_BATCH_SIZE and SINK_MAX_ROWS)
REPEATS="${REPEATS:-3}"
RESULTS_NAME="${RESULTS_NAME:-k9-hot-key}"
PARTITIONS="${PARTITIONS:-6}"
TOPIC="${TOPIC:-k9-events}"
# Same group timing as K8 (session 6 s, heartbeat 1 s, rebalance timeout 8 s). Nothing in K9 should trigger a rebalance.
SESSION_TIMEOUT_MS="${SESSION_TIMEOUT_MS:-6000}"
HEARTBEAT_MS="${HEARTBEAT_MS:-1000}"
REBALANCE_TIMEOUT_MS="${REBALANCE_TIMEOUT_MS:-8000}"
BASE_PORT="${BASE_PORT:-8000}"                  # consumer n listens on BASE_PORT+n (8001..8003 are scraped by Prometheus)
N_SINKS=3
JOIN_TIMEOUT_S="${JOIN_TIMEOUT_S:-60}"
DRAIN_TIMEOUT_S="${DRAIN_TIMEOUT_S:-420}"       # at the highest rate in tenant mode the hot partition backlog needs about 2 min
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
PG_CONTAINER="${PG_CONTAINER:-ep-postgres}"; PG_USER="${PG_USER:-eventpulse}"; PG_DB="${PG_DB:-eventpulse}"
# Pinning (same as K8): server 0,6 | consumer-sinks 1 and 7 (the 3rd one shares cpu 1) | brokers 2,8,3,9 | k6 4,10 | monitoring + Postgres 5,11.
SERVER_CPUS="${SERVER_CPUS:-0,6}"
SINK_CPU_LIST=(${SINK_CPU_LIST:-1 7 1})
KAFKA_CPUS="${KAFKA_CPUS:-2,8,3,9}"
export K6_CPUS="${K6_CPUS:-4,10}"
MON_CPUS="${MON_CPUS:-5,11}"
KEEP_PINNING="${KEEP_PINNING:-0}"
DRY_RUN="${DRY_RUN:-0}"
RETENTION_MS="${RETENTION_MS:-86400000}"
K_BROKERS="${KAFKA_BROKERS:-localhost:9092,localhost:9094,localhost:9096}"
BOOTSTRAP_INT="kafka1:19092,kafka2:19092,kafka3:19092"        # for the CLI tools inside the containers
NODEDUP_TABLE="events_nodedup"
HOT_TENANT="k9-hot"

# Tenants and their keys. Key 0 is the hot tenant. The ingest-api takes the tenant from the API key (never from the body),
# so "one tenant sends 80%" means "80% of the requests use key 0". Written out here so a run never depends on the .env file.
API_KEYS_VALUE="k9key0:$HOT_TENANT"; K9_KEYS="k9key0"
for i in $(seq 1 $((TENANTS-1))); do API_KEYS_VALUE="$API_KEYS_VALUE,k9key$i:k9-t$i"; K9_KEYS="$K9_KEYS,k9key$i"; done

# The API settings that are NOT the variable (KAFKA_KEY_MODE is added per run).
API_ENV="KAFKA_ACKS=-1 KAFKA_IDEMPOTENT=true KAFKA_COMPRESSION=none KAFKA_LINGER_MS=0 BATCH_MAX_MESSAGES=500 BATCH_MAX_WAIT_MS=5 BATCH_MAX_BYTES=900000 BATCH_MAX_IN_FLIGHT=1"
# The consumer-sink settings. CONCURRENTLY=2 = both partitions of a consumer are worked on at the same time, so the speed limit
# is per partition, not per consumer. SINK_PROCESSING_DELAY_MS with no CHUNKS setting = every chunk gets the delay.
# SINK_DEDUP=false: plain INSERT into events_nodedup (no primary key) so the audit can see every row.
SINK_ENV="CONSUMER_FROM_BEGINNING=true CONSUMER_PARTITIONS_CONCURRENTLY=2 CONSUMER_MAX_BATCH_SIZE=$BATCH SINK_MAX_ROWS=$BATCH PG_POOL_MAX=6 LAG_POLL_MS=1000 LOG_ENABLED=true CONSUMER_SESSION_TIMEOUT_MS=$SESSION_TIMEOUT_MS CONSUMER_HEARTBEAT_INTERVAL_MS=$HEARTBEAT_MS CONSUMER_REBALANCE_TIMEOUT_MS=$REBALANCE_TIMEOUT_MS SINK_PROCESSING_DELAY_MS=$DELAY_MS SINK_TRANSIENT_RETRY_WINDOW_MS=1000 SINK_RETRY_MAX_MS=1000 SINK_DEDUP=false SINK_NODEDUP_TABLE=$NODEDUP_TABLE"
# Anything of my shell that could change a run is removed (the --env-file .env does not override variables that are already set).
for k in KAFKA_ACKS KAFKA_IDEMPOTENT KAFKA_COMPRESSION KAFKA_LINGER_MS BATCH_MAX_MESSAGES BATCH_MAX_WAIT_MS BATCH_MAX_BYTES BATCH_MAX_IN_FLIGHT BATCH_MAX_QUEUE KAFKA_TOPIC KAFKA_KEY_MODE API_KEYS \
         CONSUMER_TOPIC CONSUMER_GROUP_ID CONSUMER_FROM_BEGINNING CONSUMER_ASSIGNOR CONSUMER_PARTITIONS_CONCURRENTLY CONSUMER_MAX_BATCH_SIZE \
         CONSUMER_SESSION_TIMEOUT_MS CONSUMER_HEARTBEAT_INTERVAL_MS CONSUMER_REBALANCE_TIMEOUT_MS SINK_MAX_ROWS SINK_PROCESSING_DELAY_MS \
         SINK_PROCESSING_DELAY_CHUNKS SINK_PROCESSING_DELAY_SKIP_CHUNKS SINK_TRANSIENT_RETRY_WINDOW_MS SINK_RETRY_MAX_MS SINK_DEDUP SINK_NODEDUP_TABLE \
         CRASH_AT CRASH_AFTER_BATCHES DEBUG_FAULTS PG_POOL_MAX LAG_POLL_MS METRICS_PORT KAFKA_CLIENT_ID KAFKA_HEALTH_MIN_ISR; do unset "$k" || true; done

# The cells: one cell = one key mode at one rate. Cell names look like tenant-r1500.
ALL_CELLS=(); for r in $RATES; do for m in $MODES; do ALL_CELLS+=("$m-r$r"); done; done
CELLS="${CELLS:-${ALL_CELLS[*]}}"
cell_no() { local i; for i in "${!ALL_CELLS[@]}"; do [ "${ALL_CELLS[$i]}" = "$1" ] && { echo $((i+1)); return; }; done; echo 0; }
for c in $CELLS; do [ "$(cell_no "$c")" != 0 ] || { echo "ERROR: unknown cell '$c'. Known: ${ALL_CELLS[*]}" >&2; exit 1; }; done
for m in $MODES; do case "$m" in tenant|event) ;; *) echo "ERROR: MODES may only contain tenant and event" >&2; exit 1;; esac; done
# Run id of one run: 8 hex chars = a9 + cell number + repeat + 00. Every event id of the run starts with it.
run_id() { printf 'a9%02x%02x00' "$1" "$2"; }       # run_id <cell number> <repeat>

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-2/K9
STAGE_DIR="$(dirname "$HERE")"
RUNNER_DIR="$(dirname "$STAGE_DIR")"
ANALYZE="$RUNNER_DIR/analyze.py"
REPO="$(cd "$RUNNER_DIR/../.." && pwd)"
API_DIR="$REPO/services/ingest-api"
SINK_DIR="$REPO/services/consumer-sink"
OUT="$HERE/results/$RESULTS_NAME"
CAP="$OUT/captures"; RUNS_TSV="$OUT/runs.tsv"; SUMMARY="$OUT/summary.tsv"

die() { echo "ERROR: $*" >&2; exit 1; }

# ---- 3. Plan / dry run ---------------------------------------------------------
n_cell=$(wc -w <<<"$CELLS")
per_run_s=$(( DURATION_S + 100 ))   # topic + API + group setup, load, drain (the hot partition backlog in tenant mode), audit (rough)
echo "== $EXP_ID: $n_cell cells x $REPEATS repeats = $((n_cell*REPEATS)) measured runs, ${DURATION_S}s each, results in $OUT =="
echo "== expected total about $(( n_cell*REPEATS*per_run_s/60 )) min =="
if [ "$DRY_RUN" = "1" ]; then
  echo "  per run: topic recreated -> API (key mode = the variable) -> $N_SINKS consumers (one group) -> k6 load -> lag + bytes per partition -> drain to lag 0 -> audit in Postgres"
  echo "  load: $HOT_SHARE% of the requests for tenant $HOT_TENANT, the rest spread over $((TENANTS-1)) other tenants ($TENANTS keys in total)"
  echo "  consumer speed limit: chunks of up to $BATCH rows, ${DELAY_MS} ms of extra work per chunk, 2 partitions at the same time per consumer"
  echo "  rates: $RATES | key modes: $MODES | cells: $CELLS"
  for rep in $(seq 1 "$REPEATS"); do
    order="$CELLS"; [ $((rep % 2)) = 0 ] && order=$(tr ' ' '\n' <<<"$CELLS" | tac | tr '\n' ' ')
    echo "  repeat $rep order: $order"
    echo "     run ids: $(for v in $order; do printf '%s ' "$(run_id "$(cell_no "$v")" "$rep")"; done)"
  done
  echo "  API: KAFKA_BROKERS=$K_BROKERS KAFKA_TOPIC=$TOPIC KAFKA_KEY_MODE=<tenant|event> API_KEYS=<$TENANTS keys> $API_ENV"
  echo "  sink: $SINK_ENV  (+ CONSUMER_ASSIGNOR=roundRobin, ports $((BASE_PORT+1))..$((BASE_PORT+N_SINKS)))"
  echo "  topic: $TOPIC deleted + recreated before every run ($PARTITIONS partitions, RF 3, min.insync.replicas=2)"
  echo "  CPU plan: server=$SERVER_CPUS consumers=${SINK_CPU_LIST[*]} brokers=$KAFKA_CPUS k6=$K6_CPUS monitoring+postgres=$MON_CPUS"
  echo "  changes the machine: SIGTERM / SIGKILL of the consumer-sinks and the API the script started (by design),"
  echo "    docker update --cpuset-cpus (restored), table $NODEDUP_TABLE + index in Postgres (created if missing, dropped on exit if I created it),"
  echo "    rows of each run deleted from $NODEDUP_TABLE, topic $TOPIC recreated, consumer groups k9-* deleted"
  exit 0
fi

# ---- 4. Helpers -----------------------------------------------------------------
KAFKA_CONTAINERS=""; MON_CONTAINERS=""
live_broker() { docker ps --format '{{.Names}}' | grep -E '^ep-kafka[0-9]+$' | sort | head -1; }
kt()  { timeout "${TMO:-20}" docker exec "$(live_broker)" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INT" "$@"; }
kg()  { timeout "${TMO:-20}" docker exec "$(live_broker)" /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_INT" "$@"; }
klog() { timeout "${TMO:-30}" docker exec "$(live_broker)" /opt/kafka/bin/kafka-log-dirs.sh --bootstrap-server "$BOOTSTRAP_INT" "$@"; }
pgq() { docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -tAc "$1"; }
RANGE() { echo "event_id BETWEEN '$1-0000-0000-0000-000000000000' AND '$1-ffff-ffff-ffff-ffffffffffff'"; }
now() { date +%s.%N; }
to_n() { awk -v x="$1" 'BEGIN{printf "%d", x}'; }
alive() { [ -r "/proc/$1/stat" ] && ! awk '{exit ($3=="Z")?0:1}' "/proc/$1/stat"; }   # a killed child waiting to be reaped is not alive
# A metric of consumer n without labels, from its own /metrics (0 if it does not answer)
smetric() { curl -fsS --max-time 0.5 "localhost:$((BASE_PORT+$1))/metrics" 2>/dev/null | awk -v n="$2" '$1==n {s+=$2} END{printf "%d", s+0}'; }

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
  docker exec "$(live_broker)" test -x /opt/kafka/bin/kafka-log-dirs.sh || die "kafka-log-dirs.sh not found inside a broker (needed for bytes per partition)"
  pgq "select 1" >/dev/null || die "cannot run psql in container $PG_CONTAINER"
  local p
  for p in 8000 $((BASE_PORT+1)) $((BASE_PORT+2)) $((BASE_PORT+3)); do ss -ltn "sport = :$p" | grep -q LISTEN && die "something already listens on :$p (an old ingest-api or consumer-sink?). Stop it first"; done
  [ -f "$SINK_DIR/dist/main.js" ] || die "$SINK_DIR/dist/main.js missing: run 'pnpm --filter @eventpulse/consumer-sink build'"
  grep -q "SINK_PROCESSING_DELAY_MS" "$SINK_DIR/dist/config.js" || die "consumer-sink dist has no SINK_PROCESSING_DELAY_MS: rebuild it"
  [ -f "$SINK_DIR/.env" ] || die "$SINK_DIR/.env missing"
  [ -f "$API_DIR/dist/main.js" ] || die "$API_DIR/dist/main.js missing: run 'pnpm --filter @eventpulse/ingest-api build'"
  grep -q "KAFKA_KEY_MODE" "$API_DIR/dist/config.js" || die "ingest-api dist has no KAFKA_KEY_MODE: rebuild it"
  [ -f "$API_DIR/.env" ] || die "$API_DIR/.env missing"
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

SERVER_PID=""
start_server() {   # start_server <key mode>
  stop_server
  # API_KEYS has commas and colons, so it is passed as one quoted word; $API_ENV is split on purpose (one NAME=value per word).
  # shellcheck disable=SC2086
  ( cd "$API_DIR" && exec env KAFKA_BROKERS="$K_BROKERS" KAFKA_TOPIC="$TOPIC" KAFKA_KEY_MODE="$1" API_KEYS="$API_KEYS_VALUE" $API_ENV \
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

# --- consumers: SINK_PIDS[n] is consumer n (1..3), port BASE_PORT+n -----------------
declare -A SINK_PIDS=()
GROUP=""
start_sink() {   # start_sink <n>
  local n="$1" cpu="${SINK_CPU_LIST[$(( $1 - 1 ))]}"
  # shellcheck disable=SC2086
  ( cd "$SINK_DIR" && exec env KAFKA_BROKERS="$K_BROKERS" CONSUMER_TOPIC="$TOPIC" CONSUMER_GROUP_ID="$GROUP" CONSUMER_ASSIGNOR=roundRobin \
      METRICS_PORT="$((BASE_PORT+n))" KAFKA_CLIENT_ID="k9-sink-$n" $SINK_ENV \
      taskset -c "$cpu" node --env-file=.env dist/main.js ) >"$CAP/$testid.sink$n.out" 2>&1 &
  SINK_PIDS[$n]=$!
}
wait_assigned() {   # wait_assigned <n>: consumer n has had its first assignment (0.1 s polls)
  local n="$1" i
  for i in $(seq 1 $((JOIN_TIMEOUT_S*10))); do
    [ "$(smetric "$n" ep_consumer_rebalances_total)" -ge 1 ] && return 0
    alive "${SINK_PIDS[$n]}" || die "consumer $n died before it joined (see $CAP/$testid.sink$n.out)"; sleep 0.1
  done
  die "consumer $n was not assigned a partition in ${JOIN_TIMEOUT_S}s (see $CAP/$testid.sink$n.out)"
}
owned_total() {   # partitions owned by the live consumers, added up (a settled group owns exactly $PARTITIONS)
  local n s=0 v
  for n in "${!SINK_PIDS[@]}"; do if alive "${SINK_PIDS[$n]}"; then v=$(smetric "$n" ep_consumer_assigned_partitions); s=$((s+v)); fi; done
  echo "$s"
}
wait_settled() {   # all partitions owned, three polls in a row (the group has stopped moving)
  local i ok=0
  for i in $(seq 1 $((JOIN_TIMEOUT_S*2))); do
    if [ "$(owned_total)" = "$PARTITIONS" ]; then ok=$((ok+1)); else ok=0; fi
    [ "$ok" -ge 3 ] && return 0; sleep 0.5
  done
  die "the group did not settle on $PARTITIONS owned partitions in ${JOIN_TIMEOUT_S}s"
}
stop_sinks() {   # SIGTERM (leave the group), wait up to 10 s, then SIGKILL
  local n p i
  for n in "${!SINK_PIDS[@]}"; do p="${SINK_PIDS[$n]}"; kill -TERM "$p" 2>/dev/null || true; done
  for i in $(seq 1 100); do
    local any=0; for n in "${!SINK_PIDS[@]}"; do alive "${SINK_PIDS[$n]}" && any=1; done
    [ "$any" = 0 ] && break; sleep 0.1
  done
  for n in "${!SINK_PIDS[@]}"; do p="${SINK_PIDS[$n]}"; kill -KILL "$p" 2>/dev/null || true; wait "$p" 2>/dev/null || true; done
  SINK_PIDS=()
}

CREATED_TABLE=0; SAMPLER_PID=""
stop_sampler() { [ -n "${SAMPLER_PID:-}" ] && { kill "$SAMPLER_PID" 2>/dev/null || true; wait "$SAMPLER_PID" 2>/dev/null || true; }; SAMPLER_PID=""; }
cleanup() {
  stop_sampler; stop_sinks; stop_server
  [ "$CREATED_TABLE" = 1 ] && pgq "DROP TABLE IF EXISTS $NODEDUP_TABLE" >/dev/null 2>&1 || true
  restore_pinning
}
trap cleanup EXIT

acked_total() { curl -fsS --max-time 2 "$BASE_URL/metrics" 2>/dev/null | awk '/^ep_kafka_messages_acked_total/ {s+=$NF} END{printf "%d", s+0}'; }
record() { printf '%s\t%s\t%s\t%s\t%s\n' "$testid" "$1" "$2" "$3" "$4" >>"$RUNS_TSV"; }

# The number of rows of this run in Postgres, sampled every ~0.3 s: how much the group has drained at any moment of the load.
sample_rows() {   # sample_rows <file> <run id>   (runs in the background until it is killed)
  while :; do echo "$(now) $(pgq "SELECT count(*) FROM $NODEDUP_TABLE WHERE $(RANGE "$2")" | tr -d ' \r')" >>"$1"; sleep 0.3; done
}
rows_at() {   # rows_at <file> <epoch>: the last sampled row count at or before that time
  awk -v t="$2" '$1 <= t {v=$2} END{printf "%d", v+0}' "$1"
}

# Bytes per partition of the topic (what the brokers store; the 3 replicas are the same size, so broker 1 is enough).
# kafka-log-dirs prints a few text lines and then ONE json line; "partition" there is e.g. k9-events-3.
capture_bytes() {   # capture_bytes <file>  -> lines "partition bytes"
  klog --describe --topic-list "$TOPIC" 2>/dev/null | grep '^{' | head -1 \
    | jq -r '.brokers[0].logDirs[0].partitions[] | "\(.partition | split("-") | last) \(.size)"' | sort -n >"$1" 2>/dev/null || : >"$1"
}
bytes_stats() {   # bytes_stats <file> -> "<hot partition> <hot share %> <partitions with over 5% of the bytes>"
  awk '{b[$1]=$2; s+=$2} END{ if (s==0) { print "NA NA NA"; exit } for (p in b) { if (b[p]>m) { m=b[p]; mp=p } if (b[p]>0.05*s) n++ } printf "%s %.1f %d", mp, 100*m/s, n }' "$1"
}
# Lag per partition, right now (the lag the load has built up). Columns of kafka-consumer-groups: GROUP TOPIC PARTITION CUR END LAG
capture_lag() {   # capture_lag <group> <file>
  kg --describe --group "$1" 2>/dev/null | awk -v t="$TOPIC" '$2==t && $6 ~ /^[0-9]+$/ {print $3, $6}' | sort -n >"$2" || : >"$2"
}
lag_stats() {   # lag_stats <file> -> "<max lag on one partition> <partition> <lag of the other partitions added up>"
  awk '{l[$1]=$2; s+=$2} END{ for (p in l) if (l[p]>=m) { m=l[p]; mp=p } if (m=="") { print "NA NA NA"; exit } printf "%d %s %d", m, mp, s-m }' "$1"
}

run_k6() {   # run_k6 <testid> <run id> <cell> <rate>   (extra env names are forwarded to k6 by runner.sh)
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$1" SCRIPT="stage-2/K9/script.js" \
      RUN_ID="$2" CELL="$3" RATE="$4" DURATION="$DURATION" PHASE=measured K9_KEYS="$K9_KEYS" HOT_SHARE="$HOT_SHARE" \
      BASE_URL="$BASE_URL" ROUTE="/healthz" ./runner.sh )
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
  cell="$1"; local rep="$2" cno mode rate
  mode="${cell%%-*}"; rate="${cell##*-r}"; cno=$(cell_no "$cell")
  testid="k9-${cell}-n${rep}"
  local rid; rid=$(run_id "$cno" "$rep")
  echo; echo "== cell=$cell (key=$mode, $rate req/s) repeat=$rep ($testid) run id $rid =="
  stop_server; stop_sinks
  for c in $KAFKA_CONTAINERS; do for _ in $(seq 1 60); do [ "$(docker inspect -f '{{.State.Health.Status}}' "$c")" = healthy ] && break; sleep 1; done; done
  recreate_topic "$TOPIC"
  pgq "DELETE FROM $NODEDUP_TABLE WHERE $(RANGE "$rid")" >/dev/null
  start_server "$mode"
  echo "   settling 5 s"; sleep 5
  GROUP="k9-$testid-$$"
  local status=ok base_acked; base_acked=$(acked_total)

  # --- the consumers, all started before the load ---
  local n
  for n in $(seq 1 "$N_SINKS"); do start_sink "$n"; wait_assigned "$n"; done
  wait_settled
  echo "   group settled: ${#SINK_PIDS[@]} consumers own $(owned_total) partitions"

  # --- the load, in the background; the sampler counts rows of this run in Postgres ---
  local rows_file="$CAP/$testid.rows.txt"; : >"$rows_file"
  sample_rows "$rows_file" "$rid" & SAMPLER_PID=$!
  run_k6 "$testid" "$rid" "$cell" "$rate" &
  local k6pid=$! i
  for i in $(seq 1 600); do   # k6 has started when the API's acked counter moves
    [ "$(( $(acked_total) - base_acked ))" -ge 20 ] && break
    kill -0 "$k6pid" 2>/dev/null || break; sleep 0.1
  done
  local t0; t0=$(now)
  echo "   load started (t=0): $rate req/s for ${DURATION_S}s, key=$mode"
  wait "$k6pid" || { status=FAILED; echo "!! $testid: k6 failed, see its manifest.md"; }
  local t_load_end; t_load_end=$(now)
  record measured "$(to_n "$t0")" "$(to_n "$t_load_end")" "$status"

  # --- what the load left behind: lag and bytes per partition, taken right after the load ---
  capture_lag "$GROUP" "$CAP/$testid.lag-at-load-end.txt"
  capture_bytes "$CAP/$testid.partition-bytes.txt"
  local lag_max lag_part lag_rest hot_part hot_share parts_used consumed_rps
  read -r lag_max lag_part lag_rest < <(lag_stats "$CAP/$testid.lag-at-load-end.txt")
  read -r hot_part hot_share parts_used < <(bytes_stats "$CAP/$testid.partition-bytes.txt")
  consumed_rps=$(awk -v r="$(rows_at "$rows_file" "$t_load_end")" -v d="$DURATION_S" 'BEGIN{printf "%d", r/d}')

  # --- drain and audit ---
  echo "   load done, waiting for group lag 0"
  drain_topic "$GROUP" || { echo "!! drain timeout after ${DRAIN_TIMEOUT_S}s: the audit of $testid is incomplete"; status=DRAIN_TIMEOUT; }
  local drain_s; drain_s=$(awk -v a="$(now)" -v b="$t_load_end" 'BEGIN{printf "%.0f", a-b}')
  stop_sampler
  stop_sinks
  local total uniq hot_rows inv_all pairs_all inv_hot pairs_hot
  total=$(pgq "SELECT count(*) FROM $NODEDUP_TABLE WHERE $(RANGE "$rid")" | tr -d ' \r')
  uniq=$(pgq "SELECT count(DISTINCT event_id) FROM $NODEDUP_TABLE WHERE $(RANGE "$rid")" | tr -d ' \r')
  hot_rows=$(pgq "SELECT count(*) FROM $NODEDUP_TABLE WHERE $(RANGE "$rid") AND tenant_id='$HOT_TENANT'" | tr -d ' \r')
  # Ordering audit. For each tenant, sort its events by received_at (the order in which the API got them). stored_at is the
  # time the sink's INSERT started. An event whose stored_at is EARLIER than the one just before it (in received order) was
  # stored before an older event of the same tenant = overtaken. Events in the same INSERT share stored_at, so they never count.
  IFS='|' read -r inv_all pairs_all inv_hot pairs_hot < <(pgq "
    SELECT count(*) FILTER (WHERE stored_at < prev), count(prev),
           count(*) FILTER (WHERE stored_at < prev AND tenant_id='$HOT_TENANT'), count(prev) FILTER (WHERE tenant_id='$HOT_TENANT')
    FROM (SELECT tenant_id, stored_at, lag(stored_at) OVER (PARTITION BY tenant_id ORDER BY received_at, event_id) AS prev
          FROM $NODEDUP_TABLE WHERE $(RANGE "$rid")) s")
  pgq "DELETE FROM $NODEDUP_TABLE WHERE $(RANGE "$rid")" >/dev/null
  kg --delete --group "$GROUP" >/dev/null 2>&1 || true
  stop_server

  local js; js=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  if [ -n "$js" ] && [ "$status" != DRAIN_TIMEOUT ]; then
    local ok202 missing hot_sent other_sent hot_pct
    ok202=$(jq -r '.metrics.resp_202 | (.count // .values.count // 0)' "$js")
    hot_sent=$(jq -r '.metrics.k9_hot_sent | (.count // .values.count // 0)' "$js")
    other_sent=$(jq -r '.metrics.k9_other_sent | (.count // .values.count // 0)' "$js")
    hot_pct=$(awk -v h="$hot_sent" -v o="$other_sent" 'BEGIN{ if (h+o>0) printf "%.1f", 100*h/(h+o); else print "NA" }')
    missing=$(( ok202 - uniq ))
    python3 "$ANALYZE" row --json "$js" --testid "$testid" --phase measured --rate "$rate" --rows "$uniq" \
      --ok-code 202 --fail-code 503 --p99-limit-ms 1000000 --err-limit-pct 100 \
      --var "key_mode=$mode" \
      --extra "acked=$ok202" --extra "missing=$missing" --extra "hot_sent_pct=$hot_pct" --extra "hot_rows=$hot_rows" \
      --extra "hot_part=$hot_part" --extra "hot_bytes_pct=$hot_share" --extra "parts_used=$parts_used" \
      --extra "consumed_rps=$consumed_rps" --extra "lag_max=$lag_max" --extra "lag_part=$lag_part" --extra "lag_rest=$lag_rest" \
      --extra "drain_s=$drain_s" --extra "overtaken=$inv_all" --extra "pairs=$pairs_all" --extra "hot_overtaken=$inv_hot" --extra "hot_pairs=$pairs_hot" \
      --extra "status=$status" --out "$SUMMARY"
    echo "   -> acked $ok202 (hot $hot_pct%), rows $uniq (missing $missing); drained during load $consumed_rps/s; bytes: hottest partition $hot_part has $hot_share% ($parts_used partitions over 5%); lag at load end: max $lag_max on p$lag_part, other partitions $lag_rest; drain took ${drain_s}s; overtaken $inv_all of $pairs_all (hot tenant $inv_hot of $pairs_hot)"
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
# The audit table: same columns as events, no primary key, no unique index (a plain index on event_id for the audit).
if [ "$(pgq "SELECT to_regclass('$NODEDUP_TABLE') IS NOT NULL")" != t ]; then
  pgq "CREATE TABLE $NODEDUP_TABLE (LIKE events INCLUDING DEFAULTS)" >/dev/null; CREATED_TABLE=1
  pgq "CREATE INDEX ${NODEDUP_TABLE}_event_id_idx ON $NODEDUP_TABLE (event_id)" >/dev/null
fi

{
  echo "experiment: $EXP_ID   date: $(date -Is)"
  echo "git: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo n/a), $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l) dirty files"
  echo "kernel: $(uname -r)   node: $(node -v)   k6: $(k6 version | head -1)   cpus: $(nproc)"
  echo "pinning: server=$SERVER_CPUS consumers=${SINK_CPU_LIST[*]} brokers=$KAFKA_CPUS k6=$K6_CPUS monitoring+postgres=$MON_CPUS ($MON_CONTAINERS $PG_CONTAINER)"
  echo "settings: RATES=[$RATES] MODES=[$MODES] DURATION_S=$DURATION_S HOT_SHARE=$HOT_SHARE TENANTS=$TENANTS DELAY_MS=$DELAY_MS BATCH=$BATCH REPEATS=$REPEATS CELLS=[$CELLS] TOPIC=$TOPIC PARTITIONS=$PARTITIONS"
  echo "group: session=${SESSION_TIMEOUT_MS}ms heartbeat=${HEARTBEAT_MS}ms rebalance_timeout=${REBALANCE_TIMEOUT_MS}ms assignor=roundRobin"
  echo "api: KAFKA_BROKERS=$K_BROKERS KAFKA_KEY_MODE=<tenant|event> tenants=$HOT_TENANT,k9-t1..k9-t$((TENANTS-1)) $API_ENV"
  echo "sink: $SINK_ENV"
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
} >"$OUT/env.txt"

for rep in $(seq 1 "$REPEATS"); do
  order="$CELLS"; [ $((rep % 2)) = 0 ] && order=$(tr ' ' '\n' <<<"$CELLS" | tac | tr '\n' ' ')
  for v in $order; do one_run "$v" "$rep"; done
done

echo; python3 "$ANALYZE" table "$SUMMARY"
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Summary: $SUMMARY  captures: $CAP  env: $OUT/env.txt"
