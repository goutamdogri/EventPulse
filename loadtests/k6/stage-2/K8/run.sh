#!/usr/bin/env bash
# K8: Rebalances and group stability
# ONE command runs everything:
#     ./run.sh            8 cells (4 scenarios x 2 assignors) x 3 repeats, about 2 min each, about 50 min in total
#
# What one run does:
#   1. fresh topic k8-events (6 partitions, RF 3, min ISR 2); the real ingest-api (acks=all, idempotent) is started
#   2. the starting consumer-sinks are started in ONE new group (same group settings in every cell, see below) and wait until
#      the 6 partitions are owned
#   3. k6 sends RATE req/s for DURATION through the ingest-api (about RATE x DURATION events, all with this run's id prefix)
#   4. EVENT_AT_S seconds into the load the scenario does its thing to the group (table below)
#   5. when k6 is done, wait until the group's lag is 0 (everything read and committed)
#   6. audit in Postgres (events_nodedup, plain INSERT, no primary key): rows of this run, distinct event ids, compare with
#      the number of 202 answers. rows - distinct = events stored twice = reprocessing made visible
#
# The scenario is WHAT happens to the group, the assignor is HOW the group handles it:
#   s1  scale out   1 consumer owns all 6 partitions; at EVENT_AT_S two more consumers join
#   s2  scale in    3 consumers; at EVENT_AT_S one of them is stopped with SIGTERM (it leaves the group by itself)
#   s3  zombie      2 consumers; at EVENT_AT_S one is frozen with SIGSTOP (no heartbeats), after ZOMBIE_HOLD_S it is resumed
#   s4  slow        2 consumers; one of them gets SLOW_MS of artificial work in SLOW_CHUNKS chunks (after SLOW_SKIP chunks),
#                   which is longer than the rebalance timeout (it is also the max poll interval of this client)
#   assignor  rr = roundRobin (eager: every rebalance revokes everything), coop = cooperativeSticky (incremental)
# Repeat is the OUTER loop; repeat 2 runs the cells in reverse order so drift hits all of them the same way.
#
# Usage:  DRY_RUN=1 ./run.sh                                            print the plan, run nothing
#         CELLS="s1-coop" REPEATS=1 RESULTS_NAME=k8-trial ./run.sh      small trial (never quote its numbers)
#
# Env knobs: RATE, DURATION_S, EVENT_AT_S, ZOMBIE_HOLD_S, SLOW_MS, SLOW_CHUNKS, SLOW_SKIP, SESSION_TIMEOUT_MS, HEARTBEAT_MS,
#            REBALANCE_TIMEOUT_MS, CELLS, REPEATS, RESULTS_NAME, PARTITIONS, DRY_RUN.
set -euo pipefail

EXP_ID="K8"
# ---- 1. Settings (override from the environment) ------------------------------
RATE="${RATE:-2000}"                            # req/s; about 120,000 events in 60 s. Far below the K1 knee (about 7,500)
DURATION_S="${DURATION_S:-60}"
DURATION="${DURATION_S}s"
EVENT_AT_S="${EVENT_AT_S:-20}"                  # when the scenario acts on the group (seconds after the load starts)
ZOMBIE_HOLD_S="${ZOMBIE_HOLD_S:-15}"            # s3: how long the frozen consumer stays frozen
SLOW_MS="${SLOW_MS:-12000}"                     # s4: artificial work per slow chunk (longer than REBALANCE_TIMEOUT_MS)
SLOW_CHUNKS="${SLOW_CHUNKS:-2}"                 # s4: how many chunks are slow
SLOW_SKIP="${SLOW_SKIP:-40}"                    # s4: normal chunks before the slow ones (puts the stall in the middle of the load)
CELLS="${CELLS:-s1-rr s1-coop s2-rr s2-coop s3-rr s3-coop s4-rr s4-coop}"
REPEATS="${REPEATS:-3}"
RESULTS_NAME="${RESULTS_NAME:-k8-rebalances}"
PARTITIONS="${PARTITIONS:-6}"
TOPIC="${TOPIC:-k8-events}"
# Group timing, the same in every cell. Session 6 s / heartbeat 1 s: a silent member is dropped after 6 s (the default 45 s
# would make the experiment long). Rebalance timeout 8 s: a chunk that takes longer than that makes the member miss its
# poll deadline. Normal chunks take far less than 1 s, so only s4 goes over it.
SESSION_TIMEOUT_MS="${SESSION_TIMEOUT_MS:-6000}"
HEARTBEAT_MS="${HEARTBEAT_MS:-1000}"
REBALANCE_TIMEOUT_MS="${REBALANCE_TIMEOUT_MS:-8000}"
BASE_PORT="${BASE_PORT:-8000}"                  # consumer n listens on BASE_PORT+n (8001..8003 are scraped by Prometheus)
JOIN_TIMEOUT_S="${JOIN_TIMEOUT_S:-60}"
DRAIN_TIMEOUT_S="${DRAIN_TIMEOUT_S:-300}"
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
PG_CONTAINER="${PG_CONTAINER:-ep-postgres}"; PG_USER="${PG_USER:-eventpulse}"; PG_DB="${PG_DB:-eventpulse}"
# Pinning: server 0,6 | consumer-sinks 1 and 7 (the 3rd one shares cpu 1) | brokers 2,8,3,9 | k6 4,10 | monitoring + Postgres 5,11.
# The container cpusets are moved with docker update (and put back on exit).
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

# The API settings, written out so a run never depends on my shell or on services/ingest-api/.env. Same for every cell.
API_ENV="KAFKA_ACKS=-1 KAFKA_IDEMPOTENT=true KAFKA_COMPRESSION=none KAFKA_LINGER_MS=0 BATCH_MAX_MESSAGES=500 BATCH_MAX_WAIT_MS=5 BATCH_MAX_BYTES=900000 BATCH_MAX_IN_FLIGHT=1 KAFKA_KEY_MODE=event"
# The consumer-sink settings that are NOT the variable. One partition at a time per consumer, chunks of up to 1000 rows.
# SINK_DEDUP=false: plain INSERT into events_nodedup, so a reprocessed row is stored twice and the audit can count it.
# The two SINK_*_MS values are the retry window of a down database; they must stay below the rebalance timeout (the sink checks).
SINK_ENV="CONSUMER_FROM_BEGINNING=true CONSUMER_PARTITIONS_CONCURRENTLY=1 CONSUMER_MAX_BATCH_SIZE=1000 SINK_MAX_ROWS=1000 PG_POOL_MAX=5 LAG_POLL_MS=1000 LOG_ENABLED=true CONSUMER_SESSION_TIMEOUT_MS=$SESSION_TIMEOUT_MS CONSUMER_HEARTBEAT_INTERVAL_MS=$HEARTBEAT_MS CONSUMER_REBALANCE_TIMEOUT_MS=$REBALANCE_TIMEOUT_MS SINK_TRANSIENT_RETRY_WINDOW_MS=1000 SINK_RETRY_MAX_MS=1000 SINK_DEDUP=false SINK_NODEDUP_TABLE=$NODEDUP_TABLE DEBUG_FAULTS=true"
for k in KAFKA_ACKS KAFKA_IDEMPOTENT KAFKA_COMPRESSION KAFKA_LINGER_MS BATCH_MAX_MESSAGES BATCH_MAX_WAIT_MS BATCH_MAX_BYTES BATCH_MAX_IN_FLIGHT BATCH_MAX_QUEUE KAFKA_TOPIC KAFKA_KEY_MODE \
         CONSUMER_TOPIC CONSUMER_GROUP_ID CONSUMER_FROM_BEGINNING CONSUMER_ASSIGNOR CONSUMER_PARTITIONS_CONCURRENTLY CONSUMER_MAX_BATCH_SIZE \
         CONSUMER_SESSION_TIMEOUT_MS CONSUMER_HEARTBEAT_INTERVAL_MS CONSUMER_REBALANCE_TIMEOUT_MS SINK_MAX_ROWS SINK_PROCESSING_DELAY_MS \
         SINK_PROCESSING_DELAY_CHUNKS SINK_PROCESSING_DELAY_SKIP_CHUNKS SINK_TRANSIENT_RETRY_WINDOW_MS SINK_RETRY_MAX_MS SINK_DEDUP SINK_NODEDUP_TABLE \
         CRASH_AT CRASH_AFTER_BATCHES DEBUG_FAULTS PG_POOL_MAX LAG_POLL_MS METRICS_PORT KAFKA_CLIENT_ID KAFKA_HEALTH_MIN_ISR; do unset "$k" || true; done

ALL_CELLS=(s1-rr s1-coop s2-rr s2-coop s3-rr s3-coop s4-rr s4-coop)
cell_no() { local i; for i in "${!ALL_CELLS[@]}"; do [ "${ALL_CELLS[$i]}" = "$1" ] && { echo $((i+1)); return; }; done; echo 0; }
for c in $CELLS; do [ "$(cell_no "$c")" != 0 ] || { echo "ERROR: unknown cell '$c'. Known: ${ALL_CELLS[*]}" >&2; exit 1; }; done
# Run id of one run: 8 hex chars = a8 + cell number + repeat + 00. Every event id of the run starts with it.
run_id() { printf 'a8%02x%02x00' "$1" "$2"; }       # run_id <cell number> <repeat>

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-2/K8
STAGE_DIR="$(dirname "$HERE")"
RUNNER_DIR="$(dirname "$STAGE_DIR")"
ANALYZE="$RUNNER_DIR/analyze.py"
REPO="$(cd "$RUNNER_DIR/../.." && pwd)"
API_DIR="$REPO/services/ingest-api"
SINK_DIR="$REPO/services/consumer-sink"
TEXTFILE_DIR="${TEXTFILE_DIR:-$REPO/infra/compose/textfile}"
OUT="$HERE/results/$RESULTS_NAME"
CAP="$OUT/captures"; RUNS_TSV="$OUT/runs.tsv"; SUMMARY="$OUT/summary.tsv"
PHASE_PROM="$TEXTFILE_DIR/ep_k8.prom"

die() { echo "ERROR: $*" >&2; exit 1; }

# ---- 3. Plan / dry run ---------------------------------------------------------
n_cell=$(wc -w <<<"$CELLS")
per_run_s=$(( DURATION_S + 70 ))   # setup + joins + load + (zombie / slow stall tail) + drain + audit (rough)
echo "== $EXP_ID: $n_cell cells x $REPEATS repeats = $((n_cell*REPEATS)) measured runs, $RATE req/s for ${DURATION_S}s (about $((RATE*DURATION_S)) events), results in $OUT =="
echo "== expected total about $(( n_cell*REPEATS*per_run_s/60 )) min =="
if [ "$DRY_RUN" = "1" ]; then
  echo "  per run: topic recreated -> API -> starting consumers (one group) -> k6 load -> scenario acts at t=${EVENT_AT_S}s -> drain to lag 0 -> audit in Postgres"
  echo "  s1: 1 consumer, 2 more start at t=${EVENT_AT_S}s | s2: 3 consumers, one gets SIGTERM at t=${EVENT_AT_S}s"
  echo "  s3: 2 consumers, one gets SIGSTOP at t=${EVENT_AT_S}s and SIGCONT ${ZOMBIE_HOLD_S}s later | s4: 2 consumers, one has ${SLOW_MS} ms of extra work in ${SLOW_CHUNKS} chunks after ${SLOW_SKIP} normal chunks"
  echo "  group timing: session timeout ${SESSION_TIMEOUT_MS} ms, heartbeat ${HEARTBEAT_MS} ms, rebalance timeout ${REBALANCE_TIMEOUT_MS} ms (same in every cell)"
  for rep in $(seq 1 "$REPEATS"); do
    order="$CELLS"; [ $((rep % 2)) = 0 ] && order=$(tr ' ' '\n' <<<"$CELLS" | tac | tr '\n' ' ')
    echo "  repeat $rep order: $order"
    echo "     run ids: $(for v in $order; do printf '%s ' "$(run_id "$(cell_no "$v")" "$rep")"; done)"
  done
  echo "  API: KAFKA_BROKERS=$K_BROKERS KAFKA_TOPIC=$TOPIC $API_ENV"
  echo "  sink: $SINK_ENV  (+ CONSUMER_ASSIGNOR=roundRobin|cooperativeSticky, ports $((BASE_PORT+1))..$((BASE_PORT+3)))"
  echo "  topic: $TOPIC deleted + recreated before every run ($PARTITIONS partitions, RF 3, min.insync.replicas=2)"
  echo "  CPU plan: server=$SERVER_CPUS consumers=${SINK_CPU_LIST[*]} brokers=$KAFKA_CPUS k6=$K6_CPUS monitoring+postgres=$MON_CPUS"
  echo "  changes the machine: SIGTERM / SIGSTOP / SIGCONT / SIGKILL of the consumer-sinks and the API the script started (by design),"
  echo "    docker update --cpuset-cpus (restored), table $NODEDUP_TABLE + index in Postgres (created if missing, dropped on exit),"
  echo "    rows of each run deleted from $NODEDUP_TABLE, topic $TOPIC recreated, consumer groups k8-* deleted, textfile $PHASE_PROM (removed on exit)"
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
# A metric of consumer n without labels, from its own /metrics (0 if it does not answer; --max-time because a SIGSTOPped one never answers)
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
  pgq "select 1" >/dev/null || die "cannot run psql in container $PG_CONTAINER"
  local p
  for p in 8000 $((BASE_PORT+1)) $((BASE_PORT+2)) $((BASE_PORT+3)); do ss -ltn "sport = :$p" | grep -q LISTEN && die "something already listens on :$p (an old ingest-api or consumer-sink?). Stop it first"; done
  [ -f "$SINK_DIR/dist/main.js" ] || die "$SINK_DIR/dist/main.js missing: run 'pnpm build' in services/consumer-sink"
  grep -q "ep_consumer_rebalance_gap_seconds" "$SINK_DIR/dist/observability/sink-stats.js" || die "consumer-sink dist is older than the K8 change (no revoke metrics): run 'pnpm --filter @eventpulse/consumer-sink build'"
  grep -q "SINK_PROCESSING_DELAY_CHUNKS" "$SINK_DIR/dist/config.js" || die "consumer-sink dist is older than the K8 change (no SINK_PROCESSING_DELAY_CHUNKS): rebuild it"
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

# --- consumers: SINK_PIDS[n] is consumer n (1..3), port BASE_PORT+n -----------------
declare -A SINK_PIDS=()
GROUP=""; ASSIGNOR=""
start_sink() {   # start_sink <n> [extra env words]
  local n="$1" extra="${2:-}" cpu="${SINK_CPU_LIST[$(( $1 - 1 ))]}"
  # shellcheck disable=SC2086
  ( cd "$SINK_DIR" && exec env KAFKA_BROKERS="$K_BROKERS" CONSUMER_TOPIC="$TOPIC" CONSUMER_GROUP_ID="$GROUP" CONSUMER_ASSIGNOR="$ASSIGNOR" \
      METRICS_PORT="$((BASE_PORT+n))" KAFKA_CLIENT_ID="k8-sink-$n" $SINK_ENV $extra \
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
# The metrics die with the process, so the counters this experiment reads are copied to a file while it still answers.
# A file is only replaced when the consumer answered (a frozen one gives nothing; the earlier copy stays).
grab_sink() {
  local n="$1" t
  t=$(curl -fsS --max-time 1 "localhost:$((BASE_PORT+n))/metrics" 2>/dev/null \
      | grep -E '^ep_consumer_(rebalances|revokes|partitions_revoked|messages_consumed|rows_inserted|commit_errors)_total |^ep_consumer_rebalance_gap_seconds_(sum|count) ' || true)
  [ -n "$t" ] && printf '%s\n' "$t" >"$CAP/$testid.sink$n.metrics"
  return 0
}
grab_all() { local n; for n in "${!SINK_PIDS[@]}"; do if alive "${SINK_PIDS[$n]}"; then grab_sink "$n"; fi; done; }
sum_grabbed() { cat "$CAP/$testid".sink*.metrics 2>/dev/null | awk -v n="$1" '$1==n {s+=$2} END{printf "%d", s+0}'; }
sum_grabbed_f() { cat "$CAP/$testid".sink*.metrics 2>/dev/null | awk -v n="$1" '$1==n {s+=$2} END{printf "%.2f", s+0}'; }
stop_sinks() {   # SIGTERM (leave the group), wait up to 10 s, then SIGKILL; a frozen one is continued first
  local n p i
  for n in "${!SINK_PIDS[@]}"; do p="${SINK_PIDS[$n]}"; kill -CONT "$p" 2>/dev/null || true; kill -TERM "$p" 2>/dev/null || true; done
  for i in $(seq 1 100); do
    local any=0; for n in "${!SINK_PIDS[@]}"; do alive "${SINK_PIDS[$n]}" && any=1; done
    [ "$any" = 0 ] && break; sleep 0.1
  done
  for n in "${!SINK_PIDS[@]}"; do p="${SINK_PIDS[$n]}"; kill -KILL "$p" 2>/dev/null || true; wait "$p" 2>/dev/null || true; done
  SINK_PIDS=()
}

# One gauge for Grafana: 0 steady (before the scenario acts), 1 the scenario is acting, 2 after it, -1 not in a run.
set_phase() {
  { echo "# HELP ep_k8_phase K8 run phase: 0 steady, 1 scenario acting, 2 after, -1 no run."
    echo "# TYPE ep_k8_phase gauge"
    echo "ep_k8_phase{testid=\"${testid:-none}\",cell=\"${cell:-none}\"} $1"; } >"$PHASE_PROM.tmp" && mv "$PHASE_PROM.tmp" "$PHASE_PROM"
}
CREATED_TABLE=0; SAMPLER_PID=""
stop_sampler() { [ -n "${SAMPLER_PID:-}" ] && { kill "$SAMPLER_PID" 2>/dev/null || true; wait "$SAMPLER_PID" 2>/dev/null || true; }; SAMPLER_PID=""; }
cleanup() {
  stop_sampler; stop_sinks; stop_server
  [ "$CREATED_TABLE" = 1 ] && pgq "DROP TABLE IF EXISTS $NODEDUP_TABLE" >/dev/null 2>&1 || true
  rm -f "$PHASE_PROM" "$PHASE_PROM.tmp"; restore_pinning
}
trap cleanup EXIT

acked_total() { curl -fsS --max-time 2 "$BASE_URL/metrics" 2>/dev/null | awk '/^ep_kafka_messages_acked_total/ {s+=$NF} END{printf "%d", s+0}'; }
record() { printf '%s\t%s\t%s\t%s\t%s\n' "$testid" "$1" "$2" "$3" "$4" >>"$RUNS_TSV"; }

# The consumption gap: how long the group read nothing. The number of rows of this run in Postgres is sampled every ~0.5 s
# (the consumers' own counters die with a killed or stopped process, the table does not). A gap = time between two samples
# where the count went up. Only the window from the first row to the end of the load counts.
sample_rows() {   # sample_rows <file> <run id>   (runs in the background until it is killed)
  while :; do echo "$(now) $(pgq "SELECT count(*) FROM $NODEDUP_TABLE WHERE $(RANGE "$2")" | tr -d ' \r')" >>"$1"; sleep 0.3; done
}
gap_stats() {   # gap_stats <file> <end epoch>  ->  "<longest gap s> <gaps over 2 s>"
  awk -v end="$2" '
    $2+0 > 0 && !started { started=1; last=$1; prev=$2; next }
    started && $1 <= end { if ($2+0 > prev+0) { g=$1-last; if (g>mx) mx=g; if (g>2) n++; last=$1; prev=$2 } }
    END { if (started) { g=end-last; if (g>mx) mx=g; if (g>2) n++ } printf "%.1f %d", mx+0, n+0 }' "$1"
}

run_k6() {   # run_k6 <testid> <run id> <cell>   (extra env names are forwarded to k6 by runner.sh)
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$1" SCRIPT="stage-2/K8/script.js" \
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
  cell="$1"; local rep="$2" cno scen asg
  scen="${cell%%-*}"; asg="${cell#*-}"; cno=$(cell_no "$cell")
  ASSIGNOR=roundRobin; [ "$asg" = coop ] && ASSIGNOR=cooperativeSticky
  testid="k8-${cell}-n${rep}"
  local rid; rid=$(run_id "$cno" "$rep")
  echo; echo "== cell=$cell (scenario=$scen assignor=$ASSIGNOR) repeat=$rep ($testid) run id $rid =="
  set_phase -1
  stop_server; stop_sinks
  for c in $KAFKA_CONTAINERS; do for _ in $(seq 1 60); do [ "$(docker inspect -f '{{.State.Health.Status}}' "$c")" = healthy ] && break; sleep 1; done; done
  recreate_topic "$TOPIC"
  pgq "DELETE FROM $NODEDUP_TABLE WHERE $(RANGE "$rid")" >/dev/null
  start_server
  echo "   settling 5 s"; sleep 5
  GROUP="k8-$testid-$$"
  local status=ok base_acked; base_acked=$(acked_total)

  # --- the consumers that exist before the load ---
  local n slow_env="SINK_PROCESSING_DELAY_MS=$SLOW_MS SINK_PROCESSING_DELAY_CHUNKS=$SLOW_CHUNKS SINK_PROCESSING_DELAY_SKIP_CHUNKS=$SLOW_SKIP"
  case "$scen" in
    s1) start_sink 1; wait_assigned 1 ;;
    s2) for n in 1 2 3; do start_sink "$n"; wait_assigned "$n"; done ;;
    s3) for n in 1 2; do start_sink "$n"; wait_assigned "$n"; done ;;
    s4) start_sink 1; wait_assigned 1; start_sink 2 "$slow_env"; wait_assigned 2 ;;
  esac
  wait_settled
  echo "   group settled: ${#SINK_PIDS[@]} consumer(s) own $(owned_total) partitions"

  # --- the load, in the background; the sampler counts rows of this run in Postgres ---
  local rows_file="$CAP/$testid.rows.txt"; : >"$rows_file"
  sample_rows "$rows_file" "$rid" & SAMPLER_PID=$!
  run_k6 "$testid" "$rid" "$cell" &
  local k6pid=$! i
  for i in $(seq 1 600); do   # k6 has started when the API's acked counter moves
    [ "$(( $(acked_total) - base_acked ))" -ge 20 ] && break
    kill -0 "$k6pid" 2>/dev/null || break; sleep 0.1
  done
  local t0 t_event="" t_end_event=""; t0=$(now); set_phase 0
  echo "   load started (t=0), $RATE req/s for ${DURATION_S}s; the scenario acts at t=${EVENT_AT_S}s"

  # --- the scenario acts on the group ---
  if [ "$scen" != s4 ]; then
    sleep "$EVENT_AT_S"
    t_event=$(now); set_phase 1
    case "$scen" in
      s1) start_sink 2; start_sink 3; record event "$(to_n "$t_event")" "$(to_n "$t_event")" "consumers 2 and 3 started" ;;
      s2) grab_sink 3; kill -TERM "${SINK_PIDS[3]}"; record event "$(to_n "$t_event")" "$(to_n "$t_event")" "consumer 3 got SIGTERM" ;;
      s3) grab_sink 2; kill -STOP "${SINK_PIDS[2]}"; record event "$(to_n "$t_event")" "$(to_n "$t_event")" "consumer 2 frozen (SIGSTOP)"
          sleep "$ZOMBIE_HOLD_S"; kill -CONT "${SINK_PIDS[2]}"; t_end_event=$(now)
          record resume "$(to_n "$t_end_event")" "$(to_n "$t_end_event")" "consumer 2 continued (SIGCONT) after ${ZOMBIE_HOLD_S}s" ;;
    esac
    echo "   t=$(awk -v n="$t_event" -v t="$t0" 'BEGIN{printf "%.1f", n-t}')s: scenario $scen applied"
    set_phase 2
  else
    echo "   s4: nothing is done to the group; consumer 2 stalls by itself in its chunk $((SLOW_SKIP+1)) ($((SLOW_MS/1000)) s of work, rebalance timeout $((REBALANCE_TIMEOUT_MS/1000)) s)"
  fi
  wait "$k6pid" || { status=FAILED; echo "!! $testid: k6 failed, see its manifest.md"; }
  local t_load_end; t_load_end=$(now)
  record measured "$(to_n "$t0")" "$(to_n "$t_load_end")" "$status"
  local gap_max gap_n; read -r gap_max gap_n < <(gap_stats "$rows_file" "$t_load_end")

  # --- drain and audit ---
  echo "   load done, waiting for group lag 0"
  drain_topic "$GROUP" || { echo "!! drain timeout after ${DRAIN_TIMEOUT_S}s: the audit of $testid is incomplete"; status=DRAIN_TIMEOUT; }
  local drain_s; drain_s=$(awk -v a="$(now)" -v b="$t_load_end" 'BEGIN{printf "%.0f", a-b}')
  stop_sampler
  grab_all
  stop_sinks
  local total uniq
  total=$(pgq "SELECT count(*) FROM $NODEDUP_TABLE WHERE $(RANGE "$rid")" | tr -d ' \r')
  uniq=$(pgq "SELECT count(DISTINCT event_id) FROM $NODEDUP_TABLE WHERE $(RANGE "$rid")" | tr -d ' \r')
  pgq "DELETE FROM $NODEDUP_TABLE WHERE $(RANGE "$rid")" >/dev/null
  kg --delete --group "$GROUP" >/dev/null 2>&1 || true
  stop_server; set_phase -1

  local js; js=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  if [ -n "$js" ] && [ "$status" != DRAIN_TIMEOUT ]; then
    local ok202 missing dup_rows
    ok202=$(jq -r '.metrics.resp_202 | (.count // .values.count // 0)' "$js")
    missing=$(( ok202 - uniq )); dup_rows=$(( total - uniq ))
    python3 "$ANALYZE" row --json "$js" --testid "$testid" --phase measured --rate "$RATE" --rows "$uniq" \
      --ok-code 202 --fail-code 503 --p99-limit-ms 1000000 --err-limit-pct 100 \
      --var "scenario=$scen" --var "assignor=$asg" \
      --extra "acked=$ok202" --extra "rows_total=$total" --extra "missing=$missing" --extra "dup_rows=$dup_rows" \
      --extra "max_gap_s=$gap_max" --extra "gaps_gt2s=$gap_n" --extra "drain_s=$drain_s" \
      --extra "assigns=$(sum_grabbed ep_consumer_rebalances_total)" --extra "revokes=$(sum_grabbed ep_consumer_revokes_total)" \
      --extra "parts_revoked=$(sum_grabbed ep_consumer_partitions_revoked_total)" --extra "cb_gap_s=$(sum_grabbed_f ep_consumer_rebalance_gap_seconds_sum)" \
      --extra "commit_err=$(sum_grabbed ep_consumer_commit_errors_total)" --extra "status=$status" --out "$SUMMARY"
    echo "   -> acked $ok202, distinct rows $uniq (missing $missing), stored twice $dup_rows, longest read gap ${gap_max}s ($gap_n over 2 s), revokes $(sum_grabbed ep_consumer_revokes_total) ($(sum_grabbed ep_consumer_partitions_revoked_total) partitions)"
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
  echo "settings: RATE=$RATE DURATION_S=$DURATION_S EVENT_AT_S=$EVENT_AT_S ZOMBIE_HOLD_S=$ZOMBIE_HOLD_S SLOW_MS=$SLOW_MS SLOW_CHUNKS=$SLOW_CHUNKS SLOW_SKIP=$SLOW_SKIP REPEATS=$REPEATS CELLS=[$CELLS] TOPIC=$TOPIC PARTITIONS=$PARTITIONS"
  echo "group: session=${SESSION_TIMEOUT_MS}ms heartbeat=${HEARTBEAT_MS}ms rebalance_timeout=${REBALANCE_TIMEOUT_MS}ms"
  echo "api: KAFKA_BROKERS=$K_BROKERS $API_ENV"
  echo "sink: $SINK_ENV  (+ CONSUMER_ASSIGNOR=<roundRobin|cooperativeSticky>; s4 adds SINK_PROCESSING_DELAY_* to consumer 2)"
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
} >"$OUT/env.txt"

for rep in $(seq 1 "$REPEATS"); do
  order="$CELLS"; [ $((rep % 2)) = 0 ] && order=$(tr ' ' '\n' <<<"$CELLS" | tac | tr '\n' ' ')
  for v in $order; do one_run "$v" "$rep"; done
done

echo; python3 "$ANALYZE" table "$SUMMARY"
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Summary: $SUMMARY  captures: $CAP  env: $OUT/env.txt"
