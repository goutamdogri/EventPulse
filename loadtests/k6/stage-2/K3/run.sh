#!/usr/bin/env bash
# K3: Partitions versus consumers
# ONE command runs everything:
#     ./run.sh            both sweeps (pg + delay), about 2.7 hours
#     ./run.sh pg         only the real-Postgres sweep
#     ./run.sh delay      only the delayed-sink sweep
#
# The question is how fast a fixed backlog is drained by N consumers of one group when the topic has P partitions.
# So this is NOT an arrival-rate test: there is no k6 during the measured part. The steps are
#   1. PREFILL (once per partition count): a topic k3-p<P> with P partitions gets about 300,000 events through the
#      real ingest-api and k6 (so the records are valid events). Every event id starts with a run id that belongs to
#      that topic, so the rows this experiment writes to Postgres can be found (and deleted) later.
#   2. DRAIN (one cell = P partitions x N consumers x repeat): delete the rows of that topic from Postgres, start N
#      consumer-sink processes with a brand new group id (reading from the beginning), and watch them drain the topic.
#      The same topic is read again and again, only the group id is new.
# Two sweeps, because Postgres could become the limit before the consumers do:
#   pg      the sink as it is (real inserts into Postgres)
#   delay   the same, plus SINK_PROCESSING_DELAY_MS=$DELAY_MS per batch, so a consumer can do at most about
#           1000 rows / DELAY_MS ms. That makes the CONSUMER the limit and the partition effect visible.
#
# Usage:  DRY_RUN=1 ./run.sh                                       print the plan, run nothing
#         PARTS="3" CONSUMERS_PG="1 3" REPEATS=1 RESULTS_NAME=k3-trial ./run.sh pg    small trial (never quote it)
set -euo pipefail

EXP_ID="K3"
MODE="${1:-${MODE:-all}}"                       # pg | delay | all
case "$MODE" in pg|delay|all) ;; *) echo "usage: ./run.sh [pg|delay|all]" >&2; exit 1;; esac

# ---- 1. Settings (override from the environment) ------------------------------
REPEATS="${REPEATS:-3}"
RESULTS_NAME="${RESULTS_NAME:-k3-sweep}"
PARTS="${PARTS:-1 3 6 12}"                      # partition counts (one topic each)
CONSUMERS_PG="${CONSUMERS_PG:-1 2 3 4 6 8 12}"  # consumers in the pg sweep
CONSUMERS_DELAY="${CONSUMERS_DELAY:-1 3 6 12}"  # consumers in the delay sweep
DELAY_MS="${DELAY_MS:-200}"                     # SINK_PROCESSING_DELAY_MS of the delay sweep
PREFILL="${PREFILL:-auto}"                      # auto = reuse a topic that already has enough records | always | never
PREFILL_RATE="${PREFILL_RATE:-4000}"
PREFILL_DURATION="${PREFILL_DURATION:-75s}"     # RATE x DURATION = 300,000 events
MIN_BACKLOG="${MIN_BACKLOG:-250000}"            # in auto mode a topic with fewer records is filled again
DRAIN_TIMEOUT_S="${DRAIN_TIMEOUT_S:-400}"       # a cell that is not drained by then is marked TIMEOUT
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
PG_CONTAINER="${PG_CONTAINER:-ep-postgres}"
PG_USER="${PG_USER:-eventpulse}"; PG_DB="${PG_DB:-eventpulse}"
# Pinning. DRAIN part (as the containers are in docker-compose): consumers 0,6,3,9,4,10 | Postgres 1,7,2,8 |
# brokers 3,9,4,10 | monitoring 5,11. PREFILL part (the K2 plan): server 0,6 | brokers 1,7,2,8 | k6 3,9,4,10 |
# monitoring + idle Postgres 5,11. The containers are moved for the prefill and put back before the drains.
CONSUMER_CPUS="${CONSUMER_CPUS:-0,6,3,9,4,10}"
SERVER_CPUS="${SERVER_CPUS:-0,6}"
KAFKA_CPUS="${KAFKA_CPUS:-1,7,2,8}"
export K6_CPUS="${K6_CPUS:-3,9,4,10}"
MON_CPUS="${MON_CPUS:-5,11}"
KAFKA_CONTAINERS="${KAFKA_CONTAINERS:-}"; MON_CONTAINERS="${MON_CONTAINERS:-}"
KEEP_PINNING="${KEEP_PINNING:-0}"
DRY_RUN="${DRY_RUN:-0}"
RETENTION_MS="${RETENTION_MS:-604800000}"       # 7 days: the topics are filled once and read for hours
K_BROKERS="${KAFKA_BROKERS:-localhost:9092,localhost:9094,localhost:9096}"
BASE_PORT=8001                                  # consumer i listens on 8000+i (Prometheus scrapes 8001-8012)

# The API during the prefill: the K2 baseline, written out so nothing depends on my shell or .env.
API_ENV="KAFKA_ACKS=-1 KAFKA_IDEMPOTENT=true KAFKA_COMPRESSION=none KAFKA_LINGER_MS=0 BATCH_MAX_MESSAGES=500 BATCH_MAX_WAIT_MS=5 BATCH_MAX_BYTES=900000 BATCH_MAX_IN_FLIGHT=1 KAFKA_KEY_MODE=event"
# The consumer-sink settings that are NOT the variable, written out for the same reason (SINK_PROCESSING_DELAY_MS is added per sweep).
SINK_ENV="CONSUMER_FROM_BEGINNING=true CONSUMER_ASSIGNOR=roundRobin CONSUMER_PARTITIONS_CONCURRENTLY=1 CONSUMER_MAX_BATCH_SIZE=1000 SINK_MAX_ROWS=1000 PG_POOL_MAX=5 LAG_POLL_MS=1000 LOG_ENABLED=true"
for k in KAFKA_ACKS KAFKA_IDEMPOTENT KAFKA_COMPRESSION KAFKA_LINGER_MS BATCH_MAX_MESSAGES BATCH_MAX_WAIT_MS BATCH_MAX_BYTES BATCH_MAX_IN_FLIGHT BATCH_MAX_QUEUE KAFKA_TOPIC \
         CONSUMER_TOPIC CONSUMER_GROUP_ID CONSUMER_FROM_BEGINNING CONSUMER_ASSIGNOR CONSUMER_PARTITIONS_CONCURRENTLY CONSUMER_MAX_BATCH_SIZE \
         SINK_MAX_ROWS SINK_PROCESSING_DELAY_MS PG_POOL_MAX LAG_POLL_MS METRICS_PORT KAFKA_CLIENT_ID CONSUMER_MAX_BYTES CONSUMER_MAX_BYTES_PER_PARTITION CONSUMER_MAX_WAIT_MS; do unset "$k" || true; done

# Each topic has its own run id (8 hex chars, the first 8 chars of every event id written to it).
run_id() { case "$1" in 1) echo a3a30001;; 3) echo a3a30003;; 6) echo a3a30006;; 12) echo a3a30012;; *) printf 'a3a3%04x' "$1";; esac; }

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-2/K3
STAGE_DIR="$(dirname "$HERE")"
RUNNER_DIR="$(dirname "$STAGE_DIR")"
REPO="$(cd "$RUNNER_DIR/../.." && pwd)"
API_DIR="$REPO/services/ingest-api"
SINK_DIR="$REPO/services/consumer-sink"
OUT="$HERE/results/$RESULTS_NAME"
CAP="$OUT/captures"; RUNS_TSV="$OUT/runs.tsv"; SUMMARY="$OUT/summary.tsv"

die() { echo "ERROR: $*" >&2; exit 1; }

# ---- 3. The cells and the plan -------------------------------------------------
declare -a CELLS=()          # "<mode>:<partitions>:<consumers>"
build_cells() {
  local m p c
  for m in pg delay; do
    [ "$MODE" = all ] || [ "$MODE" = "$m" ] || continue
    local cons="$CONSUMERS_PG"; [ "$m" = delay ] && cons="$CONSUMERS_DELAY"
    for p in $PARTS; do for c in $cons; do CELLS+=("$m:$p:$c"); done; done
  done
}
build_cells
n_cells=${#CELLS[@]}
# Rough time per measured run: pg about 60 s (start of the consumers, the drain, stop, clean-up), delay about 90 s (the drain of 1 consumer takes 300,000 rows / (1000 / DELAY_MS ms) = 60 s).
est_s=0
for cell in "${CELLS[@]}"; do [ "${cell%%:*}" = delay ] && est_s=$((est_s+90*REPEATS)) || est_s=$((est_s+60*REPEATS)); done
est_s=$((est_s + $(wc -w <<<"$PARTS")*120))          # prefill, worst case
echo "== $EXP_ID mode=$MODE: $n_cells cells x $REPEATS repeats = $((n_cells*REPEATS)) measured runs, results in $OUT =="
echo "== expected total about $((est_s/60)) min =="
if [ "$DRY_RUN" = "1" ]; then
  echo "  partition counts: $PARTS   (topics: $(for p in $PARTS; do printf 'k3-p%s ' "$p"; done))"
  echo "  consumers pg: $CONSUMERS_PG   delay: $CONSUMERS_DELAY (SINK_PROCESSING_DELAY_MS=$DELAY_MS)"
  echo "  prefill: $PREFILL, $PREFILL_RATE req/s x $PREFILL_DURATION, run ids: $(for p in $PARTS; do printf '%s ' "$(run_id "$p")"; done)"
  echo "  API during prefill: $API_ENV"
  echo "  consumer settings: $SINK_ENV (+ SINK_PROCESSING_DELAY_MS in the delay sweep)"
  echo "  cells (repeat 1 in this order, repeat 2 reversed, ...): ${CELLS[*]}"
  echo "  drain: delete the topic's rows in Postgres, new group id, consumers on :$BASE_PORT.., timeout ${DRAIN_TIMEOUT_S}s"
  echo "  CPU plan drain: consumers=$CONSUMER_CPUS (all share them) postgres/brokers/monitoring as in docker-compose"
  echo "  CPU plan prefill: server=$SERVER_CPUS brokers=$KAFKA_CPUS k6=$K6_CPUS monitoring+postgres=$MON_CPUS"
  exit 0
fi

# ---- 4. Helpers -----------------------------------------------------------------
KC1=""
kt()  { docker exec "$KC1" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 "$@"; }
topic_sum() { docker exec "$KC1" /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:19092 --topic "$1" --time -1 | awk -F: '{s+=$3} END{print s+0}'; }
topic_parts() { kt --describe --topic "$1" 2>/dev/null | awk '/PartitionCount:/ { for(i=1;i<=NF;i++) if($i=="PartitionCount:") print $(i+1) }' | head -1; }
pgq() { docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -tAc "$1"; }
# The rows of one topic: event ids of the form <run id>-xxxx-..., so a range on the uuid finds them with the index.
RANGE() { local r; r=$(run_id "$1"); echo "event_id BETWEEN '$r-0000-0000-0000-000000000000' AND '$r-ffff-ffff-ffff-ffffffffffff'"; }

detect_containers() {
  [ -n "$KAFKA_CONTAINERS" ] || KAFKA_CONTAINERS=$(docker ps --format '{{.Names}}' | grep -E '^ep-kafka[0-9]+$' | sort | tr '\n' ' ' || true)
  [ "$(wc -w <<<"$KAFKA_CONTAINERS")" = 3 ] || die "expected 3 broker containers, found [$KAFKA_CONTAINERS]"
  KC1=$(awk '{print $1}' <<<"$KAFKA_CONTAINERS")
  [ -n "$MON_CONTAINERS" ] || MON_CONTAINERS=$(docker ps --format '{{.Names}} {{.Image}}' | awk '$2 ~ /prometheus|grafana|node-exporter|node_exporter/ {print $1}' | tr '\n' ' ')
}

preflight() {
  for c in k6 taskset curl docker python3 awk node; do command -v "$c" >/dev/null || die "'$c' not found in PATH"; done
  [ -x "$RUNNER_DIR/runner.sh" ] || die "$RUNNER_DIR/runner.sh missing or not executable"
  [ -f "$STAGE_DIR/ingest-kafka.js" ] || die "$STAGE_DIR/ingest-kafka.js missing (the prefill load script)"
  curl -fsS --max-time 3 "$PROM_URL/-/ready" >/dev/null || die "Prometheus not ready at $PROM_URL"
  detect_containers
  for c in $KAFKA_CONTAINERS; do [ "$(docker inspect -f '{{.State.Health.Status}}' "$c")" = healthy ] || die "$c is not healthy"; done
  kt --list >/dev/null || die "kafka-topics.sh inside $KC1 failed"
  pgq "select 1" >/dev/null || die "cannot run psql in container $PG_CONTAINER"
  local p
  for p in $(seq 8001 8012) 8000; do ss -ltn "sport = :$p" | grep -q LISTEN && die "something already listens on :$p (an old ingest-api or consumer-sink?). Stop it first"; done
  [ -f "$SINK_DIR/dist/main.js" ] || die "$SINK_DIR/dist/main.js missing: run 'pnpm build' in services/consumer-sink"
  [ -f "$SINK_DIR/.env" ] || die "$SINK_DIR/.env missing"
  [ -f "$API_DIR/dist/main.js" ] || die "$API_DIR/dist/main.js missing: run 'pnpm build' in services/ingest-api"
  [ -f "$API_DIR/.env" ] || die "$API_DIR/.env missing"
  if [ -z "${API_KEY:-}" ]; then
    API_KEY=$(grep -E '^API_KEYS=' "$API_DIR/.env" 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"' | cut -d, -f1 | cut -d: -f1 || true)
    [ -n "$API_KEY" ] || die "API_KEY not set and not found in $API_DIR/.env (API_KEYS=key:tenant)"
  fi
  export API_KEY
  # Prometheus only scrapes 8001-8012 if infra/compose/prometheus-config.yml has all of them (a hint, not a blocker).
  curl -fsS "$PROM_URL/api/v1/targets" 2>/dev/null | grep -q 'host.docker.internal:8012' || echo "WARNING: Prometheus does not list host.docker.internal:8012 as a target: the dashboard will miss consumers 9-12"
}

declare -A OLD_CPUSET=()
pin_container() {
  local c="$1" cpus="$2"
  OLD_CPUSET["$c"]="$(docker inspect -f '{{.HostConfig.CpusetCpus}}' "$c")"
  docker update --cpuset-cpus "$cpus" "$c" >/dev/null && echo "pinned $c -> $cpus (was '${OLD_CPUSET[$c]}')"
}
restore_pinning() {
  [ "$KEEP_PINNING" = 1 ] && return 0
  local c old
  for c in "${!OLD_CPUSET[@]}"; do
    old="${OLD_CPUSET[$c]}"; [ -n "$old" ] || old="0-$(( $(nproc) - 1 ))"
    docker update --cpuset-cpus "$old" "$c" >/dev/null 2>&1 || true
  done
  OLD_CPUSET=()
}

recreate_topic() {   # recreate_topic <topic> <partitions>
  local t="$1" parts="$2" i ok
  kt --delete --topic "$t" >/dev/null 2>&1 || true
  for i in $(seq 1 60); do kt --list 2>/dev/null | grep -qx "$t" || break; sleep 1; done
  for i in $(seq 1 30); do
    kt --create --topic "$t" --partitions "$parts" --replication-factor 3 \
       --config min.insync.replicas=2 --config retention.ms="$RETENTION_MS" >/dev/null 2>&1 && break
    [ "$i" = 30 ] && die "could not create topic $t"; sleep 1
  done
  for i in $(seq 1 30); do
    ok=$(kt --describe --topic "$t" 2>/dev/null | awk '/Partition:/ { for(k=1;k<=NF;k++){ if($k=="Replicas:") r=$(k+1); if($k=="Isr:") s=$(k+1) } if (split(r,a,",")==split(s,b,",")) n++ } END{print n+0}')
    [ "$ok" = "$parts" ] && return 0; sleep 1
  done
  die "topic $t: not all partitions have a full ISR after 30 s"
}

# ---- 5. ingest-api (only during the prefill) ------------------------------------
SERVER_PID=""
start_server() {   # start_server <topic>
  stop_server
  # shellcheck disable=SC2086
  ( cd "$API_DIR" && exec env KAFKA_BROKERS="$K_BROKERS" KAFKA_TOPIC="$1" $API_ENV \
      taskset -c "$SERVER_CPUS" node --env-file=.env dist/main.js ) >"$OUT/server.out" 2>&1 &
  SERVER_PID=$!
  local _
  for _ in $(seq 1 30); do curl -fsS --max-time 1 "$BASE_URL/healthz" >/dev/null 2>&1 && break; sleep 1; done
  for _ in $(seq 1 20); do curl -fsS --max-time 2 "$BASE_URL/readyz" >/dev/null 2>&1 && return 0; sleep 1; done
  die "ingest-api not ready for topic $1 (see $OUT/server.out)"
}
stop_server() {
  if [ -n "${SERVER_PID:-}" ] && kill -0 "$SERVER_PID" 2>/dev/null; then kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; fi
  SERVER_PID=""
}

# ---- 6. consumer-sink processes (the variable of the drain) ---------------------
declare -a SINK_PIDS=()
start_consumers() {   # start_consumers <topic> <group> <n> <delay_ms>
  local topic="$1" gid="$2" n="$3" delay="$4" i
  SINK_PIDS=()
  for i in $(seq 1 "$n"); do
    # shellcheck disable=SC2086
    ( cd "$SINK_DIR" && exec env KAFKA_BROKERS="$K_BROKERS" CONSUMER_TOPIC="$topic" CONSUMER_GROUP_ID="$gid" \
        METRICS_PORT=$((BASE_PORT + i - 1)) KAFKA_CLIENT_ID="k3-c$i" SINK_PROCESSING_DELAY_MS="$delay" $SINK_ENV \
        taskset -c "$CONSUMER_CPUS" node --env-file=.env dist/main.js ) >"$CAP/$testid-c$i.out" 2>&1 &
    SINK_PIDS+=($!)
  done
}
stop_consumers() {
  local p
  for p in "${SINK_PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done   # SIGTERM: the current chunk finishes and commits
  for p in "${SINK_PIDS[@]:-}"; do [ -n "$p" ] && wait "$p" 2>/dev/null || true; done
  SINK_PIDS=()
}
cleanup() { stop_consumers; stop_server; restore_pinning; }
trap cleanup EXIT

# Watches the group while it drains: every 0.5 s it reads /metrics of every consumer and writes one line to a samples
# file. It prints key=value lines at the end. All numbers come from the consumers' own counters (rows inserted, messages
# consumed, partitions assigned, rebalances), not from Postgres, so the watching adds no load on the database.
# "stable" = every consumer answers, the group owns all P partitions, and the rebalance counter did not move for 1 s.
# rate_window = rows inserted between "stable" and "drained" / that time; rate_all = rows / time since the first row.
watch_drain() {   # watch_drain <ports csv> <partitions> <backlog> <timeout s> <samples file>
  python3 -I - "$@" <<'PY'
import sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
ports = [int(x) for x in sys.argv[1].split(",")]
P, backlog, timeout, out = int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4]), sys.argv[5]
N = len(ports)
KEYS = ("ep_consumer_rows_inserted_total", "ep_consumer_messages_consumed_total",
        "ep_consumer_assigned_partitions", "ep_consumer_rebalances_total",
        "ep_consumer_duplicates_total", "ep_consumer_dlq_total")

def scrape(port):
    try:
        txt = urllib.request.urlopen("http://127.0.0.1:%d/metrics" % port, timeout=1).read().decode()
    except Exception:
        return None
    d = dict.fromkeys(KEYS, 0.0)
    for line in txt.splitlines():
        if line.startswith("#"):
            continue
        name = line.split("{")[0].split(" ")[0]
        if name in d:
            d[name] += float(line.rsplit(" ", 1)[1])
    return d

ex = ThreadPoolExecutor(N)
t0 = time.monotonic()
f = open(out, "w")
f.write("t\tready\tassigned_sum\trebalances\trows\tconsumed\tassigned_per_consumer\n")
rebal_hist, stable, first_row, end, timed_out, last, rebal_stable = [], None, None, None, 0, None, None
while True:
    ts = time.monotonic() - t0
    res = list(ex.map(scrape, ports))
    ok = [r for r in res if r]
    rows = sum(r["ep_consumer_rows_inserted_total"] for r in ok)
    consumed = sum(r["ep_consumer_messages_consumed_total"] for r in ok)
    asum = sum(r["ep_consumer_assigned_partitions"] for r in ok)
    rebal = sum(r["ep_consumer_rebalances_total"] for r in ok)
    per = ",".join(str(int(r["ep_consumer_assigned_partitions"])) if r else "-" for r in res)
    f.write("%.2f\t%d\t%d\t%d\t%d\t%d\t%s\n" % (ts, len(ok), asum, rebal, rows, consumed, per))
    f.flush()
    last = (ts, rows, per, rebal, sum(r["ep_consumer_duplicates_total"] for r in ok), sum(r["ep_consumer_dlq_total"] for r in ok))
    rebal_hist.append(rebal)
    if first_row is None and rows > 0:
        first_row = ts
    if stable is None and len(ok) == N and asum == P and len(rebal_hist) >= 3 and rebal_hist[-1] == rebal_hist[-3]:
        stable = (ts, rows)
        rebal_stable = rebal
    if consumed >= backlog:
        end = (ts, rows)
        break
    if ts > timeout:
        timed_out = 1
        end = (ts, rows)
        break
    time.sleep(max(0.0, 0.5 - (time.monotonic() - t0 - ts)))

def fmt(x):
    return "n/a" if x is None else ("%.0f" % x)
t_end, rows_end = end
rate_window = rate_all = pre_stable = window_s = None
if stable and t_end > stable[0]:
    window_s = t_end - stable[0]
    rate_window = (rows_end - stable[1]) / window_s
if first_row is not None and t_end > first_row:
    rate_all = rows_end / (t_end - first_row)
if stable and rows_end > 0:
    pre_stable = 100.0 * stable[1] / rows_end
idle = sum(1 for x in last[2].split(",") if x == "0")
print("rate_window=%s" % fmt(rate_window))
print("rate_all=%s" % fmt(rate_all))
print("window_s=%s" % ("n/a" if window_s is None else "%.1f" % window_s))
print("t_stable=%s" % ("n/a" if not stable else "%.1f" % stable[0]))
print("t_end=%.1f" % t_end)
print("rows=%d" % rows_end)
print("pre_stable_pct=%s" % ("n/a" if pre_stable is None else "%.0f" % pre_stable))
print("assigned=%s" % last[2])
print("idle=%d" % idle)
print("rebalances=%d" % last[3])
print("late_rebalances=%s" % ("n/a" if rebal_stable is None else "%d" % (last[3] - rebal_stable)))
print("dups=%d" % last[4])
print("dlq=%d" % last[5])
print("timed_out=%d" % timed_out)
PY
}

# ---- 7. Prefill: fill topic k3-p<P> once through the real API --------------------
record() { printf '%s\t%s\t%s\t%s\t%s\n' "$testid" "$1" "$2" "$3" "$4" >>"$RUNS_TSV"; }

prefill_topic() {   # prefill_topic <P>
  local p="$1" topic="k3-p$1" rid cur n t0 status=ok
  rid=$(run_id "$p")
  cur=$(topic_parts "$topic" || true); n=0; [ -n "$cur" ] && n=$(topic_sum "$topic")
  if [ "$PREFILL" = never ] || { [ "$PREFILL" = auto ] && [ "$cur" = "$p" ] && [ "$n" -ge "$MIN_BACKLOG" ]; }; then
    [ "$cur" = "$p" ] || die "PREFILL=$PREFILL but topic $topic has partitions '${cur:-none}', need $p"
    echo "== prefill $topic: reused ($n records, $cur partitions) =="; return 0
  fi
  testid="k3-prefill-p$p"
  echo; echo "== prefill $topic: $p partitions, run id $rid, $PREFILL_RATE req/s x $PREFILL_DURATION =="
  recreate_topic "$topic" "$p"; start_server "$topic"
  t0=$(date +%s)
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$testid" SCRIPT="stage-2/ingest-kafka.js" \
      RATE="$PREFILL_RATE" DURATION="$PREFILL_DURATION" RUN_ID="$rid" PAYLOAD_BYTES=500 PRE_VUS=500 MAX_VUS=3000 \
      SUMMARY_PATH="$OUT/prefill/$testid-summary.json" API_KEY="$API_KEY" BASE_URL="$BASE_URL" ROUTE="/healthz" ./runner.sh ) || { status=FAILED; echo "!! prefill $topic failed"; }
  sleep 6
  stop_server
  record prefill "$t0" "$(date +%s)" "$status"
  n=$(topic_sum "$topic"); echo "   -> $topic holds $n records"
  [ "$status" = ok ] && [ "$n" -ge "$MIN_BACKLOG" ] || die "prefill of $topic failed or too small ($n records)"
  printf '%s\t%s\t%s\n' "$topic" "$rid" "$n" >>"$OUT/prefill/topics.tsv"
}

# ---- 8. One drain cell -----------------------------------------------------------
one_run() {   # one_run <mode> <partitions> <consumers> <repeat>
  local mode="$1" p="$2" n="$3" rep="$4" topic="k3-p$2" delay=0 status=ok t0 backlog gid ports i
  [ "$mode" = delay ] && delay="$DELAY_MS"
  testid="k3-$mode-p$p-c$n-n$rep"
  backlog=$(topic_sum "$topic")
  echo; echo "== $testid: $n consumers on $p partitions, backlog $backlog, delay ${delay} ms =="
  # Fresh start: remove the rows of this topic (only its own run id), then vacuum so dead rows do not slow the next insert.
  pgq "DELETE FROM events WHERE $(RANGE "$p")" >/dev/null
  docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -qc "VACUUM events" >/dev/null 2>&1 || true
  sleep 3
  gid="k3-$testid-$(date +%s)"
  ports=$(seq -s, "$BASE_PORT" $((BASE_PORT + n - 1)))
  t0=$(date +%s)
  start_consumers "$topic" "$gid" "$n" "$delay"
  local -A R=()
  local k v
  while IFS== read -r k v; do R["$k"]="$v"; done < <(watch_drain "$ports" "$p" "$backlog" "$DRAIN_TIMEOUT_S" "$CAP/$testid.samples.tsv")
  stop_consumers
  [ "${R[timed_out]:-1}" = 0 ] || status=TIMEOUT
  local rows_db audit=ok
  rows_db=$(pgq "SELECT count(*) FROM events WHERE $(RANGE "$p")" | tr -d ' \r')
  [ "$rows_db" = "$backlog" ] || audit="MISMATCH($rows_db)"
  [ "${R[dups]:-0}" = 0 ] && [ "${R[dlq]:-0}" = 0 ] || audit="$audit+dup/dlq"
  record measured "$t0" "$(date +%s)" "$status"
  [ -f "$SUMMARY" ] || printf 'testid\tmode\tpartitions\tconsumers\trepeat\tbacklog\trows_in_db\trate_window\trate_all\twindow_s\tt_stable_s\tt_end_s\tpre_stable_pct\tidle_consumers\tassigned_per_consumer\trebalances\taudit\tstatus\tlate_rebalances\n' >"$SUMMARY"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$testid" "$mode" "$p" "$n" "$rep" "$backlog" "$rows_db" \
    "${R[rate_window]:-n/a}" "${R[rate_all]:-n/a}" "${R[window_s]:-n/a}" "${R[t_stable]:-n/a}" "${R[t_end]:-n/a}" "${R[pre_stable_pct]:-n/a}" \
    "${R[idle]:-n/a}" "${R[assigned]:-n/a}" "${R[rebalances]:-n/a}" "$audit" "$status" "${R[late_rebalances]:-n/a}" >>"$SUMMARY"
  echo "   -> rate (stable window) ${R[rate_window]:-n/a} rows/s, whole drain ${R[rate_all]:-n/a} rows/s, idle consumers ${R[idle]:-n/a}, assigned [${R[assigned]:-n/a}], rebalances ${R[rebalances]:-n/a} (${R[late_rebalances]:-n/a} after stable), audit $audit, $status"
  # Remove the finished group so the broker does not collect hundreds of dead groups.
  docker exec "$KC1" /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --delete --group "$gid" >/dev/null 2>&1 || true
  sleep 3
}

# Median / min / max of rate_window per cell, over the repeats (a quick look; the real analysis comes after the run).
print_table() {
  echo; echo "== per cell: median / min / max of rate_window (rows/s) over the repeats =="
  printf 'mode\tpartitions\tconsumers\truns\tmedian\tmin\tmax\tidle(last)\n'
  tail -n +2 "$SUMMARY" | awk -F'\t' '$18=="ok" && $8!="n/a" {print $2"\t"$3"\t"$4}' | sort -u -k1,1 -k2,2n -k3,3n | while IFS=$'\t' read -r m p c; do
    local vals cnt med mn mx idle
    vals=$(tail -n +2 "$SUMMARY" | awk -F'\t' -v m="$m" -v p="$p" -v c="$c" '$2==m && $3==p && $4==c && $18=="ok" && $8!="n/a" {print $8}' | sort -n)
    cnt=$(wc -l <<<"$vals"); med=$(sed -n "$(( (cnt+1)/2 ))p" <<<"$vals"); mn=$(head -1 <<<"$vals"); mx=$(tail -1 <<<"$vals")
    idle=$(tail -n +2 "$SUMMARY" | awk -F'\t' -v m="$m" -v p="$p" -v c="$c" '$2==m && $3==p && $4==c {x=$14} END{print x}')
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$m" "$p" "$c" "$cnt" "$med" "$mn" "$mx" "$idle"
  done
}

# ---- 9. Main ----------------------------------------------------------------------
mkdir -p "$CAP" "$OUT/prefill"
preflight
printf 'testid\tphase\tstart_epoch\tend_epoch\tstatus\n' >"$RUNS_TSV"

# Prefill with the K2 pinning, then put every container back before the drains.
for c in $KAFKA_CONTAINERS; do pin_container "$c" "$KAFKA_CPUS"; done
pin_container "$PG_CONTAINER" "$MON_CPUS"
for c in $MON_CONTAINERS; do pin_container "$c" "$MON_CPUS"; done
for p in $PARTS; do prefill_topic "$p"; done
restore_pinning

{
  echo "experiment: $EXP_ID   date: $(date -Is)   mode: $MODE"
  echo "git: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo n/a), $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l) dirty files"
  echo "kernel: $(uname -r)   node: $(node -v)   k6: $(k6 version | head -1)   cpus: $(nproc)"
  echo "settings: REPEATS=$REPEATS PARTS=[$PARTS] CONSUMERS_PG=[$CONSUMERS_PG] CONSUMERS_DELAY=[$CONSUMERS_DELAY] DELAY_MS=$DELAY_MS PREFILL=$PREFILL DRAIN_TIMEOUT_S=$DRAIN_TIMEOUT_S"
  echo "prefill: $PREFILL_RATE req/s x $PREFILL_DURATION   topics (topic, run id, records):"; cat "$OUT/prefill/topics.tsv" 2>/dev/null || true
  for p in $PARTS; do echo "topic k3-p$p now holds $(topic_sum "k3-p$p") records"; done
  echo "api (prefill): $API_ENV"
  echo "sink: $SINK_ENV   (SINK_PROCESSING_DELAY_MS=$DELAY_MS in the delay sweep, 0 in pg)"
  echo "pinning of the drain: consumers=$CONSUMER_CPUS (all share these CPUs); containers now:"
  for c in $KAFKA_CONTAINERS $PG_CONTAINER $MON_CONTAINERS; do docker inspect -f '  {{.Name}} cpuset={{.HostConfig.CpusetCpus}} nanocpus={{.HostConfig.NanoCpus}} memory={{.HostConfig.Memory}}' "$c"; done
  echo "--- broker image ---"; docker inspect -f '{{.Config.Image}}' "$KC1"
  echo "--- postgres ---"; docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -tAc "select version()" | head -1
  for s in shared_buffers synchronous_commit max_connections wal_level fsync full_page_writes; do echo "$s=$(pgq "show $s")"; done
  echo "events rows now: $(pgq 'select count(*) from events')"
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
} >"$OUT/env.txt"

echo; echo "== drains start =="
for rep in $(seq 1 "$REPEATS"); do
  order=("${CELLS[@]}")
  if [ $((rep % 2)) = 0 ]; then order=(); for ((i=${#CELLS[@]}-1; i>=0; i--)); do order+=("${CELLS[$i]}"); done; fi
  for cell in "${order[@]}"; do IFS=: read -r m p c <<<"$cell"; one_run "$m" "$p" "$c" "$rep"; done
done

print_table
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Summary: $SUMMARY  samples: $CAP/*.samples.tsv  env: $OUT/env.txt"
