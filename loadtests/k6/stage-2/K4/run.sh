#!/usr/bin/env bash
# K4: Kill a broker under load
# ONE command runs everything:
#     ./run.sh            2 variants (acks=all, acks=1) x 3 repeats, 210 s of load each, about 40 min
#
# What one run does (this is NOT a rate sweep: one steady rate, and the cluster is damaged on a timetable):
#   t=0     steady load at RATE req/s through the real ingest-api into a fresh topic k4-events (6 partitions, RF 3, min ISR 2)
#   t=30    docker kill broker A   (a broker that is NOT the active KRaft controller)  -> "one_down"
#   t=90    docker kill broker B   (the other non-controller broker; now 2 of 3 brokers are dead) -> "two_down"
#   t=135   docker start A and B   -> "recovery"
#   t=210   load ends. Wait until all brokers are back and every partition has a full ISR again.
#   audit   start consumer-sink (new group, from the beginning), drain the topic into Postgres, count the DISTINCT event
#           ids of this run, compare with the number of 202 answers k6 received. 202 but not in Postgres = LOST.
# The two variants differ only in the API's producer settings:
#   all    KAFKA_ACKS=-1 KAFKA_IDEMPOTENT=true     (the Stage 2 default: a write needs the min ISR = 2 copies)
#   acks1  KAFKA_ACKS=1  KAFKA_IDEMPOTENT=false    (a write needs only the partition leader; idempotence needs acks=all)
# Repeat is the OUTER loop; repeat 2 runs the variants in reverse order so drift hits both the same way.
#
# Usage:  DRY_RUN=1 ./run.sh                                          print the plan, run nothing
#         VARIANTS="all" REPEATS=1 RESULTS_NAME=k4-trial ./run.sh     small trial (never quote its numbers)
#
# Env knobs: RATE, STEADY_S, HOLD1_S, HOLD2_S, RECOVER_S, VARIANTS, REPEATS, RESULTS_NAME, KILL1, KILL2 (container names,
# default: chosen at run time from the two brokers that are not the active controller), PARTITIONS, SINK_N, DRY_RUN.
set -euo pipefail

EXP_ID="K4"
# ---- 1. Settings (override from the environment) ------------------------------
RATE="${RATE:-2000}"                            # req/s. K1 knee is about 7,500, so there is room when one broker is gone
STEADY_S="${STEADY_S:-30}"                      # load before the first kill
HOLD1_S="${HOLD1_S:-60}"                        # one broker dead
HOLD2_S="${HOLD2_S:-45}"                        # two brokers dead
RECOVER_S="${RECOVER_S:-75}"                    # both started again, load still running
VARIANTS="${VARIANTS:-all acks1}"
REPEATS="${REPEATS:-3}"
RESULTS_NAME="${RESULTS_NAME:-k4-kill}"
PARTITIONS="${PARTITIONS:-6}"
TOPIC="${TOPIC:-k4-events}"
SINK_N="${SINK_N:-3}"                           # consumer-sink processes for the audit drain (ports 8001..)
DRAIN_TIMEOUT_S="${DRAIN_TIMEOUT_S:-400}"
ISR_TIMEOUT_S="${ISR_TIMEOUT_S:-180}"
KILL1="${KILL1:-}"; KILL2="${KILL2:-}"
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
PG_CONTAINER="${PG_CONTAINER:-ep-postgres}"; PG_USER="${PG_USER:-eventpulse}"; PG_DB="${PG_DB:-eventpulse}"
# Pinning: same plan as K2. server 0,6 | brokers 1,7,2,8 | k6 3,9,4,10 | monitoring + Postgres 5,11. The containers are
# moved with docker update (and put back on exit). A killed and restarted container keeps its cpuset.
SERVER_CPUS="${SERVER_CPUS:-0,6}"
KAFKA_CPUS="${KAFKA_CPUS:-1,7,2,8}"
export K6_CPUS="${K6_CPUS:-3,9,4,10}"
MON_CPUS="${MON_CPUS:-5,11}"
KEEP_PINNING="${KEEP_PINNING:-0}"
DRY_RUN="${DRY_RUN:-0}"
RETENTION_MS="${RETENTION_MS:-86400000}"
K_BROKERS="${KAFKA_BROKERS:-localhost:9092,localhost:9094,localhost:9096}"   # all three: the client can bootstrap with one dead

T1=$STEADY_S; T2=$((T1+HOLD1_S)); T3=$((T2+HOLD2_S)); DUR_S=$((T3+RECOVER_S)); DURATION="${DUR_S}s"

# The API settings that are NOT the variable, written out so a run never depends on my shell or on services/ingest-api/.env.
BASE_ENV="KAFKA_COMPRESSION=none KAFKA_LINGER_MS=0 BATCH_MAX_MESSAGES=500 BATCH_MAX_WAIT_MS=5 BATCH_MAX_BYTES=900000 BATCH_MAX_IN_FLIGHT=1 KAFKA_KEY_MODE=event"
declare -A CFG=( [all]="KAFKA_ACKS=-1 KAFKA_IDEMPOTENT=true" [acks1]="KAFKA_ACKS=1 KAFKA_IDEMPOTENT=false" )
SINK_ENV="CONSUMER_FROM_BEGINNING=true CONSUMER_ASSIGNOR=roundRobin CONSUMER_PARTITIONS_CONCURRENTLY=1 CONSUMER_MAX_BATCH_SIZE=1000 SINK_MAX_ROWS=1000 PG_POOL_MAX=5 LAG_POLL_MS=1000 LOG_ENABLED=true"
for k in KAFKA_ACKS KAFKA_IDEMPOTENT KAFKA_COMPRESSION KAFKA_LINGER_MS BATCH_MAX_MESSAGES BATCH_MAX_WAIT_MS BATCH_MAX_BYTES BATCH_MAX_IN_FLIGHT BATCH_MAX_QUEUE KAFKA_TOPIC KAFKA_KEY_MODE \
         CONSUMER_TOPIC CONSUMER_GROUP_ID CONSUMER_FROM_BEGINNING CONSUMER_ASSIGNOR CONSUMER_PARTITIONS_CONCURRENTLY CONSUMER_MAX_BATCH_SIZE \
         SINK_MAX_ROWS SINK_PROCESSING_DELAY_MS PG_POOL_MAX LAG_POLL_MS METRICS_PORT KAFKA_CLIENT_ID KAFKA_HEALTH_MIN_ISR; do unset "$k" || true; done
for v in $VARIANTS; do [ -n "${CFG[$v]+x}" ] || { echo "ERROR: unknown variant '$v'. Known: all acks1" >&2; exit 1; }; done

# Run id of one run: 8 hex chars = a4 + variant number + repeat + 00. Every event id of the run starts with it.
run_id() { printf 'a4%02x%02x00' "$1" "$2"; }       # run_id <variant number> <repeat>
variant_no() { case "$1" in all) echo 1;; acks1) echo 2;; esac; }

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-2/K4
STAGE_DIR="$(dirname "$HERE")"
RUNNER_DIR="$(dirname "$STAGE_DIR")"
ANALYZE="$RUNNER_DIR/analyze.py"
REPO="$(cd "$RUNNER_DIR/../.." && pwd)"
API_DIR="$REPO/services/ingest-api"
SINK_DIR="$REPO/services/consumer-sink"
TEXTFILE_DIR="${TEXTFILE_DIR:-$REPO/infra/compose/textfile}"
OUT="$HERE/results/$RESULTS_NAME"
CAP="$OUT/captures"; RUNS_TSV="$OUT/runs.tsv"; SUMMARY="$OUT/summary.tsv"
PHASE_PROM="$TEXTFILE_DIR/ep_k4.prom"

die() { echo "ERROR: $*" >&2; exit 1; }

# ---- 3. Plan / dry run ---------------------------------------------------------
n_var=$(wc -w <<<"$VARIANTS")
per_run_s=$(( DUR_S + 60 + ISR_TIMEOUT_S/3 + 60 ))   # load + setup + wait for full ISR + audit drain (rough)
echo "== $EXP_ID: $n_var variants x $REPEATS repeats = $((n_var*REPEATS)) measured runs, $RATE req/s, ${DUR_S}s of load each, results in $OUT =="
echo "== expected total about $(( n_var*REPEATS*per_run_s/60 )) min =="
if [ "$DRY_RUN" = "1" ]; then
  echo "  timetable per run: t=0 steady | t=${T1}s kill broker A | t=${T2}s kill broker B | t=${T3}s start both | t=${DUR_S}s load ends"
  echo "  A and B: ${KILL1:-(auto)} and ${KILL2:-(auto)}   auto = the two brokers that are not the active KRaft controller"
  for rep in $(seq 1 "$REPEATS"); do
    order="$VARIANTS"; [ $((rep % 2)) = 0 ] && order=$(tr ' ' '\n' <<<"$VARIANTS" | tac | tr '\n' ' ')
    echo "  repeat $rep order: $order   (run ids: $(for v in $order; do printf '%s ' "$(run_id "$(variant_no "$v")" "$rep")"; done))"
  done
  for v in $VARIANTS; do echo "  variant $v: ${CFG[$v]}   (KAFKA_HEALTH_MIN_ISR follows acks: 2 for all, 1 for acks1)"; done
  echo "  API common: KAFKA_BROKERS=$K_BROKERS KAFKA_TOPIC=$TOPIC $BASE_ENV"
  echo "  topic: $TOPIC deleted + recreated before every run ($PARTITIONS partitions, RF 3, min.insync.replicas=2)"
  echo "  audit: $SINK_N consumer-sinks, new group, from the beginning, timeout ${DRAIN_TIMEOUT_S}s; rows counted per run id in Postgres"
  echo "  CPU plan: server=$SERVER_CPUS brokers=$KAFKA_CPUS k6=$K6_CPUS monitoring+postgres=$MON_CPUS"
  echo "  changes the machine: docker kill/start of two brokers per run (restarted at t=${T3}s and again in the EXIT trap), docker update --cpuset-cpus (restored), textfile $PHASE_PROM (removed on exit)"
  exit 0
fi

# ---- 4. Helpers -----------------------------------------------------------------
KAFKA_CONTAINERS=""; MON_CONTAINERS=""
# The CLI tools talk to localhost:19092 INSIDE a running broker, so a dead broker in a bootstrap list can never stall them.
live_broker() { docker ps --format '{{.Names}}' | grep -E '^ep-kafka[0-9]+$' | sort | head -1; }   # any broker that is running now
kt()  { timeout "${TMO:-20}" docker exec "$(live_broker)" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 "$@"; }
kq()  { timeout "${TMO:-20}" docker exec "$(live_broker)" /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:19092 "$@"; }
kg()  { timeout "${TMO:-20}" docker exec "$(live_broker)" /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 "$@"; }
pgq() { docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -tAc "$1"; }
# Rows of one run: event ids look like <run id>-xxxx-..., so a range on the uuid uses the primary-key index.
RANGE() { echo "event_id BETWEEN '$1-0000-0000-0000-000000000000' AND '$1-ffff-ffff-ffff-ffffffffffff'"; }
now() { date +%s.%N; }
sleep_until() { local d; d=$(awk -v t="$1" -v n="$(now)" 'BEGIN{d=t-n; printf "%.3f", (d>0?d:0)}'); sleep "$d"; }
to_n() { awk -v x="$1" 'BEGIN{printf "%d", x}'; }

detect_containers() {
  KAFKA_CONTAINERS=$(docker ps -a --format '{{.Names}}' | grep -E '^ep-kafka[0-9]+$' | sort | tr '\n' ' ')
  [ "$(wc -w <<<"$KAFKA_CONTAINERS")" = 3 ] || die "expected 3 broker containers, found [$KAFKA_CONTAINERS]"
  MON_CONTAINERS=$(docker ps --format '{{.Names}} {{.Image}}' | awk '$2 ~ /prometheus|grafana|node-exporter|node_exporter|kafka-exporter/ {print $1}' | tr '\n' ' ')
}

preflight() {
  for c in k6 taskset curl docker python3 awk node jq timeout; do command -v "$c" >/dev/null || die "'$c' not found in PATH"; done
  [ -x "$RUNNER_DIR/runner.sh" ] || die "$RUNNER_DIR/runner.sh missing or not executable"
  [ -f "$ANALYZE" ] || die "$ANALYZE missing"
  curl -fsS --max-time 3 "$PROM_URL/-/ready" >/dev/null || die "Prometheus not ready at $PROM_URL"
  detect_containers
  for c in $KAFKA_CONTAINERS; do [ "$(docker inspect -f '{{.State.Health.Status}}' "$c")" = healthy ] || die "$c is not healthy"; done
  docker ps --format '{{.Names}}' | grep -qx ep-kafka-exporter || echo "WARNING: ep-kafka-exporter is not running: the dashboard will miss leaders/ISR (docker compose -f infra/compose/docker-compose.kafka.yml up -d kafka-exporter)"
  kt --list >/dev/null || die "kafka-topics.sh inside a broker failed"
  pgq "select 1" >/dev/null || die "cannot run psql in container $PG_CONTAINER"
  local p
  for p in 8000 $(seq 8001 $((8000+SINK_N))); do ss -ltn "sport = :$p" | grep -q LISTEN && die "something already listens on :$p (an old ingest-api or consumer-sink?). Stop it first"; done
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

# Every broker running and healthy again. Used before each run and in the EXIT trap, so Ctrl+C never leaves a broker dead.
start_all_brokers() {
  local c
  for c in $KAFKA_CONTAINERS; do [ "$(docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null)" = true ] || docker start "$c" >/dev/null 2>&1 || true; done
}

# Number of partitions of a topic whose ISR has as many members as the replica list AND that have a leader.
full_isr_count() {
  kt --describe --topic "$1" 2>/dev/null | awk '/Partition:/ { r=""; s=""; l=-1; for(k=1;k<=NF;k++){ if($k=="Replicas:") r=$(k+1); if($k=="Isr:") s=$(k+1); if($k=="Leader:") l=$(k+1) } if (l>=0 && split(r,a,",")==split(s,b,",")) n++ } END{print n+0}'
}
wait_full_isr() {   # wait_full_isr <topic> <timeout s>; returns 1 on timeout
  local t="$1" lim="$2" i
  for i in $(seq 1 "$lim"); do [ "$(full_isr_count "$t")" = "$PARTITIONS" ] && return 0; sleep 1; done
  return 1
}
controller_id() { kq describe --status 2>/dev/null | awk '/^LeaderId:/ {print $2}'; }

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
start_server() {   # start_server <variant>
  stop_server
  # shellcheck disable=SC2086
  ( cd "$API_DIR" && exec env KAFKA_BROKERS="$K_BROKERS" KAFKA_TOPIC="$TOPIC" $BASE_ENV ${CFG[$1]} \
      taskset -c "$SERVER_CPUS" node --env-file=.env dist/main.js ) >"$CAP/$testid.server.out" 2>&1 &
  SERVER_PID=$!
  local _
  for _ in $(seq 1 30); do curl -fsS --max-time 1 "$BASE_URL/healthz" >/dev/null 2>&1 && break; sleep 1; done
  for _ in $(seq 1 20); do curl -fsS --max-time 2 "$BASE_URL/readyz" >/dev/null 2>&1 && return 0; sleep 1; done
  die "ingest-api not ready for variant $1 (see $CAP/$testid.server.out)"
}
stop_server() {
  if [ -n "${SERVER_PID:-}" ] && kill -0 "$SERVER_PID" 2>/dev/null; then kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; fi
  SERVER_PID=""
}

declare -a SINK_PIDS=()
start_sinks() {   # start_sinks <group>
  local i; SINK_PIDS=()
  for i in $(seq 1 "$SINK_N"); do
    # shellcheck disable=SC2086
    ( cd "$SINK_DIR" && exec env KAFKA_BROKERS="$K_BROKERS" CONSUMER_TOPIC="$TOPIC" CONSUMER_GROUP_ID="$1" \
        METRICS_PORT=$((8000 + i)) KAFKA_CLIENT_ID="k4-audit-$i" $SINK_ENV \
        taskset -c "$SERVER_CPUS" node --env-file=.env dist/main.js ) >"$CAP/$testid.sink$i.out" 2>&1 &
    SINK_PIDS+=($!)
  done
}
stop_sinks() {
  local p
  for p in "${SINK_PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done
  for p in "${SINK_PIDS[@]:-}"; do [ -n "$p" ] && wait "$p" 2>/dev/null || true; done
  SINK_PIDS=()
}

# One gauge for Grafana: 0 steady, 1 one broker down, 2 two brokers down, 3 brokers started again, -1 not in a run.
set_phase() {   # set_phase <number>
  { echo "# HELP ep_k4_phase K4 run phase: 0 steady, 1 one broker down, 2 two down, 3 restarted, -1 no run."
    echo "# TYPE ep_k4_phase gauge"
    echo "ep_k4_phase{testid=\"${testid:-none}\",acks=\"${variant:-none}\"} $1"; } >"$PHASE_PROM.tmp" && mv "$PHASE_PROM.tmp" "$PHASE_PROM"
}
cleanup() { stop_sinks; stop_server; start_all_brokers; rm -f "$PHASE_PROM" "$PHASE_PROM.tmp"; restore_pinning; }
trap cleanup EXIT

# ---- 5. Per-run helpers -----------------------------------------------------------
warmup() {
  seq 1 100 | xargs -P 10 -I{} sh -c 'curl -s -o /dev/null -X POST "'"$BASE_URL"'/v1/events" \
    -H "Content-Type: application/json" -H "Authorization: Bearer $API_KEY" \
    -d "{\"event_id\":\"$(cat /proc/sys/kernel/random/uuid)\",\"type\":\"warmup\",\"occurred_at\":\"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\",\"schema_version\":1,\"payload\":{\"w\":1}}"'
}
acked_total() { curl -fsS --max-time 2 "$BASE_URL/metrics" 2>/dev/null | awk '/^ep_kafka_messages_acked_total/ {s+=$NF} END{printf "%d", s+0}'; }

# Server counters worth keeping after the run (cumulative since the server started; it restarts before every run).
scrape() {
  curl -fsS --max-time 3 "$BASE_URL/metrics" 2>/dev/null \
    | grep -E '^(ep_http_503_total|ep_kafka_send_errors_total|ep_kafka_messages_acked_total|ep_kafka_messages_failed_total|ep_batcher_rejected_total)' >"$1" || true
}
reasons() {   # reasons <file> <metric>  ->  "reason=count,reason=count" for the non-zero labels
  awk -v m="$2" '$0 ~ "^"m"[{ ]" { split($0,a,"\""); v=$NF; if (v+0>0) { l=(a[2]==""?"all":a[2]); printf "%s%s=%d", (n++?",":""), l, v } } END{ if(!n) printf "none" }' "$1"
}
# Runs in the background and with short time-outs: with two brokers dead the controller quorum is gone, and a describe
# can hang, which must not delay the next kill on the timetable.
snap() {   # snap <label>: leaders and ISR of the topic, and who the controller is, with a timestamp
  (
    local t; t=$(date +%s)
    {
      echo "=== exp=$EXP_ID testid=$testid label=$1 t=$t cmd=kafka-topics --describe / kafka-metadata-quorum describe --status"
      TMO=6 kt --describe --topic "$TOPIC" 2>&1 | grep -E 'Partition:' || echo "(describe failed or timed out)"
      TMO=6 kq describe --status 2>&1 | grep -E '^(LeaderId|LeaderEpoch|CurrentVoters)' || echo "(quorum status failed or timed out)"
    } >"$CAP/$testid.kafka.$1.txt" 2>&1
  ) &
}
record() { printf '%s\t%s\t%s\t%s\t%s\n' "$testid" "$1" "$2" "$3" "$4" >>"$RUNS_TSV"; }

run_k6() {   # run_k6 <testid> <run id> <variant>   (extra env names are forwarded to k6 by runner.sh)
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$1" SCRIPT="stage-2/K4/script.js" \
      RUN_ID="$2" ACKS="$3" RATE="$RATE" DURATION="$DURATION" T_ONE_DOWN="$T1" T_TWO_DOWN="$T2" T_RESTART="$T3" PHASE=measured \
      API_KEY="$API_KEY" BASE_URL="$BASE_URL" ROUTE="/healthz" ./runner.sh )
}

# The drain of the audit: wait until the group's total lag is 0 (every record of the topic is inserted and committed).
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

seg_counts() {   # seg_counts <json> <metric>  ->  "steady/one_down/two_down/recovery"
  local s out=""
  for s in steady one_down two_down recovery; do
    out+="$(jq -r --arg k "$2{seg:$s}" '.metrics[$k] | (.count // .values.count // 0)' "$1" 2>/dev/null || echo 0)/"
  done
  echo "${out%/}"
}

one_run() {  # one_run <variant> <rep>
  variant="$1"; local rep="$2" vno; vno=$(variant_no "$variant")
  testid="k4-${variant}-n${rep}"
  local rid; rid=$(run_id "$vno" "$rep")
  echo; echo "== variant=$variant repeat=$rep ($testid) run id $rid: ${CFG[$variant]} =="
  set_phase -1
  stop_server; stop_sinks
  start_all_brokers
  for c in $KAFKA_CONTAINERS; do for _ in $(seq 1 60); do [ "$(docker inspect -f '{{.State.Health.Status}}' "$c")" = healthy ] && break; sleep 1; done; done
  recreate_topic "$TOPIC"
  pgq "DELETE FROM events WHERE $(RANGE "$rid")" >/dev/null
  # Who is the active KRaft controller? The two kills must not hit it (killing it adds a controller election).
  local ctl; ctl=$(controller_id)
  local ka="$KILL1" kb="$KILL2"
  if [ -z "$ka" ] || [ -z "$kb" ]; then
    local cands; cands=$(for n in 1 2 3; do [ "$n" != "$ctl" ] && echo "ep-kafka$n"; done)
    ka=$(sed -n 1p <<<"$cands"); kb=$(sed -n 2p <<<"$cands")
  fi
  echo "   active controller: node ${ctl:-?}; kill order: $ka then $kb"
  start_server "$variant"; warmup
  echo "   settling 10 s"; sleep 10
  local pre="$CAP/$testid.pre.metrics" post="$CAP/$testid.post.metrics" status=ok
  scrape "$pre"
  snap "before"
  local base_acked; base_acked=$(acked_total)

  # --- the load, in the background; the controller below does the kills on the timetable ---
  run_k6 "$testid" "$rid" "$variant" &
  local k6pid=$!
  local i t0
  for i in $(seq 1 600); do   # k6 has started when the API's acked counter moves (0.1 s polls, 60 s at most)
    [ "$(( $(acked_total) - base_acked ))" -ge 20 ] && break
    kill -0 "$k6pid" 2>/dev/null || break; sleep 0.1
  done
  t0=$(now)
  echo "   load started (t=0). kills at t=${T1}s and t=${T2}s, restart at t=${T3}s, end at t=${DUR_S}s"
  set_phase 0
  sleep_until "$(awk -v t="$t0" -v d=$((T1-2)) 'BEGIN{printf "%.3f", t+d}')"; snap "t-2s"
  sleep_until "$(awk -v t="$t0" -v d="$T1" 'BEGIN{printf "%.3f", t+d}')"
  docker kill "$ka" >/dev/null; set_phase 1; record kill1 "$(to_n "$(now)")" "$(to_n "$(now)")" "$ka"
  echo "   t=$(awk -v n="$(now)" -v t="$t0" 'BEGIN{printf "%.0f", n-t}')s: killed $ka"
  sleep_until "$(awk -v t="$t0" -v d=$((T1+8)) 'BEGIN{printf "%.3f", t+d}')"; snap "one_down+8s"
  sleep_until "$(awk -v t="$t0" -v d="$T2" 'BEGIN{printf "%.3f", t+d}')"
  docker kill "$kb" >/dev/null; set_phase 2; record kill2 "$(to_n "$(now)")" "$(to_n "$(now)")" "$kb"
  echo "   t=$(awk -v n="$(now)" -v t="$t0" 'BEGIN{printf "%.0f", n-t}')s: killed $kb"
  sleep_until "$(awk -v t="$t0" -v d=$((T2+8)) 'BEGIN{printf "%.3f", t+d}')"; snap "two_down+8s"
  sleep_until "$(awk -v t="$t0" -v d="$T3" 'BEGIN{printf "%.3f", t+d}')"
  docker start "$ka" "$kb" >/dev/null; set_phase 3; record restart "$(to_n "$(now)")" "$(to_n "$(now)")" "$ka+$kb"
  echo "   t=$(awk -v n="$(now)" -v t="$t0" 'BEGIN{printf "%.0f", n-t}')s: started $ka and $kb"
  sleep_until "$(awk -v t="$t0" -v d=$((T3+30)) 'BEGIN{printf "%.3f", t+d}')"; snap "restart+30s"
  wait "$k6pid" || { status=FAILED; echo "!! $testid: k6 failed, see its manifest.md"; }
  sleep 6
  scrape "$post"
  record measured "$(to_n "$t0")" "$(date +%s)" "$status"

  # --- recovery of the cluster, then the audit ---
  local t_isr; t_isr=$(date +%s)
  if wait_full_isr "$TOPIC" "$ISR_TIMEOUT_S"; then record isr_full "$(date +%s)" "$(date +%s)" "after $(( $(date +%s) - t_isr )) s"; else record isr_full "$(date +%s)" "$(date +%s)" "TIMEOUT"; echo "!! ISR not full after ${ISR_TIMEOUT_S}s"; fi
  snap "after_recovery"
  stop_server
  set_phase -1
  local group="k4-audit-$testid-$$" rows
  echo "   audit: draining $TOPIC into Postgres with $SINK_N consumer-sinks"
  start_sinks "$group"
  drain_topic "$group" || { echo "!! drain timeout after ${DRAIN_TIMEOUT_S}s: the audit of $testid is incomplete"; status=DRAIN_TIMEOUT; }
  rows=$(pgq "SELECT count(DISTINCT event_id) FROM events WHERE $(RANGE "$rid")" | tr -d ' \r')
  stop_sinks
  pgq "DELETE FROM events WHERE $(RANGE "$rid")" >/dev/null     # keep the table small for the next run
  kg --delete --group "$group" >/dev/null 2>&1 || true

  local js; js=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  if [ -n "$js" ] && [ "$status" != DRAIN_TIMEOUT ]; then
    python3 "$ANALYZE" row --json "$js" --testid "$testid" --phase measured --rate "$RATE" --rows "$rows" \
      --ok-code 202 --fail-code 503 --p99-limit-ms 1000000 --err-limit-pct 100 \
      --var "acks=$variant" \
      --extra "ok_by_seg=$(seg_counts "$js" resp_202)" --extra "fail_by_seg=$(seg_counts "$js" resp_503)" --extra "other_by_seg=$(seg_counts "$js" resp_other)" \
      --extra "srv_503=$(reasons "$post" ep_http_503_total)" --extra "send_err=$(reasons "$post" ep_kafka_send_errors_total)" \
      --extra "killed=$ka+$kb" --extra "controller=${ctl:-?}" --out "$SUMMARY"
    tail -1 "$SUMMARY" | awk -F'\t' '{printf "   -> achieved %s rps, p50 %s ms, p99 %s ms, dropped %s, audit: %s\n",$5,$12,$14,$16,$18}'
    echo "   -> 202 by segment (steady/one_down/two_down/recovery): $(seg_counts "$js" resp_202); 503: $(seg_counts "$js" resp_503); other: $(seg_counts "$js" resp_other); rows in Postgres: $rows"
  else
    echo "   !! no summary row for $testid (k6 failed, or the audit drain timed out)"
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
  echo "pinning: server=$SERVER_CPUS brokers=$KAFKA_CPUS k6=$K6_CPUS monitoring+postgres=$MON_CPUS ($MON_CONTAINERS $PG_CONTAINER)"
  echo "settings: RATE=$RATE STEADY_S=$STEADY_S HOLD1_S=$HOLD1_S HOLD2_S=$HOLD2_S RECOVER_S=$RECOVER_S REPEATS=$REPEATS VARIANTS=[$VARIANTS] TOPIC=$TOPIC PARTITIONS=$PARTITIONS SINK_N=$SINK_N"
  echo "api: KAFKA_BROKERS=$K_BROKERS $BASE_ENV"
  for v in $VARIANTS; do echo "variant $v: ${CFG[$v]}"; done
  echo "--- brokers ---"
  for c in $KAFKA_CONTAINERS; do docker inspect -f '{{.Name}} image={{.Config.Image}} nanocpus={{.HostConfig.NanoCpus}} memory={{.HostConfig.Memory}} cpuset={{.HostConfig.CpusetCpus}}' "$c"; done
  docker exec "$(live_broker)" sh -c 'grep -E "^(min.insync.replicas|unclean.leader.election.enable|replica.lag.time.max.ms|default.replication.factor)" /opt/kafka/config/server.properties 2>/dev/null; env | grep -E "^KAFKA_(MIN_INSYNC|UNCLEAN|DEFAULT_REPL)"' || true
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
} >"$OUT/env.txt"

for rep in $(seq 1 "$REPEATS"); do
  order="$VARIANTS"; [ $((rep % 2)) = 0 ] && order=$(tr ' ' '\n' <<<"$VARIANTS" | tac | tr '\n' ' ')
  for v in $order; do one_run "$v" "$rep"; done
done

echo; python3 "$ANALYZE" table "$SUMMARY"
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Summary: $SUMMARY  captures: $CAP  env: $OUT/env.txt"
