#!/usr/bin/env bash
# K10: Kafka unavailable and API backpressure
# ONE command runs everything:
#     ./run.sh            2 variants (unbounded queue, bounded queue) x 3 repeats, 240 s of load each, about 35 min
#
# What one run does (NOT a rate sweep: one steady rate, and the cluster is taken away on a timetable):
#   t=0     steady load at RATE req/s through the real ingest-api into a fresh topic k10-events (6 partitions, RF 3, min ISR 2)
#   t=30    docker kill ALL 3 brokers                      -> "down"
#   t=150   docker start all 3 brokers (API NOT restarted) -> "recovery"
#   t=240   load ends. Wait until all brokers are healthy and every partition has a full ISR again.
# The two variants differ only in the API's queue settings:
#   unbounded   BATCH_MAX_QUEUE=0   (config.ts then also removes the byte cap, the send timeout and the circuit breaker)
#   bounded     BATCH_MAX_QUEUE=10000 BATCH_MAX_QUEUE_BYTES=67108864 KAFKA_SEND_TIMEOUT_MS=5000 + breaker (3 failures, 5 s cooldown)
# Both run the API with the same Node heap cap (NODE_OPTIONS=--max-old-space-size=HEAP_MB), so "the process dies when the
# heap is full" happens within minutes and not after filling the machine's RAM. Repeat is the OUTER loop; repeat 2 runs
# the variants in reverse order so drift hits both the same way. The API is restarted before every run.
#
# Usage:  DRY_RUN=1 ./run.sh                                              print the plan, run nothing
#         VARIANTS="bounded" REPEATS=1 RESULTS_NAME=k10-trial ./run.sh    small trial (never quote its numbers)
#
# Env knobs: RATE, STEADY_S, DOWN_S, RECOVER_S, HEAP_MB, CLIENT_TIMEOUT, VARIANTS, REPEATS, RESULTS_NAME, PARTITIONS, DRY_RUN.
set -euo pipefail

EXP_ID="K10"
# ---- 1. Settings (override from the environment) ------------------------------
RATE="${RATE:-2000}"                            # req/s. K1 knee is about 7,500, so the healthy API is far from its limit
STEADY_S="${STEADY_S:-30}"                      # load while Kafka is healthy
DOWN_S="${DOWN_S:-120}"                         # all 3 brokers dead
RECOVER_S="${RECOVER_S:-90}"                    # brokers started again, load still running, API NOT restarted
HEAP_MB="${HEAP_MB:-512}"                       # Node old-space cap of the API (both variants)
CLIENT_TIMEOUT="${CLIENT_TIMEOUT:-2s}"          # how long k6 waits for one answer
VARIANTS="${VARIANTS:-unbounded bounded}"
REPEATS="${REPEATS:-3}"
RESULTS_NAME="${RESULTS_NAME:-k10-kafka-down}"
PARTITIONS="${PARTITIONS:-6}"
TOPIC="${TOPIC:-k10-events}"
ISR_TIMEOUT_S="${ISR_TIMEOUT_S:-240}"
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
# Pinning: same plan as K4. server 0,6 | brokers 1,7,2,8 | k6 3,9,4,10 | monitoring 5,11. The containers are moved with
# docker update (and put back on exit). A killed and restarted container keeps its cpuset.
SERVER_CPUS="${SERVER_CPUS:-0,6}"
KAFKA_CPUS="${KAFKA_CPUS:-1,7,2,8}"
export K6_CPUS="${K6_CPUS:-3,9,4,10}"
MON_CPUS="${MON_CPUS:-5,11}"
KEEP_PINNING="${KEEP_PINNING:-0}"
DRY_RUN="${DRY_RUN:-0}"
RETENTION_MS="${RETENTION_MS:-86400000}"
K_BROKERS="${KAFKA_BROKERS:-localhost:9092,localhost:9094,localhost:9096}"   # all three: the client can bootstrap with any

T1=$STEADY_S; T2=$((T1+DOWN_S)); DUR_S=$((T2+RECOVER_S)); DURATION="${DUR_S}s"

# The API settings that are NOT the variable, written out so a run never depends on my shell or on services/ingest-api/.env.
BASE_ENV="KAFKA_ACKS=-1 KAFKA_IDEMPOTENT=true KAFKA_COMPRESSION=none KAFKA_LINGER_MS=0 BATCH_MAX_MESSAGES=500 BATCH_MAX_WAIT_MS=5 BATCH_MAX_BYTES=900000 BATCH_MAX_IN_FLIGHT=1 KAFKA_KEY_MODE=event NODE_OPTIONS=--max-old-space-size=$HEAP_MB"
declare -A CFG=(
  [unbounded]="BATCH_MAX_QUEUE=0"
  [bounded]="BATCH_MAX_QUEUE=10000 BATCH_MAX_QUEUE_BYTES=67108864 KAFKA_SEND_TIMEOUT_MS=5000 KAFKA_BREAKER_FAILURES=3 KAFKA_BREAKER_COOLDOWN_MS=5000"
)
# Unset anything from my shell that would silently change the API (the values above are the only source).
for k in KAFKA_ACKS KAFKA_IDEMPOTENT KAFKA_COMPRESSION KAFKA_LINGER_MS BATCH_MAX_MESSAGES BATCH_MAX_WAIT_MS BATCH_MAX_BYTES BATCH_MAX_IN_FLIGHT BATCH_MAX_QUEUE \
         BATCH_MAX_QUEUE_BYTES KAFKA_SEND_TIMEOUT_MS KAFKA_BREAKER_FAILURES KAFKA_BREAKER_COOLDOWN_MS KAFKA_TOPIC KAFKA_KEY_MODE KAFKA_CLIENT_ID KAFKA_HEALTH_MIN_ISR \
         KAFKA_DELIVERY_TIMEOUT_MS NODE_OPTIONS; do unset "$k" || true; done
for v in $VARIANTS; do [ -n "${CFG[$v]+x}" ] || { echo "ERROR: unknown variant '$v'. Known: unbounded bounded" >&2; exit 1; }; done

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-2/K10
STAGE_DIR="$(dirname "$HERE")"
RUNNER_DIR="$(dirname "$STAGE_DIR")"
ANALYZE="$RUNNER_DIR/analyze.py"
REPO="$(cd "$RUNNER_DIR/../.." && pwd)"
API_DIR="$REPO/services/ingest-api"
TEXTFILE_DIR="${TEXTFILE_DIR:-$REPO/infra/compose/textfile}"
OUT="$HERE/results/$RESULTS_NAME"
CAP="$OUT/captures"; RUNS_TSV="$OUT/runs.tsv"; SUMMARY="$OUT/summary.tsv"
PHASE_PROM="$TEXTFILE_DIR/ep_k10.prom"

die() { echo "ERROR: $*" >&2; exit 1; }

# ---- 3. Plan / dry run ---------------------------------------------------------
n_var=$(wc -w <<<"$VARIANTS")
per_run_s=$(( DUR_S + 60 + ISR_TIMEOUT_S/4 + 30 ))   # load + setup + wait for brokers and full ISR + settle (rough)
echo "== $EXP_ID: $n_var variants x $REPEATS repeats = $((n_var*REPEATS)) measured runs, $RATE req/s, ${DUR_S}s of load each, results in $OUT =="
echo "== expected total about $(( n_var*REPEATS*per_run_s/60 )) min =="
if [ "$DRY_RUN" = "1" ]; then
  echo "  timetable per run: t=0 steady | t=${T1}s kill all 3 brokers | t=${T2}s start them again (API not restarted) | t=${DUR_S}s load ends"
  for rep in $(seq 1 "$REPEATS"); do
    order="$VARIANTS"; [ $((rep % 2)) = 0 ] && order=$(tr ' ' '\n' <<<"$VARIANTS" | tac | tr '\n' ' ')
    echo "  repeat $rep order: $order"
  done
  for v in $VARIANTS; do echo "  variant $v: ${CFG[$v]}"; done
  echo "  API common: KAFKA_BROKERS=$K_BROKERS KAFKA_TOPIC=$TOPIC $BASE_ENV"
  echo "  k6: open model, client timeout $CLIENT_TIMEOUT, up to 6000 VUs"
  echo "  topic: $TOPIC deleted + recreated before every run ($PARTITIONS partitions, RF 3, min.insync.replicas=2)"
  echo "  CPU plan: server=$SERVER_CPUS brokers=$KAFKA_CPUS k6=$K6_CPUS monitoring=$MON_CPUS"
  echo "  changes the machine: docker kill/start of all 3 brokers per run (started again at t=${T2}s and in the EXIT trap), docker update --cpuset-cpus (restored), textfile $PHASE_PROM (removed on exit), the API is started and stopped per run"
  exit 0
fi

# ---- 4. Helpers -----------------------------------------------------------------
KAFKA_CONTAINERS=""; MON_CONTAINERS=""
# The CLI tools talk to localhost:19092 INSIDE a running broker, so a dead broker in a bootstrap list can never stall them.
live_broker() { docker ps --format '{{.Names}}' | grep -E '^ep-kafka[0-9]+$' | sort | head -1; }   # any broker that is running now
kt()  { timeout "${TMO:-20}" docker exec "$(live_broker)" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 "$@"; }
now() { date +%s.%N; }
sleep_until() { local d; d=$(awk -v t="$1" -v n="$(now)" 'BEGIN{d=t-n; printf "%.3f", (d>0?d:0)}'); sleep "$d"; }
to_n() { awk -v x="$1" 'BEGIN{printf "%d", x}'; }

detect_containers() {
  KAFKA_CONTAINERS=$(docker ps -a --format '{{.Names}}' | grep -E '^ep-kafka[0-9]+$' | sort | tr '\n' ' ')
  [ "$(wc -w <<<"$KAFKA_CONTAINERS")" = 3 ] || die "expected 3 broker containers, found [$KAFKA_CONTAINERS]"
  MON_CONTAINERS=$(docker ps --format '{{.Names}} {{.Image}}' | awk '$2 ~ /prometheus|grafana|node-exporter|node_exporter|kafka-exporter/ {print $1}' | tr '\n' ' ')
}

preflight() {
  for c in k6 taskset curl docker python3 awk node jq timeout getconf; do command -v "$c" >/dev/null || die "'$c' not found in PATH"; done
  [ -x "$RUNNER_DIR/runner.sh" ] || die "$RUNNER_DIR/runner.sh missing or not executable"
  [ -f "$ANALYZE" ] || die "$ANALYZE missing"
  curl -fsS --max-time 3 "$PROM_URL/-/ready" >/dev/null || die "Prometheus not ready at $PROM_URL"
  detect_containers
  for c in $KAFKA_CONTAINERS; do [ "$(docker inspect -f '{{.State.Health.Status}}' "$c")" = healthy ] || die "$c is not healthy"; done
  docker ps --format '{{.Names}}' | grep -qx ep-kafka-exporter || echo "WARNING: ep-kafka-exporter is not running: the dashboard will miss leaders/ISR (docker compose -f infra/compose/docker-compose.kafka.yml up -d kafka-exporter)"
  kt --list >/dev/null || die "kafka-topics.sh inside a broker failed"
  ss -ltn "sport = :8000" | grep -q LISTEN && die "something already listens on :8000 (an old ingest-api?). Stop it first"
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

# Every broker running again. Used before each run and in the EXIT trap, so Ctrl+C never leaves a broker dead.
start_all_brokers() {
  local c
  for c in $KAFKA_CONTAINERS; do [ "$(docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null)" = true ] || docker start "$c" >/dev/null 2>&1 || true; done
}
wait_brokers_healthy() {
  local c _
  for c in $KAFKA_CONTAINERS; do for _ in $(seq 1 90); do [ "$(docker inspect -f '{{.State.Health.Status}}' "$c")" = healthy ] && break; sleep 1; done; done
}

# Number of partitions of a topic whose ISR has as many members as the replica list AND that have a leader.
full_isr_count() {
  kt --describe --topic "$1" 2>/dev/null | awk '/Partition:/ { r=""; s=""; l=-1; for(k=1;k<=NF;k++){ if($k=="Replicas:") r=$(k+1); if($k=="Isr:") s=$(k+1); if($k=="Leader:") l=$(k+1) } if (l>=0 && split(r,ra,",")==split(s,sa,",")) n++ } END{print n+0}'
}
wait_full_isr() {   # wait_full_isr <topic> <timeout s>; returns 1 on timeout
  local t="$1" lim="$2" i
  for i in $(seq 1 "$lim"); do [ "$(full_isr_count "$t")" = "$PARTITIONS" ] && return 0; sleep 1; done
  return 1
}

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
  SERVER_PID=$!   # the subshell exec()s into env -> taskset -> node, so this is the node pid
  local _
  for _ in $(seq 1 30); do curl -fsS --max-time 1 "$BASE_URL/healthz" >/dev/null 2>&1 && break; sleep 1; done
  for _ in $(seq 1 20); do curl -fsS --max-time 2 "$BASE_URL/readyz" >/dev/null 2>&1 && return 0; sleep 1; done
  die "ingest-api not ready for variant $1 (see $CAP/$testid.server.out)"
}
stop_server() {
  if [ -n "${SERVER_PID:-}" ] && kill -0 "$SERVER_PID" 2>/dev/null; then kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; fi
  SERVER_PID=""
}

# One sample per second of the API's resident memory (read from /proc, which costs nothing), so the peak and the moment of
# death survive even when the process is gone and Prometheus has nothing left to scrape. Lines: epoch rss_mb alive|dead
SAMPLER_PID=""
sampler() {   # sampler <pid> <file>
  local pid="$1" f="$2" ps rss; ps=$(getconf PAGESIZE)
  while :; do
    if [ -r "/proc/$pid/statm" ] && kill -0 "$pid" 2>/dev/null; then
      read -r _ rss _ <"/proc/$pid/statm" || rss=0
      echo "$(now) $(( rss * ps / 1048576 )) alive" >>"$f"
    else
      echo "$(now) 0 dead" >>"$f"
    fi
    sleep 1
  done
}
stop_sampler() { [ -n "${SAMPLER_PID:-}" ] && { kill "$SAMPLER_PID" 2>/dev/null || true; wait "$SAMPLER_PID" 2>/dev/null || true; }; SAMPLER_PID=""; }

# One gauge for Grafana: 0 steady, 1 all brokers down, 2 brokers started again, -1 not in a run.
set_phase() {   # set_phase <number>
  { echo "# HELP ep_k10_phase K10 run phase: 0 steady, 1 all brokers down, 2 brokers started again, -1 no run."
    echo "# TYPE ep_k10_phase gauge"
    echo "ep_k10_phase{testid=\"${testid:-none}\",variant=\"${variant:-none}\"} $1"; } >"$PHASE_PROM.tmp" && mv "$PHASE_PROM.tmp" "$PHASE_PROM"
}
cleanup() { stop_sampler; stop_server; start_all_brokers; rm -f "$PHASE_PROM" "$PHASE_PROM.tmp"; restore_pinning; }
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
    | grep -E '^(ep_http_503_total|ep_kafka_send_errors_total|ep_kafka_messages_acked_total|ep_kafka_messages_failed_total|ep_batcher_rejected_total|ep_kafka_producer_connected|ep_batcher_queue_depth|ep_kafka_breaker_state)' >"$1" || true
}
reasons() {   # reasons <file> <metric>  ->  "reason=count,reason=count" for the non-zero labels
  awk -v m="$2" '$0 ~ "^"m"[{ ]" { split($0,a,"\""); v=$NF; if (v+0>0) { l=(a[2]==""?"all":a[2]); printf "%s%s=%d", (n++?",":""), l, v } } END{ if(!n) printf "none" }' "$1"
}
sumof() { awk -v m="$2" '$0 ~ "^"m"[{ ]" {s+=$NF} END{printf "%d", s+0}' "$1"; }   # sumof <file> <metric>
record() { printf '%s\t%s\t%s\t%s\t%s\n' "$testid" "$1" "$2" "$3" "$4" >>"$RUNS_TSV"; }

run_k6() {   # run_k6 <testid> <variant>   (extra env names are forwarded to k6 by runner.sh)
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$1" SCRIPT="stage-2/K10/script.js" \
      VARIANT="$2" RATE="$RATE" DURATION="$DURATION" T_DOWN="$T1" T_RESTART="$T2" CLIENT_TIMEOUT="$CLIENT_TIMEOUT" PHASE=measured \
      API_KEY="$API_KEY" BASE_URL="$BASE_URL" ROUTE="/healthz" ./runner.sh )
}

seg_counts() {   # seg_counts <json> <metric>  ->  "steady/down/recovery"
  local s out=""
  for s in steady down recovery; do
    out+="$(jq -r --arg k "$2{seg:$s}" '.metrics[$k] | (.count // .values.count // 0)' "$1" 2>/dev/null || echo 0)/"
  done
  echo "${out%/}"
}

one_run() {  # one_run <variant> <rep>
  variant="$1"; local rep="$2"
  testid="k10-${variant}-n${rep}"
  echo; echo "== variant=$variant repeat=$rep ($testid): ${CFG[$variant]} =="
  set_phase -1
  stop_server; stop_sampler
  start_all_brokers; wait_brokers_healthy
  recreate_topic "$TOPIC"
  start_server "$variant"; warmup
  echo "   settling 10 s"; sleep 10
  local pre="$CAP/$testid.pre.metrics" post="$CAP/$testid.post.metrics" status=ok
  scrape "$pre"
  local base_acked; base_acked=$(acked_total)
  : >"$CAP/$testid.rss.txt"
  sampler "$SERVER_PID" "$CAP/$testid.rss.txt" & SAMPLER_PID=$!

  # --- the load, in the background; the controller below does the kill and the restart on the timetable ---
  run_k6 "$testid" "$variant" &
  local k6pid=$!
  local i t0
  for i in $(seq 1 600); do   # k6 has started when the API's acked counter moves (0.1 s polls, 60 s at most)
    [ "$(( $(acked_total) - base_acked ))" -ge 20 ] && break
    kill -0 "$k6pid" 2>/dev/null || break; sleep 0.1
  done
  t0=$(now)
  echo "   load started (t=0). all brokers killed at t=${T1}s, started again at t=${T2}s, end at t=${DUR_S}s"
  set_phase 0
  sleep_until "$(awk -v t="$t0" -v d="$T1" 'BEGIN{printf "%.3f", t+d}')"
  # shellcheck disable=SC2086
  docker kill $KAFKA_CONTAINERS >/dev/null; set_phase 1; record kill_all "$(to_n "$(now)")" "$(to_n "$(now)")" "$KAFKA_CONTAINERS"
  echo "   t=$(awk -v n="$(now)" -v t="$t0" 'BEGIN{printf "%.0f", n-t}')s: killed all brokers"
  sleep_until "$(awk -v t="$t0" -v d="$T2" 'BEGIN{printf "%.3f", t+d}')"
  local acked_restart; acked_restart=$(acked_total)       # 0 if the API is dead or does not answer
  # shellcheck disable=SC2086
  docker start $KAFKA_CONTAINERS >/dev/null; set_phase 2; record restart "$(to_n "$(now)")" "$(to_n "$(now)")" "$KAFKA_CONTAINERS"
  echo "   t=$(awk -v n="$(now)" -v t="$t0" 'BEGIN{printf "%.0f", n-t}')s: started all brokers (API not restarted)"
  wait "$k6pid" || { status=FAILED; echo "!! $testid: k6 failed, see its manifest.md"; }
  sleep 6
  local alive=no; kill -0 "$SERVER_PID" 2>/dev/null && alive=yes
  scrape "$post"
  stop_sampler
  record measured "$(to_n "$t0")" "$(date +%s)" "$status"
  set_phase -1

  # --- numbers that need the API (or its memory samples) ---
  local peak died="-" acked_after="n/a" connected="n/a" first_dead
  peak=$(awk '$3=="alive" && $2>m {m=$2} END{print m+0}' "$CAP/$testid.rss.txt")
  first_dead=$(awk '$3=="dead" {print $1; exit}' "$CAP/$testid.rss.txt")
  [ -n "$first_dead" ] && died=$(awk -v d="$first_dead" -v t="$t0" 'BEGIN{printf "%.0f", d-t}')
  if [ "$alive" = yes ]; then
    acked_after=$(( $(sumof "$post" ep_kafka_messages_acked_total) - acked_restart ))
    connected=$(sumof "$post" ep_kafka_producer_connected)
  fi
  grep -qiE 'heap out of memory|JavaScript heap' "$CAP/$testid.server.out" 2>/dev/null && echo "   API log: heap out of memory"
  echo "   API alive at the end: $alive; peak RSS ${peak} MB; died at t=${died}s; acks after the restart: $acked_after; producer connected: $connected"

  # --- the cluster must be whole again before the next run ---
  local t_isr; t_isr=$(date +%s)
  start_all_brokers; wait_brokers_healthy
  if wait_full_isr "$TOPIC" "$ISR_TIMEOUT_S"; then record isr_full "$(date +%s)" "$(date +%s)" "after $(( $(date +%s) - t_isr )) s"; else record isr_full "$(date +%s)" "$(date +%s)" "TIMEOUT"; echo "!! ISR not full after ${ISR_TIMEOUT_S}s"; fi
  stop_server

  local js; js=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  if [ -n "$js" ]; then
    python3 "$ANALYZE" row --json "$js" --testid "$testid" --phase measured --rate "$RATE" \
      --ok-code 202 --fail-code 503 --p99-limit-ms 1000000 --err-limit-pct 100 \
      --var "variant=$variant" \
      --extra "ok_by_seg=$(seg_counts "$js" resp_202)" --extra "fail_by_seg=$(seg_counts "$js" resp_503)" --extra "other_by_seg=$(seg_counts "$js" resp_other)" \
      --extra "api_alive=$alive" --extra "peak_rss_mb=$peak" --extra "died_at_s=$died" --extra "acks_after_restart=$acked_after" --extra "connected=$connected" \
      --extra "srv_503=$(reasons "$post" ep_http_503_total)" --extra "rejected=$(reasons "$post" ep_batcher_rejected_total)" --out "$SUMMARY"
    tail -1 "$SUMMARY" | awk -F'\t' '{printf "   -> achieved %s rps, p50 %s ms, p99 %s ms, dropped %s\n",$5,$12,$14,$16}'
    echo "   -> 202 by segment (steady/down/recovery): $(seg_counts "$js" resp_202); 503: $(seg_counts "$js" resp_503); other: $(seg_counts "$js" resp_other)"
  else
    echo "   !! no summary row for $testid (k6 failed before writing its summary)"
  fi
}

# ---- 6. Main ----------------------------------------------------------------------
mkdir -p "$CAP"
preflight
printf 'testid\tphase\tstart_epoch\tend_epoch\tstatus\n' >"$RUNS_TSV"
for c in $KAFKA_CONTAINERS; do pin_container "$c" "$KAFKA_CPUS"; done
for c in $MON_CONTAINERS; do pin_container "$c" "$MON_CPUS"; done

{
  echo "experiment: $EXP_ID   date: $(date -Is)"
  echo "git: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo n/a), $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l) dirty files"
  echo "kernel: $(uname -r)   node: $(node -v)   k6: $(k6 version | head -1)   cpus: $(nproc)"
  echo "pinning: server=$SERVER_CPUS brokers=$KAFKA_CPUS k6=$K6_CPUS monitoring=$MON_CPUS ($MON_CONTAINERS)"
  echo "settings: RATE=$RATE STEADY_S=$STEADY_S DOWN_S=$DOWN_S RECOVER_S=$RECOVER_S HEAP_MB=$HEAP_MB CLIENT_TIMEOUT=$CLIENT_TIMEOUT REPEATS=$REPEATS VARIANTS=[$VARIANTS] TOPIC=$TOPIC PARTITIONS=$PARTITIONS"
  echo "api: KAFKA_BROKERS=$K_BROKERS $BASE_ENV"
  for v in $VARIANTS; do echo "variant $v: ${CFG[$v]}"; done
  echo "--- brokers ---"
  for c in $KAFKA_CONTAINERS; do docker inspect -f '{{.Name}} image={{.Config.Image}} nanocpus={{.HostConfig.NanoCpus}} memory={{.HostConfig.Memory}} cpuset={{.HostConfig.CpusetCpus}}' "$c"; done
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
  echo "--- memory ---"; free -m | head -2
} >"$OUT/env.txt"

for rep in $(seq 1 "$REPEATS"); do
  order="$VARIANTS"; [ $((rep % 2)) = 0 ] && order=$(tr ' ' '\n' <<<"$VARIANTS" | tac | tr '\n' ' ')
  for v in $order; do one_run "$v" "$rep"; done
done

echo; python3 "$ANALYZE" table "$SUMMARY"
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Summary: $SUMMARY  captures: $CAP  env: $OUT/env.txt"
