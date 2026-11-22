#!/usr/bin/env bash
# K2: Producer settings sweep
# ONE command runs the whole sweep:
#     ./run.sh            16 configs x 2 repeats, 30 s each at RATE (default 4000 req/s), about 50 min
#
# The sweep changes ONE group of settings at a time around a baseline (one-factor-at-a-time, not a full grid):
#   base   wait 5 ms, 500 messages, acks=all(-1), idempotence on, compression none, linger.ms 0
#   A      batcher grid: BATCH_MAX_WAIT_MS 1/5/20 x BATCH_MAX_MESSAGES 50/500/5000 (5 ms/500 is the baseline itself)
#   B      durability: acks=1 (idempotence off), acks=0 (idempotence off), acks=all with idempotence off
#   C      compression: gzip, snappy, lz4, zstd (acks=all, idempotence on)
#
# What a run does (same method as K1):
#   1. preflight   - tools, Prometheus, brokers healthy, ingest-api built, consumer-sink OFF
#   2. pinning     - docker update --cpuset-cpus (same plan as K1), restored on exit
#   3. env.txt     - versions, the settings of every config, pinning, git commit
#   4. per config x repeat: stop server, delete + recreate the topic, start server WITH that config, warm up,
#                  snapshot server counters, k6 via runner.sh, snapshot again, audit, one summary row
#   Repeat is the OUTER loop; repeat 1 runs the configs in table order, repeat 2 in reverse order (drift hits all
#   configs about the same way).
#
# Usage:  DRY_RUN=1 ./run.sh                                         (print the plan, run nothing)
#         CONFIGS="base lz4" REPEATS=1 RESULTS_NAME=k2-trial ./run.sh  (small trial; never quote its numbers)
#         PAYLOAD_MODE=x CONFIGS="base gzip zstd" RESULTS_NAME=k2-xpad ./run.sh   (unrealistic best case for compression)
#
# Settings of the API that are NOT part of the sweep are written out below for every config, so a run never depends on
# what happens to be exported in my shell or written in services/ingest-api/.env. Ambient KAFKA_*/BATCH_* values are ignored.
set -euo pipefail

EXP_ID="K2"
# ---- 1. Settings (override from the environment) ------------------------------
RATE="${RATE:-4000}"                            # one offered rate, below the K1 knee (about 7,500) with headroom
DURATION="${DURATION:-30s}"
REPEATS="${REPEATS:-2}"
RESULTS_NAME="${RESULTS_NAME:-k2-sweep}"
PAYLOAD_MODE="${PAYLOAD_MODE:-random}"          # random | x  (read by script.js)
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
SERVER_CPUS="${SERVER_CPUS:-0,6}"
KAFKA_CPUS="${KAFKA_CPUS:-1,7,2,8}"
export K6_CPUS="${K6_CPUS:-3,9,4,10}"
MON_CPUS="${MON_CPUS:-5,11}"
PG_CONTAINER="${PG_CONTAINER:-}"; MON_CONTAINERS="${MON_CONTAINERS:-}"; KAFKA_CONTAINERS="${KAFKA_CONTAINERS:-}"
KEEP_PINNING="${KEEP_PINNING:-0}"
DRY_RUN="${DRY_RUN:-0}"
OVERHEAD_S=60                                   # restart + topic recreate + warm-up + settle + snapshots per run (rough)
TOPIC="${TOPIC:-events.raw}"; PARTITIONS="${PARTITIONS:-6}"; RETENTION_MS="${RETENTION_MS:-86400000}"
K_BROKERS="${KAFKA_BROKERS:-localhost:9092,localhost:9094,localhost:9096}"
K_KEY_MODE="${KAFKA_KEY_MODE:-event}"           # event = spread over all 6 partitions (same reason as K1)

# The baseline, written out in full. A config below overrides some of these.
BASE_ENV="KAFKA_ACKS=-1 KAFKA_IDEMPOTENT=true KAFKA_COMPRESSION=none KAFKA_LINGER_MS=0 BATCH_MAX_MESSAGES=500 BATCH_MAX_WAIT_MS=5 BATCH_MAX_BYTES=900000 BATCH_MAX_IN_FLIGHT=1"
for k in KAFKA_ACKS KAFKA_IDEMPOTENT KAFKA_COMPRESSION KAFKA_LINGER_MS BATCH_MAX_MESSAGES BATCH_MAX_WAIT_MS BATCH_MAX_BYTES BATCH_MAX_IN_FLIGHT BATCH_MAX_QUEUE; do unset "$k" || true; done

# config name -> overrides on top of BASE_ENV. s5000 also lifts BATCH_MAX_BYTES to its hard limit (1,000,000): the
# default 900,000 B would cap a batch at about 1,800 messages of 500 B anyway, so "5000" can never be reached.
declare -A CFG=(
  [base]=""
  [w1_s50]="BATCH_MAX_WAIT_MS=1 BATCH_MAX_MESSAGES=50"
  [w1_s500]="BATCH_MAX_WAIT_MS=1 BATCH_MAX_MESSAGES=500"
  [w1_s5000]="BATCH_MAX_WAIT_MS=1 BATCH_MAX_MESSAGES=5000 BATCH_MAX_BYTES=1000000"
  [w5_s50]="BATCH_MAX_WAIT_MS=5 BATCH_MAX_MESSAGES=50"
  [w5_s5000]="BATCH_MAX_WAIT_MS=5 BATCH_MAX_MESSAGES=5000 BATCH_MAX_BYTES=1000000"
  [w20_s50]="BATCH_MAX_WAIT_MS=20 BATCH_MAX_MESSAGES=50"
  [w20_s500]="BATCH_MAX_WAIT_MS=20 BATCH_MAX_MESSAGES=500"
  [w20_s5000]="BATCH_MAX_WAIT_MS=20 BATCH_MAX_MESSAGES=5000 BATCH_MAX_BYTES=1000000"
  [acks1]="KAFKA_ACKS=1 KAFKA_IDEMPOTENT=false"
  [acks0]="KAFKA_ACKS=0 KAFKA_IDEMPOTENT=false"
  [noidem]="KAFKA_IDEMPOTENT=false"
  [gzip]="KAFKA_COMPRESSION=gzip"
  [snappy]="KAFKA_COMPRESSION=snappy"
  [lz4]="KAFKA_COMPRESSION=lz4"
  [zstd]="KAFKA_COMPRESSION=zstd"
)
ALL_CONFIGS="base w1_s50 w1_s500 w1_s5000 w5_s50 w5_s5000 w20_s50 w20_s500 w20_s5000 acks1 acks0 noidem gzip snappy lz4 zstd"
CONFIGS="${CONFIGS:-$ALL_CONFIGS}"
for c in $CONFIGS; do [ -n "${CFG[$c]+x}" ] || { echo "ERROR: unknown config '$c'. Known: $ALL_CONFIGS" >&2; exit 1; }; done

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-2/K2
STAGE_DIR="$(dirname "$HERE")"
RUNNER_DIR="$(dirname "$STAGE_DIR")"
ANALYZE="$RUNNER_DIR/analyze.py"
REPO="$(cd "$RUNNER_DIR/../.." && pwd)"
API_DIR="$REPO/services/ingest-api"
OUT="$HERE/results/$RESULTS_NAME"
CAP="$OUT/captures"; RUNS_TSV="$OUT/runs.tsv"; SUMMARY="$OUT/summary.tsv"

die() { echo "ERROR: $*" >&2; exit 1; }
to_seconds() { case "$1" in *h) echo $(( ${1%h}*3600 ));; *m) echo $(( ${1%m}*60 ));; *s) echo "${1%s}";; *) echo "$1";; esac; }

# ---- 3. Plan / dry run ---------------------------------------------------------
n_cfg=$(wc -w <<<"$CONFIGS")
total_s=$(( n_cfg*REPEATS*($(to_seconds "$DURATION")+OVERHEAD_S) ))
echo "== $EXP_ID: $n_cfg configs x $REPEATS repeats = $((n_cfg*REPEATS)) measured runs, rate $RATE req/s, $DURATION each, payload=$PAYLOAD_MODE, results in $OUT =="
echo "== expected total about $(( total_s/60 )) min =="
if [ "$DRY_RUN" = "1" ]; then
  echo "  baseline: $BASE_ENV"
  for rep in $(seq 1 "$REPEATS"); do
    order="$CONFIGS"; [ $((rep % 2)) = 0 ] && order=$(tr ' ' '\n' <<<"$CONFIGS" | tac | tr '\n' ' ')
    echo "  repeat $rep order: $order"
  done
  for c in $CONFIGS; do echo "  config $c: ${CFG[$c]:-(baseline)}"; done
  echo "  API: KAFKA_BROKERS=$K_BROKERS KAFKA_KEY_MODE=$K_KEY_MODE"
  echo "  topic: $TOPIC deleted + recreated before every run ($PARTITIONS partitions, RF 3, min.insync.replicas=2)"
  echo "  CPU plan: server=$SERVER_CPUS brokers=$KAFKA_CPUS k6=$K6_CPUS monitoring+postgres=$MON_CPUS"
  exit 0
fi

# ---- 4. Helpers (same as K1) -----------------------------------------------------
KC1=""
kt()  { docker exec "$KC1" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 "$@"; }
topic_sum() { docker exec "$KC1" /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:19092 --topic "$1" --time -1 | awk -F: '{s+=$3} END{print s+0}'; }
# Bytes of the topic's log files on broker 1. RF 3 on 3 brokers = every broker holds a full copy, so this is the whole
# topic once (compressed batches are stored as they arrive, so this is the size after compression).
topic_log_bytes() { docker exec "$KC1" sh -c "cat /var/lib/kafka/data/$1-*/*.log 2>/dev/null | wc -c" | tr -d ' \r'; }

detect_containers() {
  [ -n "$KAFKA_CONTAINERS" ] || KAFKA_CONTAINERS=$(docker ps --format '{{.Names}}' | grep -E '^ep-kafka[0-9]+$' | sort | tr '\n' ' ' || true)
  [ "$(wc -w <<<"$KAFKA_CONTAINERS")" = 3 ] || die "expected 3 broker containers, found [$KAFKA_CONTAINERS]"
  KC1=$(awk '{print $1}' <<<"$KAFKA_CONTAINERS")
  [ -n "$PG_CONTAINER" ] || PG_CONTAINER=$(docker ps --format '{{.Names}} {{.Image}}' | awk '$2 ~ /^postgres/ {print $1}' | head -1)
  [ -n "$MON_CONTAINERS" ] || MON_CONTAINERS=$(docker ps --format '{{.Names}} {{.Image}}' | awk '$2 ~ /prometheus|grafana|node-exporter|node_exporter/ {print $1}' | tr '\n' ' ')
}

preflight() {
  for c in k6 taskset curl docker python3 awk; do command -v "$c" >/dev/null || die "'$c' not found in PATH"; done
  [ -x "$RUNNER_DIR/runner.sh" ] || die "$RUNNER_DIR/runner.sh missing or not executable"
  [ -f "$ANALYZE" ] || die "$ANALYZE missing"
  curl -fsS --max-time 3 "$PROM_URL/-/ready" >/dev/null || die "Prometheus not ready at $PROM_URL"
  detect_containers
  for c in $KAFKA_CONTAINERS; do [ "$(docker inspect -f '{{.State.Health.Status}}' "$c")" = healthy ] || die "$c is not healthy"; done
  kt --list >/dev/null || die "kafka-topics.sh inside $KC1 failed"
  for p in 8001 8002 8003; do ss -ltn "sport = :$p" | grep -q LISTEN && die "something listens on :$p (consumer-sink?). Stop it: K2 measures ingest only"; done
  ss -ltn "sport = :$(sed -E 's#^[a-z]+://[^:/]+:([0-9]+).*#\1#' <<<"$BASE_URL")" | grep -q LISTEN && die "port of $BASE_URL is already in use: stop that server first (run.sh starts its own)"
  [ -f "$API_DIR/dist/main.js" ] || die "$API_DIR/dist/main.js missing: run 'pnpm build' in services/ingest-api"
  [ -f "$API_DIR/.env" ] || die "$API_DIR/.env missing"
  grep -q ep_kafka_client_tx_bytes_total "$API_DIR"/dist/observability/kafka-metrics.js || die "dist is older than K2's wire-bytes metric: run 'pnpm build' in services/ingest-api"
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

recreate_topic() {
  local t="$1" i ok
  kt --delete --topic "$t" >/dev/null 2>&1 || true
  for i in $(seq 1 60); do kt --list 2>/dev/null | grep -qx "$t" || break; sleep 1; done
  for i in $(seq 1 30); do
    kt --create --topic "$t" --partitions "$PARTITIONS" --replication-factor 3 \
       --config min.insync.replicas=2 --config retention.ms="$RETENTION_MS" >/dev/null 2>&1 && break
    [ "$i" = 30 ] && die "could not create topic $t"; sleep 1
  done
  for i in $(seq 1 30); do
    ok=$(kt --describe --topic "$t" 2>/dev/null | awk '/Partition:/ { for(k=1;k<=NF;k++){ if($k=="Replicas:") r=$(k+1); if($k=="Isr:") s=$(k+1) } if (split(r,a,",")==split(s,b,",")) n++ } END{print n+0}')
    [ "$ok" = "$PARTITIONS" ] && return 0; sleep 1
  done
  die "topic $t: not all partitions have a full ISR after 30 s"
}

SERVER_PID=""
start_server() {   # start_server <config-name>: baseline settings, then the config's overrides (later wins)
  stop_server
  # shellcheck disable=SC2086
  ( cd "$API_DIR" && exec env KAFKA_BROKERS="$K_BROKERS" KAFKA_KEY_MODE="$K_KEY_MODE" $BASE_ENV ${CFG[$1]} \
      taskset -c "$SERVER_CPUS" node --env-file=.env dist/main.js ) >"$OUT/server.out" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 30); do curl -fsS --max-time 1 "$BASE_URL/healthz" >/dev/null 2>&1 && break; sleep 1; done
  for _ in $(seq 1 20); do curl -fsS --max-time 2 "$BASE_URL/readyz" >/dev/null 2>&1 && return 0; sleep 1; done
  die "ingest-api not ready for config $1 (see $OUT/server.out)"
}
stop_server() {
  if [ -n "${SERVER_PID:-}" ] && kill -0 "$SERVER_PID" 2>/dev/null; then kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; fi
  SERVER_PID=""
}
cleanup() { stop_server; restore_pinning; }
trap cleanup EXIT

# ---- 5. Per-run helpers -----------------------------------------------------------
warmup() {
  seq 1 100 | xargs -P 10 -I{} sh -c 'curl -s -o /dev/null -X POST "'"$BASE_URL"'/v1/events" \
    -H "Content-Type: application/json" -H "Authorization: Bearer $API_KEY" \
    -d "{\"event_id\":\"$(cat /proc/sys/kernel/random/uuid)\",\"type\":\"warmup\",\"occurred_at\":\"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\",\"schema_version\":1,\"payload\":{\"w\":1}}"'
}

# Server counters (cumulative since the server started; it restarts before every run). One small grep, never the whole page.
scrape() {   # scrape <file>
  curl -fsS --max-time 3 "$BASE_URL/metrics" 2>/dev/null \
    | grep -E '^(ep_batcher_batch_size_messages_(sum|count)|ep_batcher_batch_size_bytes_(sum|count)|ep_kafka_send_seconds_(sum|count)|ep_kafka_client_tx_bytes_total|process_cpu_(user|system)_seconds_total) ' >"$1" || true
}
mval() { awk -v n="$2" '$1==n {print $2}' "$1" | head -1; }   # mval <file> <metric>

run_k6() {   # run_k6 <testid>
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$1" SCRIPT="stage-2/K2/script.js" \
      RATE="$RATE" DURATION="$DURATION" PHASE=measured PAYLOAD_MODE="$PAYLOAD_MODE" API_KEY="$API_KEY" BASE_URL="$BASE_URL" ROUTE="/healthz" ./runner.sh )
}
record() { printf '%s\t%s\t%s\t%s\t%s\n' "$testid" "$1" "$2" "$3" "$4" >>"$RUNS_TSV"; }

one_run() {  # one_run <config> <rep>
  local cfg="$1" rep="$2"
  testid="k2-${cfg}-n${rep}"
  echo; echo "== config=$cfg repeat=$rep ($testid): ${CFG[$cfg]:-baseline} =="
  stop_server; recreate_topic "$TOPIC"; start_server "$cfg"; warmup
  echo "   settling 10 s"; sleep 10
  local before after rows status=ok t0 pre="$CAP/$testid.pre.metrics" post="$CAP/$testid.post.metrics"
  before=$(topic_sum "$TOPIC"); scrape "$pre"
  t0=$(date +%s)
  run_k6 "$testid" || { status=FAILED; echo "!! $testid failed: see its manifest.md"; }
  sleep 6                                  # requests still running finish; librdkafka stats (tx_bytes) update every 5 s
  after=$(topic_sum "$TOPIC"); rows=$(( after - before )); scrape "$post"
  record measured "$t0" "$(date +%s)" "$status"
  local logb; logb=$(topic_log_bytes "$TOPIC")
  # Derived columns for summary.tsv (all from before/after differences, so the warm-up does not count)
  local d_bs d_bc d_ss d_sc d_tx d_cpu avg_batch wire_per_rec log_per_rec cpu_ms send_ms
  d_bs=$(awk -v a="$(mval "$post" ep_batcher_batch_size_messages_sum)" -v b="$(mval "$pre" ep_batcher_batch_size_messages_sum)" 'BEGIN{print a-b}')
  d_bc=$(awk -v a="$(mval "$post" ep_batcher_batch_size_messages_count)" -v b="$(mval "$pre" ep_batcher_batch_size_messages_count)" 'BEGIN{print a-b}')
  d_ss=$(awk -v a="$(mval "$post" ep_kafka_send_seconds_sum)" -v b="$(mval "$pre" ep_kafka_send_seconds_sum)" 'BEGIN{print a-b}')
  d_sc=$(awk -v a="$(mval "$post" ep_kafka_send_seconds_count)" -v b="$(mval "$pre" ep_kafka_send_seconds_count)" 'BEGIN{print a-b}')
  d_tx=$(awk -v a="$(mval "$post" ep_kafka_client_tx_bytes_total)" -v b="$(mval "$pre" ep_kafka_client_tx_bytes_total)" 'BEGIN{print a-b}')
  d_cpu=$(awk -v au="$(mval "$post" process_cpu_user_seconds_total)" -v bu="$(mval "$pre" process_cpu_user_seconds_total)" \
               -v as="$(mval "$post" process_cpu_system_seconds_total)" -v bs="$(mval "$pre" process_cpu_system_seconds_total)" 'BEGIN{print (au-bu)+(as-bs)}')
  avg_batch=$(awk -v s="$d_bs" -v c="$d_bc" 'BEGIN{ if (c>0) printf "%.1f", s/c; else print "n/a" }')
  send_ms=$(awk -v s="$d_ss" -v c="$d_sc" 'BEGIN{ if (c>0) printf "%.2f", s/c*1000; else print "n/a" }')
  wire_per_rec=$(awk -v t="$d_tx" -v r="$rows" 'BEGIN{ if (r>0) printf "%.0f", t/r; else print "n/a" }')
  cpu_ms=$(awk -v t="$d_cpu" -v r="$rows" 'BEGIN{ if (r>0) printf "%.3f", t*1000/r; else print "n/a" }')
  log_per_rec=$(awk -v b="${logb:-0}" -v n="$after" 'BEGIN{ if (n>0) printf "%.0f", b/n; else print "n/a" }')
  local js; js=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  if [ -n "$js" ]; then
    python3 "$ANALYZE" row --json "$js" --testid "$testid" --phase measured --rate "$RATE" --rows "$rows" --prefix k2 \
      --var "config=$cfg" --extra "avg_batch=$avg_batch" --extra "send_ms_avg=$send_ms" --extra "srv_cpu_ms_per_req=$cpu_ms" \
      --extra "wire_B_per_rec=$wire_per_rec" --extra "log_B_per_rec=$log_per_rec" --out "$SUMMARY"
    tail -1 "$SUMMARY" | awk -F'\t' '{printf "   -> config %s: achieved %s rps, p50 %s ms, p99 %s ms, dropped %s, audit: %s\n",$3,$5,$12,$14,$16,$18}'
    echo "   -> avg batch $avg_batch msgs, send $send_ms ms, server CPU $cpu_ms ms/req, wire $wire_per_rec B/rec, log $log_per_rec B/rec"
  else
    echo "   !! no summary json for $testid (k6 failed before finishing?)"
  fi
}

# ---- 6. Main ----------------------------------------------------------------------
mkdir -p "$CAP"
preflight
printf 'testid\tphase\tstart_epoch\tend_epoch\tstatus\n' >"$RUNS_TSV"
for c in $KAFKA_CONTAINERS; do pin_container "$c" "$KAFKA_CPUS"; done
[ -n "$PG_CONTAINER" ] && pin_container "$PG_CONTAINER" "$MON_CPUS"
for c in $MON_CONTAINERS; do pin_container "$c" "$MON_CPUS"; done

{
  echo "experiment: $EXP_ID   date: $(date -Is)"
  echo "git: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo n/a), $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l) dirty files"
  echo "kernel: $(uname -r)   node: $(node -v)   k6: $(k6 version | head -1)   cpus: $(nproc)"
  echo "pinning: server=$SERVER_CPUS brokers=$KAFKA_CPUS k6=$K6_CPUS monitoring+postgres=$MON_CPUS ($MON_CONTAINERS $PG_CONTAINER)"
  echo "settings: RATE=$RATE DURATION=$DURATION REPEATS=$REPEATS CONFIGS=[$CONFIGS] PAYLOAD_MODE=$PAYLOAD_MODE TOPIC=$TOPIC PARTITIONS=$PARTITIONS"
  echo "api: KAFKA_BROKERS=$K_BROKERS KAFKA_KEY_MODE=$K_KEY_MODE; baseline: $BASE_ENV"
  for c in $CONFIGS; do echo "config $c: ${CFG[$c]:-(baseline)}"; done
  echo "--- brokers ---"
  echo "image: $(docker inspect -f '{{.Config.Image}}' "$KC1")"
  for c in $KAFKA_CONTAINERS; do docker inspect -f '{{.Name}} nanocpus={{.HostConfig.NanoCpus}} memory={{.HostConfig.Memory}} cpuset={{.HostConfig.CpusetCpus}}' "$c"; done
  kt --describe --topic "$TOPIC" 2>/dev/null | head -2 || true
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
} >"$OUT/env.txt"

for rep in $(seq 1 "$REPEATS"); do
  order="$CONFIGS"; [ $((rep % 2)) = 0 ] && order=$(tr ' ' '\n' <<<"$CONFIGS" | tac | tr '\n' ' ')
  for cfg in $order; do one_run "$cfg" "$rep"; done
done

echo; python3 "$ANALYZE" table "$SUMMARY"
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Summary: $SUMMARY  captures: $CAP  env: $OUT/env.txt"
