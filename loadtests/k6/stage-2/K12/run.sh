#!/usr/bin/env bash
# K12: Message size and batch limits
# ONE command runs everything:
#     ./run.sh            5 variants x their size levels x 3 repeats, about 60 to 70 min in total
#
# This is NOT a rate sweep. The load is small and the same everywhere (5 req/s for 10 s per cell). What changes is the
# SIZE of the event and the LAYER of configuration that is set to catch it. One "cell" = one variant x one size:
#   1. the API is restarted with the settings of the variant (so its counters start at 0 for the cell)
#   2. the log end offset of the topic is read (sum over the partitions)
#   3. k6 sends RATE req/s for DURATION_S seconds, every event with the same size
#   4. the log end offset is read again, and the API's own counters are saved
#   5. one row goes to summary.tsv: how many 202 / 413 / 500 / 503 / status 0, and how many records Kafka really got
# The audit is the last point: records added to the topic must be equal to the number of 202 answers. If a request got
# 413 and the topic did not grow, Kafka never saw the event. If a request got 202 and the topic did not grow, the failure was silent.
#
# The variants (layers of configuration). Everything not listed is the same in every variant (acks=-1, idempotent, key=event_id):
#   edge      as shipped: BODY_LIMIT=64kb, BATCH_MAX_BYTES=900000, topic with the broker default message.max.bytes
#   batcher   BODY_LIMIT=4mb (edge opened), so the next layer, the batcher cap of 900000 bytes, is the one that catches
#   broker    BODY_LIMIT=4mb, topic max.message.bytes=200000 (topic override of message.max.bytes): the broker is the lowest limit
#   client    BODY_LIMIT=4mb, BATCH_MAX_BYTES=1000000 (the highest the config allows): sizes right around librdkafka's message.max.bytes (1000000)
#   acks0     like broker, with KAFKA_ACKS=0 and KAFKA_IDEMPOTENT=false: does the producer notice the broker's refusal at all?
# The real broker limit (message.max.bytes=1048588) cannot be reached through the API: the API refuses everything above
# 1000000 bytes on its own. That is why the broker layer is tested with a lower topic limit.
#
# Repeat is the OUTER loop; repeat 2 runs the variants in reverse order so drift hits all of them the same way.
#
# Usage:  DRY_RUN=1 ./run.sh                                                         print the plan, run nothing
#         VARIANTS="batcher" REPEATS=1 RESULTS_NAME=k12-trial ./run.sh               small trial (never quote its numbers)
#
# Env knobs: RATE, DURATION_S, VARIANTS, REPEATS, RESULTS_NAME, TOPIC, PARTITIONS, TOPIC_MAX_BYTES, MIX_RATE, MIX_BIG_BYTES, MIX_BIG_EVERY, DRY_RUN.
set -euo pipefail

EXP_ID="K12"
# ---- 1. Settings (override from the environment) ------------------------------
RATE="${RATE:-5}"                               # req/s of one cell. Small on purpose: this is about the size, not the load
DURATION_S="${DURATION_S:-10}"                  # seconds of one cell: RATE x DURATION_S = 50 requests per cell
VARIANTS="${VARIANTS:-edge batcher broker client acks0}"
REPEATS="${REPEATS:-3}"
RESULTS_NAME="${RESULTS_NAME:-k12-message-size}"
TOPIC="${TOPIC:-k12-events}"
PARTITIONS="${PARTITIONS:-6}"
TOPIC_MAX_BYTES="${TOPIC_MAX_BYTES:-200000}"    # max.message.bytes of the topic in the variants broker and acks0
MIX_RATE="${MIX_RATE:-1000}"                    # the poison-in-a-batch cell (variant broker): req/s of small events...
MIX_BIG_BYTES="${MIX_BIG_BYTES:-400000}"        # ...and the size of the oversized one (above TOPIC_MAX_BYTES, below the batcher cap)
MIX_BIG_EVERY="${MIX_BIG_EVERY:-100}"           # every 100th request is oversized
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
# Pinning (same plan as K11): server 0,6 | brokers 2,8,3,9 | k6 4,10 | monitoring + Postgres 5,11. (CPUs 1,7 are free: no consumer here.)
SERVER_CPUS="${SERVER_CPUS:-0,6}"
KAFKA_CPUS="${KAFKA_CPUS:-2,8,3,9}"
export K6_CPUS="${K6_CPUS:-4,10}"
MON_CPUS="${MON_CPUS:-5,11}"
PG_CONTAINER="${PG_CONTAINER:-ep-postgres}"
KEEP_PINNING="${KEEP_PINNING:-0}"
DRY_RUN="${DRY_RUN:-0}"
K_BROKERS="${KAFKA_BROKERS:-localhost:9092,localhost:9094,localhost:9096}"
BOOTSTRAP_INT="kafka1:19092,kafka2:19092,kafka3:19092"        # for the CLI tools inside the containers

# The API settings that are the same in every variant, written out so a run never depends on my shell or on .env.
# The variant adds its own settings AFTER these (with env, the last assignment wins).
API_ENV="KAFKA_ACKS=-1 KAFKA_IDEMPOTENT=true KAFKA_COMPRESSION=none KAFKA_LINGER_MS=0 BATCH_MAX_MESSAGES=500 BATCH_MAX_WAIT_MS=5 BATCH_MAX_IN_FLIGHT=1 KAFKA_KEY_MODE=event"
# Anything from my shell that would silently change the API is removed first.
for k in KAFKA_ACKS KAFKA_IDEMPOTENT KAFKA_COMPRESSION KAFKA_LINGER_MS BATCH_MAX_MESSAGES BATCH_MAX_WAIT_MS BATCH_MAX_BYTES BATCH_MAX_IN_FLIGHT BATCH_MAX_QUEUE \
         KAFKA_TOPIC KAFKA_KEY_MODE BODY_LIMIT KAFKA_SEND_TIMEOUT_MS KAFKA_DELIVERY_TIMEOUT_MS KAFKA_BREAKER_FAILURES; do unset "$k" || true; done

ALL_VARIANTS=(edge batcher broker client acks0)
for v in $VARIANTS; do
  case " ${ALL_VARIANTS[*]} " in *" $v "*) ;; *) echo "ERROR: unknown variant '$v'. Known: ${ALL_VARIANTS[*]}" >&2; exit 1;; esac
done

# Settings of one variant. Sets: V_ENV (extra API env), V_TOPIC_MAX (max.message.bytes of the topic, empty = broker default), V_LEVELS.
# A level is  body:<bytes>  (HTTP body size, what the edge sees),  wire:<bytes>  (what the batcher counts), or  mix.
LADDER="body:1000 body:32768 body:65000 body:66000 body:180000 body:262144 body:600000 body:880000 body:950000 body:2000000 body:5000000"
variant_cfg() {
  V_TOPIC_MAX=""
  case "$1" in
    edge)    V_ENV="BODY_LIMIT=64kb BATCH_MAX_BYTES=900000";  V_LEVELS="$LADDER" ;;
    batcher) V_ENV="BODY_LIMIT=4mb BATCH_MAX_BYTES=900000";   V_LEVELS="$LADDER" ;;
    broker)  V_ENV="BODY_LIMIT=4mb BATCH_MAX_BYTES=900000";   V_LEVELS="$LADDER mix"; V_TOPIC_MAX="$TOPIC_MAX_BYTES" ;;
    client)  V_ENV="BODY_LIMIT=4mb BATCH_MAX_BYTES=1000000";  V_LEVELS="body:880000 wire:990000 wire:999900 wire:1000000 wire:1000100" ;;
    acks0)   V_ENV="BODY_LIMIT=4mb BATCH_MAX_BYTES=900000 KAFKA_ACKS=0 KAFKA_IDEMPOTENT=false"; V_LEVELS="body:65000 body:180000 body:262144 body:600000"; V_TOPIC_MAX="$TOPIC_MAX_BYTES" ;;
  esac
}
level_label() { case "$1" in mix) echo mix;; body:*) echo "b${1#body:}";; wire:*) echo "w${1#wire:}";; esac; }
level_num()   { case "$1" in mix) echo "$MIX_BIG_BYTES";; *) echo "${1#*:}";; esac; }

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-2/K12
STAGE_DIR="$(dirname "$HERE")"
RUNNER_DIR="$(dirname "$STAGE_DIR")"
ANALYZE="$RUNNER_DIR/analyze.py"
REPO="$(cd "$RUNNER_DIR/../.." && pwd)"
API_DIR="$REPO/services/ingest-api"
TEXTFILE_DIR="${TEXTFILE_DIR:-$REPO/infra/compose/textfile}"
OUT="$HERE/results/$RESULTS_NAME"
CAP="$OUT/captures"; RUNS_TSV="$OUT/runs.tsv"; SUMMARY="$OUT/summary.tsv"
SIZE_PROM="$TEXTFILE_DIR/ep_k12.prom"

die() { echo "ERROR: $*" >&2; exit 1; }

# ---- 3. Plan / dry run ---------------------------------------------------------
cells_per_rep=0
for v in $VARIANTS; do variant_cfg "$v"; cells_per_rep=$(( cells_per_rep + $(wc -w <<<"$V_LEVELS") )); done
n_var=$(wc -w <<<"$VARIANTS")
# per cell: API restart + settle (about 9 s) + k6 start/flush and offsets (about 8 s) + the load; per variant and repeat: topic recreate (about 20 s)
total_s=$(( REPEATS * ( cells_per_rep * (17 + DURATION_S) + n_var * 20 ) ))
echo "== $EXP_ID: $n_var variants, $cells_per_rep cells per repeat x $REPEATS repeats = $((cells_per_rep*REPEATS)) measured cells, $RATE req/s for ${DURATION_S}s each, results in $OUT =="
echo "== expected total about $(( total_s/60 )) min =="
if [ "$DRY_RUN" = "1" ]; then
  echo "  topic $TOPIC: $PARTITIONS partitions, RF 3, min.insync.replicas=2 (+ max.message.bytes=$TOPIC_MAX_BYTES in broker and acks0), deleted + recreated before every variant run"
  for v in $VARIANTS; do
    variant_cfg "$v"
    echo "  variant $v: API: $API_ENV $V_ENV | topic max.message.bytes: ${V_TOPIC_MAX:-broker default (1048588)}"
    echo "     levels: $V_LEVELS"
  done
  for rep in $(seq 1 "$REPEATS"); do
    order="$VARIANTS"; [ $((rep % 2)) = 0 ] && order=$(tr ' ' '\n' <<<"$VARIANTS" | tac | tr '\n' ' ')
    echo "  repeat $rep variant order: $order"
  done
  echo "  mix cell (variant broker): $MIX_RATE req/s small events, every ${MIX_BIG_EVERY}th request is ${MIX_BIG_BYTES} bytes"
  echo "  CPU plan: server=$SERVER_CPUS brokers=$KAFKA_CPUS k6=$K6_CPUS monitoring+postgres=$MON_CPUS"
  echo "  changes the machine: docker update --cpuset-cpus (restored), topic $TOPIC deleted and recreated (this topic only), textfile $SIZE_PROM (removed on exit), the API is started and stopped by the script"
  exit 0
fi

# ---- 4. Helpers -----------------------------------------------------------------
KAFKA_CONTAINERS=""; MON_CONTAINERS=""
live_broker() { docker ps --format '{{.Names}}' | grep -E '^ep-kafka[0-9]+$' | sort | head -1; }
kt()  { timeout "${TMO:-20}" docker exec "$(live_broker)" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INT" "$@"; }
# log end offset of the topic, summed over the partitions (kafka-get-offsets --time -1 prints topic:partition:offset)
end_sum() { timeout 20 docker exec "$(live_broker)" /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server "$BOOTSTRAP_INT" --topic "$TOPIC" --time -1 2>/dev/null | awk -F: '{s+=$3} END{printf "%d", s+0}'; }
now() { date +%s.%N; }

detect_containers() {
  KAFKA_CONTAINERS=$(docker ps -a --format '{{.Names}}' | grep -E '^ep-kafka[0-9]+$' | sort | tr '\n' ' ')
  [ "$(wc -w <<<"$KAFKA_CONTAINERS")" = 3 ] || die "expected 3 broker containers, found [$KAFKA_CONTAINERS]"
  MON_CONTAINERS=$(docker ps --format '{{.Names}} {{.Image}}' | awk '$2 ~ /prometheus|grafana|node-exporter|node_exporter|kafka-exporter|postgres-exporter/ {print $1}' | tr '\n' ' ')
}

BROKER_MAX="unknown"; TENANT=""
preflight() {
  for c in k6 taskset curl docker python3 awk node jq timeout ss; do command -v "$c" >/dev/null || die "'$c' not found in PATH"; done
  [ -x "$RUNNER_DIR/runner.sh" ] || die "$RUNNER_DIR/runner.sh missing or not executable"
  [ -f "$ANALYZE" ] || die "$ANALYZE missing"
  curl -fsS --max-time 3 "$PROM_URL/-/ready" >/dev/null || die "Prometheus not ready at $PROM_URL"
  detect_containers
  for c in $KAFKA_CONTAINERS; do [ "$(docker inspect -f '{{.State.Health.Status}}' "$c")" = healthy ] || die "$c is not healthy"; done
  docker ps --format '{{.Names}}' | grep -qx ep-kafka-exporter || echo "WARNING: ep-kafka-exporter is not running: the dashboard will miss the topic offsets"
  kt --list >/dev/null || die "kafka-topics.sh inside a broker failed"
  ss -ltn "sport = :8000" | grep -q LISTEN && die "something already listens on :8000 (an old ingest-api?). Stop it first"
  [ -f "$API_DIR/dist/main.js" ] || die "$API_DIR/dist/main.js missing: run 'pnpm build' in services/ingest-api"
  [ -f "$API_DIR/.env" ] || die "$API_DIR/.env missing"
  [ -d "$TEXTFILE_DIR" ] || die "textfile directory $TEXTFILE_DIR not found"
  # API_KEYS in .env is  key:tenant[,key:tenant...]. The first pair is used. The tenant name is needed for the wire sizes.
  local pair; pair=$(grep -E '^API_KEYS=' "$API_DIR/.env" 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"' | cut -d, -f1 || true)
  [ -n "${API_KEY:-}" ] || API_KEY="${pair%%:*}"
  TENANT="${pair#*:}"
  [ -n "$API_KEY" ] && [ -n "$TENANT" ] && [ "$TENANT" != "$pair" ] || die "API_KEYS=key:tenant not found in $API_DIR/.env"
  export API_KEY TENANT
  # The message.max.bytes of the brokers (recorded in env.txt, it is the limit the topic override replaces)
  BROKER_MAX=$(timeout 20 docker exec ep-kafka1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:19092 --entity-type brokers --entity-name 1 --describe --all 2>/dev/null \
             | awk '/^ *message.max.bytes=/ {split($1,a,"="); print a[2]; exit}' || true)
  BROKER_MAX="${BROKER_MAX:-unknown}"
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
recreate_topic() {   # recreate_topic <topic> <max.message.bytes or empty>
  local t="$1" mx="$2" i cfg=()
  [ -z "$mx" ] || cfg=(--config "max.message.bytes=$mx")   # topic override of the broker's message.max.bytes, on THIS topic only
  kt --delete --topic "$t" >/dev/null 2>&1 || true
  for i in $(seq 1 60); do kt --list 2>/dev/null | grep -qx "$t" || break; sleep 1; done
  for i in $(seq 1 30); do
    kt --create --topic "$t" --partitions "$PARTITIONS" --replication-factor 3 --config min.insync.replicas=2 "${cfg[@]}" >/dev/null 2>&1 && break
    [ "$i" = 30 ] && die "could not create topic $t"; sleep 1
  done
  wait_full_isr "$t" 30 || die "topic $t: not all partitions have a full ISR after 30 s"
}

SERVER_PID=""
start_server() {   # start_server <testid> <variant env>
  stop_server
  # shellcheck disable=SC2086
  ( cd "$API_DIR" && exec env KAFKA_BROKERS="$K_BROKERS" KAFKA_TOPIC="$TOPIC" $API_ENV $2 \
      taskset -c "$SERVER_CPUS" node --env-file=.env dist/main.js ) >"$CAP/$1.server.out" 2>&1 &
  SERVER_PID=$!
  local _
  for _ in $(seq 1 30); do curl -fsS --max-time 1 "$BASE_URL/healthz" >/dev/null 2>&1 && break; sleep 1; done
  for _ in $(seq 1 20); do curl -fsS --max-time 2 "$BASE_URL/readyz" >/dev/null 2>&1 && return 0; sleep 1; done
  die "ingest-api not ready (see $CAP/$1.server.out)"
}
stop_server() {
  if [ -n "${SERVER_PID:-}" ] && kill -0 "$SERVER_PID" 2>/dev/null; then kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; fi
  SERVER_PID=""
}

# One gauge for Grafana: the size of the cell that is running (body bytes, or wire bytes for the wire levels, or the big size for mix); -1 between cells.
set_size() {
  { echo "# HELP ep_k12_size_bytes K12 size of the cell that is running (the level number), -1 between cells."
    echo "# TYPE ep_k12_size_bytes gauge"
    echo "ep_k12_size_bytes{testid=\"${testid:-none}\",variant=\"${variant:-none}\"} $1"; } >"$SIZE_PROM.tmp" && mv "$SIZE_PROM.tmp" "$SIZE_PROM"
}
cleanup() { stop_server; rm -f "$SIZE_PROM" "$SIZE_PROM.tmp"; restore_pinning; }
trap cleanup EXIT

record() { printf '%s\t%s\t%s\t%s\t%s\n' "$testid" "$1" "$2" "$3" "$4" >>"$RUNS_TSV"; }
# Counter value of the k6 lap JSON, 0 when the counter never got a value (k6 leaves a counter out when it was never added to)
kc() { jq -r --arg m "$2" '(.metrics[$m] // {}) | (.count // .values.count // 0)' "$1"; }
kg_() { jq -r --arg m "$2" '(.metrics[$m] // {}) | (.value // .values.value // 0)' "$1"; }

# One sample request with curl, saved with its status line and response body (first 400 bytes): what the client really sees.
# It is made after the cell (outside the measured window) so it cannot change the counts. The body is built like k6 builds it.
sample_request() {   # sample_request <testid> <body bytes>
  local f="$CAP/$1.sample.json"
  python3 - "$2" "$f" <<'PY'
import json, sys, uuid
body_bytes, path = int(sys.argv[1]), sys.argv[2]
mk = lambda f: json.dumps({"event_id": str(uuid.uuid4()), "type": "k12.size", "occurred_at": "2026-01-01T00:00:00.000Z", "schema_version": 1, "payload": {"f": f}}, separators=(",", ":"))
n = body_bytes - len(mk(""))
open(path, "w").write(mk("x" * max(n, 0)))
PY
  {
    echo "=== $1 sample request: body $(stat -c %s "$f") bytes, $(date -Is)"
    # --max-time: for a big body the server may answer 413 and close the connection while curl is still sending
    curl -sS --max-time 20 -o "$CAP/$1.sample.body" -w 'http_status=%{http_code}\n' -X POST "$BASE_URL/v1/events" \
         -H 'content-type: application/json' -H "authorization: Bearer $API_KEY" --data-binary "@$f" 2>&1 || echo "curl exit code $?"
    head -c 400 "$CAP/$1.sample.body" 2>/dev/null; echo
  } >>"$CAP/$1.sample.txt"
  rm -f "$f" "$CAP/$1.sample.body"
}

one_cell() {   # one_cell <variant> <rep> <level>
  variant="$1"; local rep="$2" level="$3" lab kind num status=ok
  lab=$(level_label "$level"); num=$(level_num "$level")
  kind="${level%%:*}"
  testid="k12-${variant}-${lab}-n${rep}"
  variant_cfg "$variant"
  echo; echo "== variant=$variant level=$level repeat=$rep ($testid) =="
  set_size -1
  start_server "$testid" "$V_ENV"
  sleep 3
  local end0 end1 t0
  end0=$(end_sum)
  set_size "$num"
  t0=$(now)
  local k6_size_kind="$kind" k6_size="$num" k6_rate="$RATE"
  [ "$kind" = mix ] && k6_rate="$MIX_RATE"
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$testid" SCRIPT="stage-2/K12/script.js" \
      CELL="$variant" LEVEL="$lab" SIZE_KIND="$k6_size_kind" SIZE="$k6_size" RATE="$RATE" DURATION_S="$DURATION_S" \
      MIX_RATE="$MIX_RATE" MIX_BIG_BYTES="$MIX_BIG_BYTES" MIX_BIG_EVERY="$MIX_BIG_EVERY" TENANT="$TENANT" PHASE=measured \
      API_KEY="$API_KEY" BASE_URL="$BASE_URL" ROUTE="/healthz" ./runner.sh ) || { status=FAILED; echo "!! $testid: k6 failed, see its manifest.md"; }
  record measured "$(printf '%.0f' "$t0")" "$(date +%s)" "$status"
  sleep 2   # the last acknowledged batches are on the brokers
  end1=$(end_sum)
  set_size -1
  # The API's own counters for this cell (the API was restarted for the cell, so they start at 0). Saved, then read.
  curl -fsS --max-time 3 "$BASE_URL/metrics" 2>/dev/null | grep -E '^(ep_kafka_send_errors_total|ep_batcher_rejected_total|ep_kafka_messages_acked_total|ep_kafka_messages_failed_total|ep_http_503_total|http_requests_total\{[^}]*method="POST")' >"$CAP/$testid.api-counters.txt" || true
  local acked failed too_large send_err http_st
  acked=$(awk '/^ep_kafka_messages_acked_total/ {s+=$NF} END{printf "%d", s+0}' "$CAP/$testid.api-counters.txt")
  failed=$(awk '/^ep_kafka_messages_failed_total/ {s+=$NF} END{printf "%d", s+0}' "$CAP/$testid.api-counters.txt")
  too_large=$(awk '/^ep_batcher_rejected_total/ && /reason="too_large"/ {s+=$NF} END{printf "%d", s+0}' "$CAP/$testid.api-counters.txt")
  send_err=$(awk '/^ep_kafka_send_errors_total/ { match($0,/type="[^"]*"/); t=substr($0,RSTART+6,RLENGTH-7); printf "%s=%d,", t, $NF }' "$CAP/$testid.api-counters.txt"); send_err="${send_err%,}"; send_err="${send_err:-none}"
  http_st=$(awk '/^http_requests_total/ { match($0,/status="[^"]*"/); t=substr($0,RSTART+8,RLENGTH-9); s[t]+=$NF } END{ for(k in s) printf "%s=%d,", k, s[k] }' "$CAP/$testid.api-counters.txt"); http_st="${http_st%,}"; http_st="${http_st:-none}"
  # What the client sees for one request of this size (text of the error). The body size of the sample = the body size of the cell.
  local jsn; jsn=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  local sbody="$num"; [ "$kind" = wire ] && sbody=$(( num - 147 - ${#TENANT} ))
  [ "$kind" = mix ] || sample_request "$testid" "$sbody"
  stop_server
  local kdelta=$(( end1 - end0 ))
  if [ -n "$jsn" ]; then
    local n202 n413 n500 n503 n0 bytes_body bytes_wire
    n202=$(kc "$jsn" resp_202); n413=$(kc "$jsn" resp_413); n500=$(kc "$jsn" resp_500); n503=$(kc "$jsn" resp_503); n0=$(kc "$jsn" resp_status0)
    bytes_body=$(kg_ "$jsn" body_bytes); bytes_wire=$(kg_ "$jsn" wire_bytes)
    python3 "$ANALYZE" row --json "$jsn" --testid "$testid" --phase measured --rate "$k6_rate" --rows "$kdelta" \
      --ok-code 202 --fail-code 503 --p99-limit-ms 1000000 --err-limit-pct 100 \
      --var "variant=$variant" --var "level=$lab" \
      --extra "body_bytes=$bytes_body" --extra "wire_bytes=$bytes_wire" \
      --extra "n413=$n413" --extra "n500=$n500" --extra "status0=$n0" \
      --extra "kafka_delta=$kdelta" --extra "api_acked=$acked" --extra "api_failed=$failed" --extra "batcher_too_large=$too_large" \
      --extra "send_errors=$send_err" --extra "api_http=$http_st" \
      --extra "small_ok=$(kc "$jsn" small_202)" --extra "small_fail=$(kc "$jsn" small_fail)" \
      --extra "big_ok=$(kc "$jsn" big_202)" --extra "big_fail=$(kc "$jsn" big_fail)" \
      --extra "status=$status" --out "$SUMMARY"
    echo "   -> 202:$n202 413:$n413 500:$n500 503:$n503 status0:$n0 | topic grew by $kdelta | API: acked $acked failed $failed batcher_too_large $too_large send_errors $send_err"
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
  echo "pinning: server=$SERVER_CPUS brokers=$KAFKA_CPUS k6=$K6_CPUS monitoring+postgres=$MON_CPUS ($MON_CONTAINERS $PG_CONTAINER)"
  echo "settings: RATE=$RATE DURATION_S=$DURATION_S REPEATS=$REPEATS VARIANTS=[$VARIANTS] TOPIC=$TOPIC PARTITIONS=$PARTITIONS TOPIC_MAX_BYTES=$TOPIC_MAX_BYTES MIX_RATE=$MIX_RATE MIX_BIG_BYTES=$MIX_BIG_BYTES MIX_BIG_EVERY=$MIX_BIG_EVERY"
  echo "broker: message.max.bytes=$BROKER_MAX"
  echo "tenant name length (for the wire sizes): ${#TENANT}"
  echo "api common: KAFKA_BROKERS=$K_BROKERS $API_ENV"
  for v in $VARIANTS; do variant_cfg "$v"; echo "variant $v: $V_ENV | topic max.message.bytes=${V_TOPIC_MAX:-default} | levels: $V_LEVELS"; done
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
} >"$OUT/env.txt"

for rep in $(seq 1 "$REPEATS"); do
  order="$VARIANTS"; [ $((rep % 2)) = 0 ] && order=$(tr ' ' '\n' <<<"$VARIANTS" | tac | tr '\n' ' ')
  for v in $order; do
    variant_cfg "$v"
    echo; echo "##### repeat $rep, variant $v: topic with max.message.bytes=${V_TOPIC_MAX:-default} #####"
    stop_server
    for c in $KAFKA_CONTAINERS; do for _ in $(seq 1 60); do [ "$(docker inspect -f '{{.State.Health.Status}}' "$c")" = healthy ] && break; sleep 1; done; done
    recreate_topic "$TOPIC" "$V_TOPIC_MAX"
    for lv in $V_LEVELS; do one_cell "$v" "$rep" "$lv"; done
  done
done

kt --delete --topic "$TOPIC" >/dev/null 2>&1 || true
echo; python3 "$ANALYZE" table "$SUMMARY"
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Summary: $SUMMARY  captures: $CAP  env: $OUT/env.txt"
