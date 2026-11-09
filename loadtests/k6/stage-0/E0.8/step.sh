#!/usr/bin/env bash
# E0.8: Accept queue and backlog
# ONE command runs the whole experiment:  ./step.sh
#
# What this script does (so I can re-read it later):
#   1. preflight    - check tools, sudo, Prometheus, node_exporter, the LISTEN_BACKLOG server patch, free CPU core
#   2. env snapshot - save kernel, sysctls, versions and pinning next to the results
#   3. matrix       - for every cell: settle, set sysctls, restart the server with the cell's backlog,
#                     check the kernel really uses that backlog, run k6 (via runner.sh) and take cheap
#                     counter snapshots (nstat) before and after, plus a 10 Hz sampler of the accept queue.
#   4. probe        - ONE separate run where the heavy tools (tcpdump, full ss dumps) are allowed to
#                     disturb things. Never used for headline numbers (E0.6 lesson).
#   5. cleanup      - on exit (even Ctrl+C): stop server, sampler, exporter; restore the sysctls I changed.
#
# Usage:  ./step.sh                                      (all cells + probe, REPEATS=1 = TRIAL)
#         DRY_RUN=1 ./step.sh                            (print the plan, run nothing)
#         ONLY="A_b16_n200" PROBES=0 ./step.sh           (one cell by hand)
#         PART=A ./step.sh   |  PART=B ./step.sh         (only the backlog sweep / only the mechanism cells)
#         REPEATS=3 ./step.sh                            (real data: 3 repeats per cell)
#
# EDIT markers below = the only places that change from experiment to experiment.
set -euo pipefail
export LC_ALL=C      # keeps decimal points as '.' (EPOCHREALTIME and awk would break with a ',' locale)

# ---- 1. Settings (everything can be overridden from the environment) --------
EXP_ID="E0.8"
RESULTS_NAME="${RESULTS_NAME:-e08}"
REPEATS="${REPEATS:-1}"                  # 1 = TRIAL (no variance). The real run needs >= 3.
PART="${PART:-ALL}"                      # A | B | ALL
ONLY="${ONLY:-}"                         # space separated cell ids; empty = all selected cells
PROBES="${PROBES:-1}"
STALL_MS="${STALL_MS:-1500}"             # how long /stall blocks the event loop in the stall cells
BURST_DELAY_MS="${BURST_DELAY_MS:-200}"  # burst starts this long after the stall request
BASE_URL="${BASE_URL:-http://localhost:8000}"
ROUTE="${ROUTE:-/health}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
MANAGE_SERVER="${MANAGE_SERVER:-1}"      # must stay 1: the backlog is a server start-up setting
SERVER_CPUS="${SERVER_CPUS:-0,6}"
K6_CPUS="${K6_CPUS:-1,2,3,7,8,9}"
SAMPLER_CPU="${SAMPLER_CPU:-4}"          # a core that neither the server nor k6 uses (checked in preflight)
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-0.1}" # seconds between accept-queue samples
SETTLE_MAX="${SETTLE_MAX:-3000}"         # max TIME_WAIT sockets before a cell may start (sanity, not critical here)
GAP_SECS="${GAP_SECS:-3}"                # quiet time between cells
CELL_SECONDS="${CELL_SECONDS:-30}"       # rough time per cell, only used for the time estimate
PROBE_SECS="${PROBE_SECS:-30}"           # how long tcpdump may run in the probe
DRY_RUN="${DRY_RUN:-0}"

PORT="${PORT:-$(sed -E 's#^[a-z]+://[^:/]+:([0-9]+).*#\1#' <<<"$BASE_URL")}"

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"       # loadtests/k6/stage-0/E0.8
STAGE_DIR="$(dirname "$HERE")"                              
K6_DIR="$(cd "$STAGE_DIR/.." && pwd)"                       # runner.sh lives here
REPO="$(cd "$STAGE_DIR/../../.." && pwd)"
LAB_DIR="$REPO/labs/stage-0-foundations"
SERVER_RUNNER="$LAB_DIR/script/server-runner.sh"
EXPORTER="$REPO/infra/scripts/listen-queue-exporter.sh"
STATE_FILE="${E05_STATE_DIR:-/tmp/eventpulse-e0.5}/server.env"   # server-runner.sh writes the pid here
OUT="$STAGE_DIR/results/$RESULTS_NAME"
CAP="$OUT/captures"
RUNS_TSV="$OUT/runs.tsv"
DELTAS_TSV="$OUT/deltas.tsv"

die() { echo "ERROR: $*" >&2; exit 1; }

# ---- 3. The matrix (EDIT) ----------------------------------------------------
# One line per cell:  id | listen backlog | somaxconn | tcp_abort_on_overflow | burst size | stall ms | k6 timeout | part
# Part A: backlog sweep (somaxconn is raised to 4096 so the backlog is never clamped).
# Part B: mechanism cells (clamp, resets, no-stall controls, short client timeout).
cells() {
  local n b
  for n in 50 200 1000; do            # interleaved: all backlogs for one burst size, then the next size
    for b in 511 128 16 4; do
      echo "A_b${b}_n${n}|$b|4096|0|$n|$STALL_MS|10s|A"
    done
  done
  echo "B1_clamp_s16_b511|511|16|0|200|$STALL_MS|10s|B"     # backlog 511 but somaxconn 16: kernel should clamp to 16
  echo "B2_abort1_b16|16|4096|1|200|$STALL_MS|10s|B"        # tcp_abort_on_overflow=1: overflow answers with RST
  echo "B3a_nostall_b4|4|4096|0|200|0|10s|B"                # control: free event loop, tiny backlog
  echo "B3b_nostall_b511|511|4096|0|200|0|10s|B"            # control: free event loop, default backlog
  echo "B4_timeout2s_b16|16|4096|0|200|$STALL_MS|2s|B"      # client gives up after 2 s: do I see timeouts now?
}
PROBE_CELL="P_b16_n200|16|4096|0|200|$STALL_MS|10s|P"

selected_cells() {
  local line id part
  while IFS= read -r line; do
    id="${line%%|*}"; part="${line##*|}"
    [ "$PART" = "ALL" ] || [ "$PART" = "$part" ] || continue
    if [ -n "$ONLY" ]; then case " $ONLY " in *" $id "*) ;; *) continue ;; esac; fi
    echo "$line"
  done < <(cells)
}

# ---- 4. Small helpers --------------------------------------------------------
expand_cpus() { local IFS=,; local p; for p in $1; do if [[ $p == *-* ]]; then seq "${p%-*}" "${p#*-}"; else echo "$p"; fi; done; }
in_cpu_list() { local x="$1" c; for c in $(expand_cpus "$2"); do [ "$c" = "$x" ] && return 0; done; return 1; }

# Column 2 = Recv-Q (current accept queue length), column 3 = Send-Q (the backlog the kernel is really using)
listen_field() { ss -Htln "( sport = :${PORT} )" 2>/dev/null | awk -v f="$1" 'NR==1{print $f}'; }

CAP_FILE=""
tag() { printf '\n=== exp=%s testid=%s phase=%s ts=%s cmd: %s\n' "$EXP_ID" "${testid:-none}" "$1" "$(date -Is)" "$2" >>"$CAP_FILE"; }

NSTAT_NAMES="TcpExtListenOverflows TcpExtListenDrops TcpExtTCPSynRetrans TcpExtTCPTimeouts TcpOutRsts TcpPassiveOpens TcpActiveOpens TcpAttemptFails TcpEstabResets TcpRetransSegs"
# -a absolute values, -s do not touch nstat's history file, -z also print counters that are 0
nstat_dump() { nstat -asz $NSTAT_NAMES 2>/dev/null | awk '!/^#/ {print $1, $2}' >"$1" || true; }
light_snapshot() {   # light_snapshot pre|post  (cheap: reads /proc/net/*; fine inside a measured run)
  nstat_dump "$CAP/$testid.$1.nstat"
  tag "$1" "nstat -asz $NSTAT_NAMES"; cat "$CAP/$testid.$1.nstat" >>"$CAP_FILE"
}
write_deltas() {     # write_deltas <testid> <cell>   (post minus pre, system-wide counters)
  local pre="$CAP/$1.pre.nstat" post="$CAP/$1.post.nstat"
  [ -s "$pre" ] && [ -s "$post" ] || return 0
  awk -v t="$1" -v c="$2" 'NR==FNR{pre[$1]=$2; next} {printf "%s\t%s\t%s\t%s\t%s\t%s\n", t, c, $1, pre[$1]+0, $2, $2-pre[$1]}' "$pre" "$post" >>"$DELTAS_TSV"
}

# ---- 5. Preflight --------------------------------------------------------------
preflight() {
  [ "${BASH_VERSINFO[0]}" -ge 5 ] || die "bash >= 5 needed (EPOCHREALTIME); found $BASH_VERSION"
  for c in k6 taskset curl ss nstat awk sysctl sudo node; do command -v "$c" >/dev/null || die "'$c' not found in PATH"; done
  if [ "$PROBES" = "1" ]; then command -v tcpdump >/dev/null || echo "WARN: tcpdump not found; the probe will skip the packet capture (sudo apt install tcpdump)" >&2; fi
  [ "$MANAGE_SERVER" = "1" ] || die "MANAGE_SERVER must be 1: the listen backlog is only read when the server starts"
  [ -x "$K6_DIR/runner.sh" ] || die "$K6_DIR/runner.sh missing or not executable"
  [ -f "$HERE/script.js" ] || die "$HERE/script.js missing"
  [ -f "$SERVER_RUNNER" ] || die "server-runner.sh not found at $SERVER_RUNNER"
  [ -f "$EXPORTER" ] || die "listen-queue exporter not found at $EXPORTER"
  { grep -q 'LISTEN_BACKLOG' "$LAB_DIR/src/config.js" && grep -q 'LISTEN_BACKLOG' "$LAB_DIR/src/server.js"; } 2>/dev/null \
    || die "the server does not know LISTEN_BACKLOG yet: apply server-changes.diff first (see README step 1)"
  curl -fsS --max-time 3 "$PROM_URL/-/ready" >/dev/null \
    || die "Prometheus not ready at $PROM_URL (docker compose -f infra/compose/docker-compose.yml up -d)"
  curl -fsS --max-time 3 "$PROM_URL/api/v1/query?query=up%7Bjob%3D%22node%22%7D" | grep -q '"value":\[[^]]*"1"\]' \
    || echo "WARN: node_exporter (job=node) is not 'up' in Prometheus; host panels will be empty" >&2

  # the sampler must run on a core that neither the server nor k6 uses, otherwise it would steal their CPU
  local ncpu; ncpu="$(nproc --all)"
  [ "$SAMPLER_CPU" -lt "$ncpu" ] || die "SAMPLER_CPU=$SAMPLER_CPU does not exist (this machine has $ncpu logical CPUs). Set SAMPLER_CPU to a free core."
  if in_cpu_list "$SAMPLER_CPU" "$SERVER_CPUS" || in_cpu_list "$SAMPLER_CPU" "$K6_CPUS"; then
    die "SAMPLER_CPU=$SAMPLER_CPU overlaps with SERVER_CPUS=$SERVER_CPUS or K6_CPUS=$K6_CPUS. Pick a free core."
  fi

  # k6 allocates one JavaScript VM per VU. I have not measured how much 1000 VUs need on this machine.
  local avail_kb; avail_kb="$(awk '/MemAvailable/ {print $2}' /proc/meminfo)"
  [ "${avail_kb:-0}" -ge 2000000 ] || echo "WARN: only $((avail_kb/1024)) MiB available; the 1000-connection cells may run out of memory" >&2

  # sudo is needed for sysctl -w (and tcpdump in the probe). Ask for the password once, keep the ticket alive.
  sudo -v || die "sudo is required (sysctl -w, tcpdump)"
  ( while :; do sudo -n true 2>/dev/null; sleep 50; done ) & SUDO_KEEP_PID=$!
}

env_snapshot() {
  {
    echo "experiment: $EXP_ID   date: $(date -Is)"
    echo "TRIAL: REPEATS=$REPEATS (a single repeat has no variance information)"
    echo "git: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo n/a) $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l) dirty files"
    echo "kernel: $(uname -r)   node: $(node -v 2>/dev/null)   k6: $(k6 version 2>/dev/null | head -1)   bash: $BASH_VERSION"
    echo "logical CPUs: $(nproc --all)   MemAvailable: $(awk '/MemAvailable/ {print int($2/1024)" MiB"}' /proc/meminfo)"
    echo "server pinned to: $SERVER_CPUS   k6 pinned to: $K6_CPUS   queue sampler pinned to: $SAMPLER_CPU (every ${SAMPLE_INTERVAL}s)"
    echo "settings: PART=$PART ONLY=[$ONLY] REPEATS=$REPEATS STALL_MS=$STALL_MS BURST_DELAY_MS=$BURST_DELAY_MS PROBES=$PROBES"
    echo "ORIGINAL somaxconn=$ORIG_SOMAXCONN  ORIGINAL tcp_abort_on_overflow=$ORIG_ABORT   (restored on exit)"
    echo "--- sysctls (before the first cell) ---"
    sysctl net.core.somaxconn net.ipv4.tcp_abort_on_overflow net.ipv4.tcp_max_syn_backlog net.ipv4.tcp_syncookies \
           net.ipv4.tcp_syn_retries net.ipv4.tcp_synack_retries net.ipv4.ip_local_port_range 2>&1 || true
    echo "--- ulimit -n: $(ulimit -n) ---"
  } > "$OUT/env.txt"
}

# ---- 6. Things I change on the machine, and how they are restored -----------------
ORIG_SOMAXCONN=""; ORIG_ABORT=""; SYSCTL_CHANGED=0
SERVER_PID=""; SAMPLER_PID=""; EXPORTER_PID=""; SUDO_KEEP_PID=""; TCPDUMP_PID=""

set_sysctls() {   # set_sysctls <somaxconn> <abort_on_overflow>
  SYSCTL_CHANGED=1
  sudo -n sysctl -qw net.core.somaxconn="$1" net.ipv4.tcp_abort_on_overflow="$2"
  [ "$(sysctl -n net.core.somaxconn)" = "$1" ] && [ "$(sysctl -n net.ipv4.tcp_abort_on_overflow)" = "$2" ] \
    || die "sysctl did not take effect"
}
restore_sysctls() {
  [ "$SYSCTL_CHANGED" = "1" ] || return 0
  sudo -n sysctl -qw net.core.somaxconn="$ORIG_SOMAXCONN" net.ipv4.tcp_abort_on_overflow="$ORIG_ABORT" \
    && echo "restored somaxconn=$ORIG_SOMAXCONN tcp_abort_on_overflow=$ORIG_ABORT" \
    || echo "!! COULD NOT RESTORE sysctls. Run: sudo sysctl -w net.core.somaxconn=$ORIG_SOMAXCONN net.ipv4.tcp_abort_on_overflow=$ORIG_ABORT" >&2
}

start_exporter() {
  if pgrep -f listen-queue-exporter.sh >/dev/null 2>&1; then echo "listen-queue exporter already running: leaving it alone"; return 0; fi
  PORT="$PORT" INTERVAL=0.25 bash "$EXPORTER" >"$OUT/exporter.out" 2>&1 &
  EXPORTER_PID=$!
}

# 10 Hz sampler of the listening socket (Recv-Q = queue length, Send-Q = backlog). Prometheus scrapes every 1 s,
# which is too coarse for a 1.5 s event, so this CSV is the high resolution record of the queue.
start_sampler() {   # start_sampler <csv>
  echo "epoch,recvq,sendq" >"$1"
  PORT="$PORT" SAMPLE_INTERVAL="$SAMPLE_INTERVAL" taskset -c "$SAMPLER_CPU" bash -c '
    while :; do
      read -r _ rq sq _ < <(ss -Htln "( sport = :$PORT )" | head -1)
      printf "%s,%s,%s\n" "$EPOCHREALTIME" "${rq:-NA}" "${sq:-NA}"
      sleep "$SAMPLE_INTERVAL"
    done' >>"$1" 2>/dev/null &
  SAMPLER_PID=$!
}
stop_sampler() { [ -n "$SAMPLER_PID" ] && { kill "$SAMPLER_PID" 2>/dev/null || true; wait "$SAMPLER_PID" 2>/dev/null || true; SAMPLER_PID=""; }; return 0; }

start_server() {   # start_server [VAR=value ...]  (extra env only for this server start)
  stop_server
  env "$@" SERVER_CPUS="$SERVER_CPUS" PORT="$PORT" bash "$SERVER_RUNNER" >"$OUT/server.out" 2>&1 &     # bash <file>: still the node pid after its exec
  for _ in $(seq 1 30); do curl -fsS --max-time 1 "${BASE_URL}${ROUTE}" >/dev/null 2>&1 && break; sleep 1; done
  curl -fsS --max-time 2 "${BASE_URL}${ROUTE}" >/dev/null || die "server did not come up (see $OUT/server.out)"
  SERVER_PID=$(. "$STATE_FILE"; echo "$SERVER_PID")      # server-runner.sh execs node, so this is the node pid
}
stop_server() {
  if [ -f "$STATE_FILE" ]; then
    local p; p=$(. "$STATE_FILE"; echo "${SERVER_PID:-}")
    [ -n "$p" ] && kill -0 "$p" 2>/dev/null && { kill "$p" 2>/dev/null || true; }
  fi
  for _ in $(seq 1 50); do [ -z "$(listen_field 2)" ] && break; sleep 0.1; done   # wait until the listening socket is gone
  return 0
}

cleanup() {
  set +e
  stop_sampler
  [ -n "$EXPORTER_PID" ] && kill "$EXPORTER_PID" 2>/dev/null
  [ -n "$TCPDUMP_PID" ] && kill "$TCPDUMP_PID" 2>/dev/null
  [ -n "$SUDO_KEEP_PID" ] && kill "$SUDO_KEEP_PID" 2>/dev/null
  [ "$MANAGE_SERVER" = "1" ] && stop_server
  restore_sysctls
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# ---- 7. Settle and k6 -----------------------------------------------------------
settle() {   # wait until no half-open handshakes for my port are left and TIME_WAIT is not huge
  local n syn waited=0
  while :; do
    n=$(ss -Htan state time-wait | wc -l)
    syn=$(ss -Htan "( sport = :$PORT or dport = :$PORT )" | awk '$1 ~ /^SYN/ {c++} END{print c+0}')
    if [ "$n" -le "$SETTLE_MAX" ] && [ "$syn" -eq 0 ]; then break; fi
    [ "$waited" -ge 120 ] && { echo "WARN: still TIME_WAIT=$n SYN=$syn after 120 s, continuing"; break; }
    echo "  settle: TIME_WAIT=$n (max $SETTLE_MAX), SYN sockets=$syn ..."; sleep 3; waited=$((waited+3))
  done
  sleep "$GAP_SECS"
}

run_k6() {   # run_k6 <testid> <cell> <backlog> <somaxconn> <abort> <burst> <stall_ms> <timeout> <phase>
  ( cd "$K6_DIR" && timeout 300 env RESULTS_NAME="$RESULTS_NAME" LABEL="$1" SCRIPT="stage-0/$EXP_ID/script.js" REPS=1 \
      CELL="$2" BACKLOG="$3" SOMAXCONN="$4" ABORT="$5" BURST="$6" STALL_MS="$7" TIMEOUT="$8" PHASE="${9}" \
      BURST_DELAY_MS="$BURST_DELAY_MS" K6_CPUS="$K6_CPUS" BASE_URL="$BASE_URL" ROUTE="$ROUTE" ./runner.sh )
}

# Pull the per-connection lines (E08REQ,...) out of k6's console output into a CSV. k6 prints them either as
#   time="..." level=info msg="E08REQ,..." source=console     (no terminal)    or    INFO[0001] E08REQ,... source=console
extract_req() {   # extract_req <testid>
  { echo "cell,start_ms,blocked_ms,connecting_ms,sending_ms,waiting_ms,receiving_ms,duration_ms,status,outcome"
    { grep -ho 'E08REQ,[^" ]*' "$CAP/$1.k6.log" 2>/dev/null || true; } | sed 's/^E08REQ,//'
  } >"$CAP/$1.req.csv"
}

record() { printf '%s\n' "$*" | tr ' ' '\t' >>"$RUNS_TSV"; }   # one run per line, tab separated

# Common start of a cell: sysctls, server restart, check that the kernel uses the backlog I asked for.
prepare_cell() {   # prepare_cell <backlog> <somaxconn> <abort>   -> sets EFF_SENDQ, CFG_STATUS
  local b="$1" sc="$2" ab="$3"
  settle
  set_sysctls "$sc" "$ab"
  start_server LISTEN_BACKLOG="$b"
  EFF_SENDQ="$(listen_field 3)"
  local want=$(( b < sc ? b : sc ))      # the kernel uses min(backlog, somaxconn)
  CFG_STATUS=ok
  if [ "$EFF_SENDQ" != "$want" ]; then
    CFG_STATUS=CONFIG_MISMATCH
    echo "!! effective backlog is '$EFF_SENDQ' but I expected $want (min of backlog=$b, somaxconn=$sc). Result is suspect."
  else
    echo "  kernel is using backlog $EFF_SENDQ (min($b, $sc))"
  fi
  for _ in $(seq 1 50); do [ "$(listen_field 2)" = "0" ] && break; sleep 0.2; done   # queue must start empty
}

peak_recvq() { awk -F, 'NR>1 && $2!="NA" && $2+0>m {m=$2+0} END{print m+0}' "$1"; }

# ---- 8. A measured cell: only cheap counters and the 10 Hz sampler are attached -------
run_measured() {   # run_measured <line> <repeat>
  local id b sc ab n st to part; IFS='|' read -r id b sc ab n st to part <<<"$1"
  testid="${EXP_ID,,}-${id,,}-r$2"; testid="${testid//./}"; testid="${testid//_/-}"
  echo; echo "== measured: $id  backlog=$b somaxconn=$sc abort=$ab burst=$n stall=${st}ms timeout=$to repeat=$2 ($testid) =="
  CAP_FILE="$CAP/$testid.txt"; : >"$CAP_FILE"
  prepare_cell "$b" "$sc" "$ab"
  tag setup "TIME_WAIT now: $(ss -Htan state time-wait | wc -l); listening socket: $(ss -Htln "( sport = :$PORT )" | head -1)"
  start_sampler "$CAP/$testid.queue.csv"
  light_snapshot pre; local t0; t0=$(date +%s)
  local status=ok
  run_k6 "$testid" "$id" "$b" "$sc" "$ab" "$n" "$st" "$to" measured 2> >(tee "$CAP/$testid.k6.log" >&2) \
    || { status=FAILED; echo "!! $testid failed - check its manifest.md"; }
  sleep 1                                   # let the last retries, the sampler and the log copy finish
  extract_req "$testid"
  light_snapshot post; stop_sampler
  write_deltas "$testid" "$id"
  record "$testid" "$id" measured "$t0" "$(date +%s)" "$status" "$CFG_STATUS" "$b" "$sc" "$EFF_SENDQ" "$ab" "$n" "$st" "$to" "$(peak_recvq "$CAP/$testid.queue.csv")"
}

# ---- 9. The probe: heavy tools allowed, NOT used for headline numbers --------------
census() {   # census <label>: how many sockets are in which TCP state, and the listening socket
  tag "$1" "ss -Htan '( sport = :$PORT or dport = :$PORT )' | count by state"
  ss -Htan "( sport = :$PORT or dport = :$PORT )" | awk '{c[$1]++} END{for (s in c) printf "  %-12s %d\n", s, c[s]}' | sort >>"$CAP_FILE"
  tag "$1" "ss -tln '( sport = :$PORT )'"; ss -tln "( sport = :$PORT )" >>"$CAP_FILE" 2>&1 || true
}

analyze_pcap() {   # turn the pcap into the numbers I care about (SYN retries per client port, SYN-ACK retransmits, RSTs)
  tag probe-pcap "tcpdump -nn -tt -r $PCAP | awk ... (SYNs per client port, gaps, SYN-ACK repeats, RSTs)"
  sudo -n tcpdump -nn -tt -r "$PCAP" 2>/dev/null | awk -v P="$PORT" '
    { t=$1; src=$3; dst=$5; sub(":$","",dst)
      n=split(src,a,"."); sp=a[n]; m=split(dst,b,"."); dp=b[m]
      if ($0 ~ /Flags \[S\],/ && dp==P) { c[sp]++; if (c[sp]==1) t1[sp]=t; else if (c[sp]==2) t2[sp]=t; else if (c[sp]==3) t3[sp]=t; if (!(sp in f)) f[sp]=t; if (min=="" || t<min) min=t }
      else if ($0 ~ /Flags \[S\.\],/ && sp==P) sa[dp]++
      else if ($0 ~ /Flags \[R/) rst++ }
    END {
      for (p in c) { h[c[p]]++; tot++; if (c[p]>=2) { g=t2[p]-t1[p]; g1+=g; n1++; if (g1min==""||g<g1min) g1min=g; if (g>g1max) g1max=g } if (c[p]>=3) { g=t3[p]-t2[p]; g2+=g; n2++ } }
      printf "  client source ports that sent a SYN: %d\n", tot
      for (k=1;k<=7;k++) if (h[k]) printf "  ports that sent %d SYN(s): %d\n", k, h[k]
      if (n1) printf "  gap between SYN 1 and SYN 2: avg %.3f s (min %.3f, max %.3f) over %d ports\n", g1/n1, g1min, g1max, n1
      if (n2) printf "  gap between SYN 2 and SYN 3: avg %.3f s over %d ports\n", g2/n2, n2
      for (p in sa) { if (sa[p]>1) rep++ ; sat++ }
      printf "  client ports that got SYN-ACK more than once (server retransmitting SYN-ACK): %d of %d\n", rep+0, sat+0
      printf "  RST packets seen: %d\n", rst+0 }' >>"$CAP_FILE"
}

run_probe() {   # run_probe <line>
  local id b sc ab n st to part; IFS='|' read -r id b sc ab n st to part <<<"$1"
  testid="${EXP_ID,,}-${id,,}-probe"; testid="${testid//./}"; testid="${testid//_/-}"
  echo; echo "== probe: $id backlog=$b burst=$n stall=${st}ms ($testid) - tcpdump + ss dumps attached, do NOT quote latency from this run =="
  CAP_FILE="$CAP/$testid.txt"; : >"$CAP_FILE"
  prepare_cell "$b" "$sc" "$ab"
  start_sampler "$CAP/$testid.queue.csv"
  light_snapshot pre; local t0; t0=$(date +%s)
  PCAP="$CAP/$testid.pcap"
  if command -v tcpdump >/dev/null && sudo -n true 2>/dev/null; then
    tag probe-tcpdump "sudo -n timeout $PROBE_SECS tcpdump -i lo -nn -tt -U -w $PCAP 'tcp port $PORT and (tcp[tcpflags] & (tcp-syn|tcp-rst) != 0)'"
    sudo -n timeout "$PROBE_SECS" tcpdump -i lo -nn -tt -U -w "$PCAP" "tcp port $PORT and (tcp[tcpflags] & (tcp-syn|tcp-rst) != 0)" >"$CAP/$testid.tcpdump.err" 2>&1 &
    TCPDUMP_PID=$!; sleep 1
  else
    tag probe-tcpdump "SKIPPED (no tcpdump or no sudo)"
  fi
  local status=ok
  run_k6 "$testid" "$id" "$b" "$sc" "$ab" "$n" "$st" "$to" probe 2> >(tee "$CAP/$testid.k6.log" >&2) & local kpid=$!
  # wait until the accept queue is non-empty = the stall has begun and the burst has landed (k6 needs a few seconds to start its VUs first)
  local seen=0 rq
  for _ in $(seq 1 4500); do rq="$(listen_field 2)"; [ "${rq:-0}" -ge 1 ] && { seen=1; break; }; sleep 0.02; done
  if [ "$seen" = 1 ]; then
    sleep 0.6; census probe-during-stall        # about 0.8 s after the stall started: loop still blocked
    sleep 1.2; census probe-after-stall         # about 2.0 s after the start: loop free again, SYN retries (at about 3.2 s) still pending
  else
    tag probe "WARN: queue never became non-empty; no census taken"
  fi
  wait "$kpid" || status=FAILED
  sleep 1
  extract_req "$testid"
  light_snapshot post; stop_sampler
  if [ -n "$TCPDUMP_PID" ]; then wait "$TCPDUMP_PID" 2>/dev/null || true; TCPDUMP_PID=""; sudo -n chown "$(id -u):$(id -g)" "$PCAP" 2>/dev/null || true; [ -s "$PCAP" ] && analyze_pcap; fi
  write_deltas "$testid" "$id"
  record "$testid" "$id" probe "$t0" "$(date +%s)" "$status" "$CFG_STATUS" "$b" "$sc" "$EFF_SENDQ" "$ab" "$n" "$st" "$to" "$(peak_recvq "$CAP/$testid.queue.csv")"
}

# ---- 10. Main ----------------------------------------------------------------------
mapfile -t SELECTED < <(selected_cells)
n_cells=${#SELECTED[@]}
n_probe=0; if [ "$PROBES" = "1" ] && { [ -z "$ONLY" ] || case " $ONLY " in *" P_b16_n200 "*) true;; *) false;; esac; }; then n_probe=1; fi
total_s=$(( n_cells*REPEATS*CELL_SECONDS + n_probe*(CELL_SECONDS+PROBE_SECS) ))
[ "$REPEATS" -ge 3 ] || echo "!! TRIAL: REPEATS=$REPEATS. Results have no variance information; label them 'trial, n=1'."
echo "== $EXP_ID: $((n_cells*REPEATS)) measured runs ($n_cells cells x $REPEATS repeat) + $n_probe probe run, results in $OUT =="
echo "== expected total time: about $(( total_s/60 )) min (rough: ${CELL_SECONDS}s per cell; k6 start-up with 1000 VUs not measured yet) =="
if [ "$DRY_RUN" = "1" ]; then
  for line in "${SELECTED[@]}"; do
    IFS='|' read -r id b sc ab n st to part <<<"$line"
    echo "  measured: $id  backlog=$b somaxconn=$sc abort_on_overflow=$ab burst=$n stall=${st}ms timeout=$to  x$REPEATS"
  done
  [ "$n_probe" = 1 ] && { IFS='|' read -r id b sc ab n st to part <<<"$PROBE_CELL"; echo "  probe:    $id  backlog=$b somaxconn=$sc abort_on_overflow=$ab burst=$n stall=${st}ms (tcpdump + ss dumps)"; }
  exit 0
fi

preflight
mkdir -p "$CAP"
ORIG_SOMAXCONN="$(sysctl -n net.core.somaxconn)"; ORIG_ABORT="$(sysctl -n net.ipv4.tcp_abort_on_overflow)"
env_snapshot
printf 'testid\tcell\tphase\tstart_epoch\tend_epoch\tstatus\tcfg_check\tbacklog\tsomaxconn\teff_sendq\tabort\tburst\tstall_ms\ttimeout\tpeak_recvq\n' >"$RUNS_TSV"
printf 'testid\tcell\tcounter\tpre\tpost\tdelta\n' >"$DELTAS_TSV"
start_exporter

for rep in $(seq 1 "$REPEATS"); do
  for line in "${SELECTED[@]}"; do run_measured "$line" "$rep"; done
done
[ "$n_probe" = 1 ] && run_probe "$PROBE_CELL"

bash "$HERE/summarize.sh" "$OUT" || true      # one table per run: per-connection timings + queue + counters

echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds)."
column -t -s "$(printf '\t')" "$RUNS_TSV" | cut -c1-200
echo "k6 summaries: $OUT/<timestamp>/*.json   captures: $CAP/<testid>.*   deltas: $DELTAS_TSV   env: $OUT/env.txt"