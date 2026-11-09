#!/usr/bin/env bash
# E0.8 summary: one row per run, built from the raw files step.sh saved. Usage: ./summarize.sh [results_dir]
# Needs only bash, awk, sort. Works on any machine, no Prometheus needed.
#
# Columns:
#   n ok timeout reset other        outcome of each burst connection (from captures/<testid>.req.csv)
#   c<200ms c~1s c~3s c>=5s         how many connections had a connect time in that range
#                                   (<200 ms = got in, ~1 s = first SYN dropped but retry worked, ~3 s = two drops, >=5 s = more)
#   conn_p50 conn_p99 wait_p50 wait_p99 dur_p99   milliseconds, client side (k6)
#   eff_sendq peak_recvq            backlog in use / highest queue length seen at 10 Hz (from runs.tsv)
#   overflows drops synretrans rsts system-wide kernel counters, after minus before (from deltas.tsv)
set -euo pipefail
export LC_ALL=C
OUT="${1:-$(dirname "$0")/../results/e08}"
CAP="$OUT/captures"
[ -f "$OUT/runs.tsv" ] || { echo "no $OUT/runs.tsv"; exit 1; }

pct() {  # pct <file with one number per line> <p>   (nearest-rank percentile on a sorted copy)
  sort -n "$1" | awk -v p="$2" '{a[NR]=$1} END{ if (NR==0) {print "NA"; exit} i=int((p/100)*NR); if (i<(p/100)*NR) i++; if (i<1) i=1; printf "%.0f", a[i] }'
}
delta() { awk -F'\t' -v t="$1" -v c="$3" '$1==t && $3==c {print $6}' "$OUT/deltas.tsv" | head -1; }

{
printf 'testid\tcell\tphase\tn\tok\ttimeout\treset\tother\tc<200ms\tc~1s\tc~3s\tc>=5s\tconn_p50\tconn_p99\twait_p50\twait_p99\tdur_p99\teff_sendq\tpeak_recvq\toverflows\tdrops\tsynretrans\trsts\tcfg_check\n'
tail -n +2 "$OUT/runs.tsv" | while IFS=$'\t' read -r testid cell phase _ _ status cfg backlog somax eff abort burst stall to peak; do
  f="$CAP/$testid.req.csv"
  if [ ! -s "$f" ] || [ "$(wc -l <"$f")" -lt 2 ]; then
    printf '%s\t%s\t%s\tNO_REQ_DATA\t\t\t\t\t\t\t\t\t\t\t\t\t\t%s\t%s\t\t\t\t\t%s\n' "$testid" "$cell" "$phase" "$eff" "$peak" "$cfg"; continue
  fi
  tmp=$(mktemp); tail -n +2 "$f" >"$tmp"
  # CSV columns: 1 cell, 2 start_ms, 3 blocked, 4 connecting, 5 sending, 6 waiting, 7 receiving, 8 duration, 9 status, 10 outcome
  cut -d, -f4 "$tmp" >"$tmp.c"; cut -d, -f6 "$tmp" >"$tmp.w"; cut -d, -f8 "$tmp" >"$tmp.d"
  counts=$(awk -F, '{n++; o[$10]++; c=$4+0; if (c<200) a++; else if (c<2000) b++; else if (c<5000) d++; else e++}
       END{printf "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d", n, o["ok"], o["timeout"], o["reset"], o["other"], a, b, d, e}' "$tmp")
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$testid" "$cell" "$phase" "$counts" \
    "$(pct "$tmp.c" 50)" "$(pct "$tmp.c" 99)" "$(pct "$tmp.w" 50)" "$(pct "$tmp.w" 99)" "$(pct "$tmp.d" 99)" \
    "$eff" "$peak" \
    "$(delta "$testid" "$cell" TcpExtListenOverflows)" "$(delta "$testid" "$cell" TcpExtListenDrops)" \
    "$(delta "$testid" "$cell" TcpExtTCPSynRetrans)" "$(delta "$testid" "$cell" TcpOutRsts)" "$cfg"
  rm -f "$tmp" "$tmp.c" "$tmp.w" "$tmp.d"
done
} >"$OUT/summary.tsv"

echo; echo "== summary (also saved to $OUT/summary.tsv) =="
if command -v column >/dev/null; then column -t -s "$(printf "\t")" "$OUT/summary.tsv" | cut -c1-260; else cat "$OUT/summary.tsv"; fi
