#!/usr/bin/env bash

set -euo pipefail

BASE_URL="localhost:8000"
HEALTH_URL="/health"
CPU_URL="/cpu"
CPU_WORKER_URL="/cpu-worker"
N="${N:-38}"
FILE_PATH="${FILE_PATH:-baseline-observer-output.txt}"
BATTERY="/sys/class/power_supply/BAT0/status"

STATUS=$(cat "$BATTERY")

if [[ "$STATUS" == "Discharging" ]]; then
    POWER_STATUS="Battery power"
else
    POWER_STATUS="AC power"
fi

die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

curl -fsS --max-time 5 "${BASE_URL}${HEALTH_URL}" >/dev/null ||
    die "Preflight check failed for ${BASE_URL}${HEALTH_URL}. Check the server and ROUTE."

TIMESTAMP="$(date '+%Y%m%d-%H%M%S')"

cat >> "${FILE_PATH}" <<EOF

Timestamp: $TIMESTAMP (N = $N) ($POWER_STATUS)

		url								obs-1						obs-2						obs-3						obs-4						obs-5
	-------						---------				---------				---------				---------				---------
EOF

printf '%s' "	${CPU_WORKER_URL}		" >> "$FILE_PATH"
for i in 1 2 3 4 5; do 
	Time=$(curl -s -o /dev/null -w '%{time_total}\n' "${BASE_URL}${CPU_WORKER_URL}?n=${N}")
	printf '%s' "		${Time}		" >> "$FILE_PATH"
done

printf '\n'	>> "$FILE_PATH"

printf '%s' "	${CPU_URL}					" >> "$FILE_PATH"
for i in 1 2 3 4 5; do 
	Time=$(curl -s -o /dev/null -w '%{time_total}\n' "${BASE_URL}${CPU_URL}?n=${N}")
	printf '%s' "		${Time}		" >> "$FILE_PATH"
done

printf '\n\n'	>> "$FILE_PATH"