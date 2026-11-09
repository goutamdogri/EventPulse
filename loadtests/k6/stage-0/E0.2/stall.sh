#!/usr/bin/env bash
set -euo pipefail

DURATION="${DURATION:-0}"
GAPS="${GAPS:-4}"
OFFSET="${OFFSET:-10}"
STALL="${STALL:-5000}"

# Validate integer inputs
for var in DURATION GAPS OFFSET STALL; do
    value="${!var}"

    if ! [[ "$value" =~ ^[0-9]+$ ]]; then
        echo "ERROR: $var must be a non-negative integer" >&2
        exit 1
    fi
done

# Prevent division by zero
if (( GAPS == 0 )); then
    echo "ERROR: GAPS must be greater than zero" >&2
    exit 1
fi

# Calculate repetitions
if (( DURATION > 0 )); then
    if (( DURATION <= OFFSET )); then
        echo "ERROR: DURATION must be greater than OFFSET" >&2
        exit 1
    fi

    REPS=$(( (DURATION - OFFSET) / (STALL/1000 + GAPS) ))
else
    REPS=3
fi

echo "Duration: $DURATION"
echo "Offset:   $OFFSET"
echo "Gaps:     $GAPS"
echo "Reps:     $REPS"
echo "Stall:    $STALL ms"

sleep "$OFFSET"

for ((laps = 1; laps <= REPS; laps++)); do
    echo "Running lap $laps/$REPS"
    curl --fail --silent --show-error \
        "http://localhost:8000/stall?ms=$STALL"

    sleep "$GAPS"
done