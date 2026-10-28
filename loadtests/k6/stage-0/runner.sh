
#!/usr/bin/env bash

set -Eeuo pipefail

# ------------------------------------------------------------
# 1. Capture the environment available when the script starts.
#    Later-created harness variables will not be captured.
# ------------------------------------------------------------

declare -A INITIAL_ENV=()

while IFS= read -r name; do
    INITIAL_ENV["$name"]="${!name}"
done < <(compgen -e)

# ------------------------------------------------------------
# 2. Harness configuration and defaults
# ------------------------------------------------------------

REPS="${REPS:-1}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="${SCRIPT:-myTestScript.js}"

GAP="${GAP:-10s}"
K6_CPUS="${K6_CPUS:-1,2,3,7,8,9}"
ROUTE="${ROUTE:-/health}"

BASE_URL="${BASE_URL:-http://localhost:8000}"
TIMESTAMP="$(date '+%Y%m%d-%H%M%S')"

K6_PROMETHEUS_RW_PUSH_INTERVAL="${K6_PROMETHEUS_RW_PUSH_INTERVAL:-1s}"
K6_PROMETHEUS_RW_SERVER_URL="${K6_PROMETHEUS_RW_SERVER_URL:-http://localhost:9090/api/v1/write}"
K6_PROMETHEUS_RW_TREND_AS_NATIVE_HISTOGRAM="${K6_PROMETHEUS_RW_TREND_AS_NATIVE_HISTOGRAM:-true}"
K6_PROMETHEUS_RW_TREND_STATS="${K6_PROMETHEUS_RW_TREND_STATS:-p(50),p(90),p(99),p(99.9),max}"

export K6_PROMETHEUS_RW_PUSH_INTERVAL
export K6_PROMETHEUS_RW_SERVER_URL
export K6_PROMETHEUS_RW_TREND_AS_NATIVE_HISTOGRAM
export K6_PROMETHEUS_RW_TREND_STATS


# ------------------------------------------------------------
# 3. Identify variables reserved for the harness.
#
#    Every captured environment variable not listed here is
#    forwarded to k6 as a test-script environment variable.
# ------------------------------------------------------------

HARNESS_ENV_VARS=(
    REPS
    SCRIPT
    SCRIPT_DIR
    GAP
    K6_CPUS
    ROUTE
    BASE_URL
    RESULTS_NAME
    RESULTS_DIR
    TIMESTAMP
    MANIFEST_FILE
    OUTPUT_FILE
    TESTID
    START_TIME
    END_TIME
    STATUS
    STATUS_CODE
    laps
    name
    value
    var_name
    INITIAL_ENV
    HARNESS_ENV_VARS
    HARNESS_EXCLUDE
    HARNESS_CONFIG_VARS
    K6_ENV_ARGS
    K6_CMD
    K6_PROMETHEUS_RW_PUSH_INTERVAL
    K6_PROMETHEUS_RW_SERVER_URL
    K6_PROMETHEUS_RW_TREND_AS_NATIVE_HISTOGRAM
    K6_PROMETHEUS_RW_TREND_STATS
)

declare -A HARNESS_EXCLUDE=()

for name in "${HARNESS_ENV_VARS[@]}"; do
    HARNESS_EXCLUDE["$name"]=1
done

# ------------------------------------------------------------
# 4. Helper functions
# ------------------------------------------------------------

die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

# Redact likely secrets before writing values to the manifest.
# Original values are still passed to k6.
redact_value() {
    local name="$1"
    local value="$2"

    if [[ "$name" =~ (SECRET|TOKEN|PASSWORD|PASSWD|API_KEY|PRIVATE_KEY|CREDENTIAL|AUTH) ]]; then
        printf '[REDACTED]'
    else
        printf '%s' "$value" |
            tr '\n\r' '  ' |
            sed 's/|/\\|/g'
    fi
}

# ------------------------------------------------------------
# 5. Validate configuration and dependencies
# ------------------------------------------------------------

for cmd in k6 taskset curl; do
    command -v "$cmd" >/dev/null 2>&1 ||
        die "Required command '$cmd' was not found in PATH."
done

[[ -f "$SCRIPT_DIR/$SCRIPT" ]] ||
    die "k6 script not found: $SCRIPT_DIR/$SCRIPT"

[[ "$REPS" =~ ^[1-9][0-9]*$ ]] ||
    die "REPS must be a positive integer."

# ------------------------------------------------------------
# 6. Ask for a results folder name
# ------------------------------------------------------------

read -r -p "Result folder name [default]: " RESULTS_NAME
RESULTS_NAME="${RESULTS_NAME:-default}"

[[ "$RESULTS_NAME" =~ ^[a-zA-Z0-9._-]+$ ]] ||
    die "Folder name may contain only letters, numbers, dots, underscores, and hyphens."

[[ "$RESULTS_NAME" != "." && "$RESULTS_NAME" != ".." ]] ||
    die "Invalid result folder name."

RESULTS_DIR="$SCRIPT_DIR/results/$RESULTS_NAME/$TIMESTAMP"
MANIFEST_FILE="$RESULTS_DIR/manifest.md"

mkdir -p -- "$RESULTS_DIR"
touch "$MANIFEST_FILE"

# ------------------------------------------------------------
# 7. Build k6 environment arguments automatically.
#
#    Every startup environment variable that is NOT reserved
#    for the harness becomes a k6 -e NAME=value argument.
# ------------------------------------------------------------

K6_ENV_ARGS=()

for name in "${!INITIAL_ENV[@]}"; do
    if [[ ${HARNESS_EXCLUDE[$name]+present} ]]; then
        continue
    fi

    K6_ENV_ARGS+=(-e "$name=${INITIAL_ENV[$name]}")
done

# ------------------------------------------------------------
# 8. Write the manifest header and run configuration
# ------------------------------------------------------------

{
    printf '# EventPulse Load Test Run\n\n'
    printf -- '- **Timestamp:** `%s`\n' "$TIMESTAMP"
    printf -- '- **Results directory:** `%s`\n' "$RESULTS_DIR"
    printf -- '- **k6 script:** `%s`\n' "$SCRIPT"
    printf -- '- **Base URL:** `%s`\n' "$BASE_URL"
    printf -- '- **Preflight route:** `%s`\n' "$ROUTE"
    printf -- '- **Number of laps:** `%s`\n' "$REPS"
    printf -- '- **Gap between laps:** `%s`\n' "$GAP"
    printf -- '- **Pinned CPUs:** `%s`\n' "$K6_CPUS"
    printf -- '- **Prometheus remote-write URL:** `%s`\n' \
        "$(redact_value K6_PROMETHEUS_RW_SERVER_URL "$K6_PROMETHEUS_RW_SERVER_URL")"
    printf '\n'

    printf '## k6 Test Script\n\n```javascript\n'
    cat -- "$SCRIPT_DIR/$SCRIPT"
    printf '\n```\n\n'

    printf '## Harness Configuration\n\n'
    printf '| Variable | Value |\n'
    printf '|---|---|\n'
    for name in \
        REPS SCRIPT GAP K6_CPUS ROUTE BASE_URL RESULTS_NAME \
        K6_PROMETHEUS_RW_PUSH_INTERVAL \
        K6_PROMETHEUS_RW_SERVER_URL \
        K6_PROMETHEUS_RW_TREND_AS_NATIVE_HISTOGRAM \
        K6_PROMETHEUS_RW_TREND_STATS
    do
        printf '| `%s` | `%s` |\n' \
            "$name" "$(redact_value "$name" "${!name}")"
    done
    printf '\n'

    printf '## Environment Variables Passed to k6\n\n'
    printf 'These are startup environment variables not reserved for the harness.\n\n'
    printf '| Variable | Value |\n'
    printf '|---|---|\n'

    # Sort variable names for a stable manifest.
    while IFS= read -r name; do
        [[ -n "$name" ]] || continue
        [[ ${HARNESS_EXCLUDE[$name]+present} ]] && continue

        printf '| `%s` | `%s` |\n' \
            "$name" "$(redact_value "$name" "${INITIAL_ENV[$name]}")"
    done < <(printf '%s\n' "${!INITIAL_ENV[@]}" | sort)

    printf '\n'
    printf '## Lap Results\n\n'
    printf '| Lap Number | Start Time | End Time | Status | Exit Code | Summary File |\n'
    printf '|---:|---|---|---|---:|---|\n'
} > "$MANIFEST_FILE"

# ------------------------------------------------------------
# 9. Preflight check
# ------------------------------------------------------------

printf 'Checking target endpoint: %s%s\n' "$BASE_URL" "$ROUTE"

curl -fsS --max-time 5 "${BASE_URL}${ROUTE}" >/dev/null ||
    die "Preflight check failed for ${BASE_URL}${ROUTE}. Check the server and ROUTE."

# ------------------------------------------------------------
# 10. Execute test laps
# ------------------------------------------------------------

for ((laps = 1; laps <= REPS; laps++)); do
    OUTPUT_FILE="$RESULTS_DIR/${laps}-lap.json"
    TESTID="${RESULTS_NAME}-${TIMESTAMP}-lap-${laps}"

    START_TIME="$(date '+%Y-%m-%d %H:%M:%S')"

    printf '\n'
    printf '========================================\n'
    printf 'Starting lap %s/%s\n' "$laps" "$REPS"
    printf 'Test ID: %s\n' "$TESTID"
    printf 'Start time: %s\n' "$START_TIME"
    printf '========================================\n'

    K6_CMD=(
        k6 run
        -o experimental-prometheus-rw
        --tag "testid=$TESTID"
        --summary-export="$OUTPUT_FILE"
        "${K6_ENV_ARGS[@]}"
        "$SCRIPT_DIR/$SCRIPT"
    )

    if taskset -c "$K6_CPUS" "${K6_CMD[@]}"; then
        STATUS="PASS"
        STATUS_CODE=0
    else
        STATUS_CODE=$?
        STATUS="FAIL"
    fi

    END_TIME="$(date '+%Y-%m-%d %H:%M:%S')"

    # Append the result immediately so a failed lap is recorded.
    printf '| %s | %s | %s | %s | %s | `%s` |\n' \
        "$laps" \
        "$START_TIME" \
        "$END_TIME" \
        "$STATUS" \
        "$STATUS_CODE" \
        "$(basename -- "$OUTPUT_FILE")" \
        >> "$MANIFEST_FILE"

    printf '\nLap %s finished: %s (exit code %s)\n' \
        "$laps" "$STATUS" "$STATUS_CODE"

    if (( STATUS_CODE != 0 )); then
        printf 'Stopping after failed lap %s.\n' "$laps" >&2
        exit "$STATUS_CODE"
    fi

    # Do not sleep after the final lap.
    if (( laps < REPS )); then
        sleep "$GAP"
    fi
done

printf '\nAll %s lap(s) completed successfully.\n' "$REPS"
printf 'Results directory: %s\n' "$RESULTS_DIR"
printf 'Manifest: %s\n' "$MANIFEST_FILE"
