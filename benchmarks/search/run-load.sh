#!/usr/bin/env bash
# Load tests of Search through nginx, changing one factor at a time (enunciado §6.2):
#
#   clients    1, 8, 32, 128, 256, with 1, 2 and 4 replicas
#   threads    16 and 64 Jetty threads (the baseline is 250), with 1 replica
#   pool       10 and 100 MongoDB connections (the baseline is 50), with 1 replica
#   nginx      0.5 CPUs, the limit of the deployment (the baseline here is 2), with 1 replica
#
# nginx has 2 CPUs by default in these tests: with the 0.5 of .env it saturates first and hides the
# effect of everything else. NGINX_CPUS=0.5 in the "nginx" experiment measures how much it limits.
#
#   benchmarks/search/run-load.sh           all of it: about 1 h 45 min, on an idle machine
#   benchmarks/search/run-load.sh --smoke   about 3 minutes, to check it works; the numbers mean nothing
#
# It starts the system, loads the dataset into its own database and deletes that database at the end,
# so the index of other work is not touched. It finishes with docker compose down.
# Results: benchmarks/search/results/load/ (the --smoke ones in results/load-smoke/). See README.md.
#
# Variables: DATASET_DIR, REPETITIONS, WARMUP, DURATION, WARM_RUN, BASE_NGINX_CPUS, MVN, COMPOSE.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

MVN="${MVN:-mvn}"
COMPOSE="${COMPOSE:-docker compose}"
DATASET_DIR="${DATASET_DIR:-../stage_1/sample_dataset/book}"
URL="http://localhost:8080"
BASE_THREADS=250
BASE_POOL=50
BASE_NGINX_CPUS="${BASE_NGINX_CPUS:-2}"

SMOKE=false
[ "${1:-}" = "--smoke" ] && SMOKE=true

if $SMOKE; then
    OUT="benchmarks/search/results/load-smoke"
    REPETITIONS="${REPETITIONS:-1}"; WARMUP="${WARMUP:-2}"; DURATION="${DURATION:-3}"
    CLIENTS_LEVELS="1 8"; REPLICA_LEVELS="1 2"; THREAD_LEVELS="16"; POOL_LEVELS="10"; POOL_CLIENTS="8"
    WARM_RUN="${WARM_RUN:-3}"; NGINX_LEVELS="0.5"; NGINX_CLIENTS="8"
    rm -rf "$OUT"
else
    OUT="benchmarks/search/results/load"
    REPETITIONS="${REPETITIONS:-3}"; WARMUP="${WARMUP:-15}"; DURATION="${DURATION:-30}"
    CLIENTS_LEVELS="1 8 32 128 256"; REPLICA_LEVELS="1 2 4"; THREAD_LEVELS="16 64"; POOL_LEVELS="10 100"
    POOL_CLIENTS="32 128 256"
    WARM_RUN="${WARM_RUN:-60}"; NGINX_LEVELS="0.5"; NGINX_CLIENTS="32 128 256"
    if [ -e "$OUT" ]; then
        echo "ERROR: $OUT already exists. Move it away first: results of two runs must not be mixed." >&2
        exit 1
    fi
fi

for tool in java "$MVN" git docker curl; do
    if ! command -v "$tool" > /dev/null; then
        echo "ERROR: $tool not found. Requirements: JDK 17 or newer, Maven 3.9, git, curl and Docker with Compose." >&2
        exit 1
    fi
done
if ! $COMPOSE version > /dev/null 2>&1; then
    echo "ERROR: '$COMPOSE' does not work. Is Docker running? For the old command: COMPOSE=docker-compose" >&2
    exit 1
fi
if [ ! -d "$DATASET_DIR" ]; then
    echo "ERROR: dataset not found at $DATASET_DIR. stage_1 must be next to stage_2, or set DATASET_DIR." >&2
    exit 1
fi
# pwd -W: a Windows path in Git Bash, because Git Bash does not convert the one inside -Dexec.args.
DATASET_ABSOLUTE="$(cd "$DATASET_DIR" && { pwd -W 2>/dev/null || pwd; })"

# A database of its own: the loader EMPTIES the one it receives. It takes it as an argument, not from this variable.
export MONGO_DATABASE=search_benchmark
export SEARCH_HTTP_THREADS="$BASE_THREADS"
export SEARCH_MONGO_POOL_SIZE="$BASE_POOL"
export NGINX_CPUS="$BASE_NGINX_CPUS"

cleanup() {
    local status=$?
    [ "$status" -eq 0 ] || echo "FAILED (exit $status). See the messages above and $OUT/compose.log" >&2
    echo "Cleaning up: deleting the database $MONGO_DATABASE and stopping the system"
    $COMPOSE exec -T mongo mongosh --quiet "$MONGO_DATABASE" --eval 'db.dropDatabase()' > /dev/null 2>&1 || true
    $COMPOSE down > /dev/null 2>&1 || true
}

# Starts the configuration and waits until nginx reaches every replica, each with the index loaded.
start_system() {
    local replicas="$1" threads="$2" pool="$3"
    export SEARCH_HTTP_THREADS="$threads" SEARCH_MONGO_POOL_SIZE="$pool"
    $COMPOSE up -d --wait --scale search="$replicas" mongo hz-1 hz-2 search nginx >> "$OUT/compose.log" 2>&1
    wait_for_replicas "$replicas"
    # New JVMs are slow until the JIT warms up. This load is not recorded.
    java -Xmx1g benchmarks/search/LoadGenerator.java --url "$URL" --clients 32 --warmup 0 --duration "$WARM_RUN" > /dev/null
}

wait_for_replicas() {
    local replicas="$1" deadline=$((SECONDS + 180))
    while [ "$SECONDS" -lt "$deadline" ]; do
        local answers failures seen
        answers="$(for _ in $(seq 1 $((replicas * 6))); do
            curl -s -o /dev/null -D - -w 'status:%{http_code}\n' "$URL/search?q=whale" 2>/dev/null || echo 'status:000'
        done)"
        failures="$(echo "$answers" | grep '^status:' | grep -vc '^status:200' || true)"
        seen="$(echo "$answers" | tr -d '\r' | grep -i '^x-served-by:' | sort -u | wc -l | tr -d ' ')"
        if [ "$failures" = "0" ] && [ "$seen" = "$replicas" ]; then
            return 0
        fi
        sleep 2
    done
    echo "ERROR: after 180 s, nginx does not reach $replicas healthy replicas with the index loaded" >&2
    return 1
}

run_point() {
    local experiment="$1" replicas="$2" threads="$3" pool="$4" clients="$5" repetition="$6"
    local label="${experiment}-r${replicas}-t${threads}-p${pool}-c${clients}-rep${repetition}"
    echo "$(date -u +%H:%M:%S) $label"
    bash scripts/monitor.sh -l "$label" -o "$OUT/monitor" -d $((WARMUP + DURATION)) > /dev/null 2>&1 &
    local monitor=$!
    java -Xmx1g benchmarks/search/LoadGenerator.java --url "$URL" --clients "$clients" \
        --warmup "$WARMUP" --duration "$DURATION" --out "$OUT/summary.csv" \
        --experiment "$experiment" --replicas "$replicas" --http-threads "$threads" --mongo-pool "$pool" \
        --repetition "$repetition" | tail -1 | cut -d, -f9-19
    wait "$monitor" || echo "WARNING: the monitor failed for $label" >&2
}

mkdir -p "$OUT"
trap cleanup EXIT

echo "Building the image of Search"
$COMPOSE build search >> "$OUT/compose.log" 2>&1
$COMPOSE up -d --wait mongo >> "$OUT/compose.log" 2>&1

echo "Loading the dataset from $DATASET_ABSOLUTE into $MONGO_DATABASE"
"$MVN" -q -B install -DskipTests -pl common -am
(cd search-service && "$MVN" -q -B test-compile exec:java -Dexec.args="'$DATASET_ABSOLUTE' mongodb://localhost:27017 $MONGO_DATABASE")

# Host, limits and dataset of this run: the report must state them (enunciado §6.1).
{
    echo "date_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "commit: $(git rev-parse --short HEAD)$(git diff --quiet HEAD -- search-service scripts docker-compose.yml .env || echo ' + uncommitted changes')"
    echo "uname: $(uname -a)"
    echo "logical_cpus: $(getconf _NPROCESSORS_ONLN 2>/dev/null || echo unknown)"
    if [ -r /proc/cpuinfo ]; then
        echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')"
        echo "memory: $(grep -m1 MemTotal /proc/meminfo | cut -d: -f2- | sed 's/^ *//')"
    fi
    echo "docker: $(docker info --format '{{.NCPU}} CPUs, {{.MemTotal}} bytes, {{.OperatingSystem}}')"
    java -version 2>&1 | sed 's/^/java: /'
    echo "dataset: $DATASET_ABSOLUTE ($(find "$DATASET_DIR" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ') books)"
    echo "repetitions: $REPETITIONS, warmup: ${WARMUP}s, duration: ${DURATION}s, warm run after each start: ${WARM_RUN}s"
    echo "nginx CPUs in these tests: $BASE_NGINX_CPUS, except in the experiment named nginx<CPUs>"
    echo "limits (.env):"
    grep -E '^(SEARCH|MONGO|NGINX|HZ)_(CPUS|MEM_LIMIT)=' .env | sed 's/^/  /'
} > "$OUT/host.txt"

for repetition in $(seq 1 "$REPETITIONS"); do
    for replicas in $REPLICA_LEVELS; do
        start_system "$replicas" "$BASE_THREADS" "$BASE_POOL"
        for clients in $CLIENTS_LEVELS; do
            run_point clients "$replicas" "$BASE_THREADS" "$BASE_POOL" "$clients" "$repetition"
        done
    done
    for threads in $THREAD_LEVELS; do
        start_system 1 "$threads" "$BASE_POOL"
        for clients in $CLIENTS_LEVELS; do
            run_point threads 1 "$threads" "$BASE_POOL" "$clients" "$repetition"
        done
    done
    for pool in $POOL_LEVELS; do
        start_system 1 "$BASE_THREADS" "$pool"
        for clients in $POOL_CLIENTS; do
            run_point pool 1 "$BASE_THREADS" "$pool" "$clients" "$repetition"
        done
    done
    for cpus in $NGINX_LEVELS; do
        export NGINX_CPUS="$cpus"
        start_system 1 "$BASE_THREADS" "$BASE_POOL"
        for clients in $NGINX_CLIENTS; do
            run_point "nginx$cpus" 1 "$BASE_THREADS" "$BASE_POOL" "$clients" "$repetition"
        done
        export NGINX_CPUS="$BASE_NGINX_CPUS"
    done
done

echo "Results in $OUT"
