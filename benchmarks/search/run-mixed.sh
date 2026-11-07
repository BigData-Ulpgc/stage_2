#!/usr/bin/env bash
# Search while the rest of the system works (assignment §6.4: "how services interact when running
# concurrently ... which components become bottlenecks"). The same load on Search is measured twice
# per repetition, on the same index:
#
#   idle    nothing else runs
#   mixed   the Control, Ingestion and Indexing are processing EXTRA new books at the same time,
#           publishing new versions that Search reloads while it answers
#
# It also records how fast the pipeline is alone (building the base index) and while Search is
# under load, so the interference is measured in both directions.
#
#   benchmarks/search/run-mixed.sh           3 repetitions: about 1 h on an idle machine
#   benchmarks/search/run-mixed.sh --smoke   about 6 minutes, to check it works
#
# It needs the Control (CF-09), the index writer (DR-06b) and the synthetic dataset (DR-16), and
# generates the books it needs into data/bench-books. Each repetition starts from nothing: no
# index, no datalake, a new Hazelcast cluster. It finishes with docker compose down.
# Results: benchmarks/search/results/mixed/ (summary.csv, pipeline.csv, monitor/, host.txt).
#
# Variables: BASE (books of the base index), EXTRA (books processed during the load), CLIENTS,
# WARMUP, DURATION, REPETITIONS, OUT, MVN, COMPOSE.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
. benchmarks/search/lib.sh

COMPOSE="${COMPOSE:-docker compose}"
URL="http://localhost:8080"
SMOKE=false
[ "${1:-}" = "--smoke" ] && SMOKE=true
if $SMOKE; then
    OUT="${OUT:-benchmarks/search/results/mixed-smoke}"
    BASE="${BASE:-150}"; EXTRA="${EXTRA:-600}"; CLIENTS="${CLIENTS:-8}"
    WARMUP="${WARMUP:-3}"; DURATION="${DURATION:-10}"; REPETITIONS="${REPETITIONS:-1}"
    rm -rf "$OUT"
else
    OUT="${OUT:-benchmarks/search/results/mixed}"
    BASE="${BASE:-2000}"; EXTRA="${EXTRA:-4000}"; CLIENTS="${CLIENTS:-32 128}"
    WARMUP="${WARMUP:-15}"; DURATION="${DURATION:-60}"; REPETITIONS="${REPETITIONS:-3}"
    if [ -e "$OUT" ]; then
        echo "ERROR: $OUT already exists. Move it away first: results of two runs must not be mixed." >&2
        exit 1
    fi
fi

for file in control-module/src/main/java/es/ulpgc/bigdata/control/ControlApi.java \
            indexing-service/src/main/java/es/ulpgc/bigdata/indexing/IndexingWriter.java \
            scripts/dataset/generate_dataset.py; do
    if [ ! -f "$file" ]; then
        echo "ERROR: $file is missing. This experiment needs CF-09 (#35), DR-06b (#37) and DR-16 (#38):" >&2
        echo "       git pull, and check that the branch includes develop with them." >&2
        exit 1
    fi
done
PYTHON="${PYTHON:-$(command -v python3 || command -v python)}"
for tool in java docker curl git "$PYTHON"; do
    command -v "$tool" > /dev/null || { echo "ERROR: $tool not found" >&2; exit 1; }
done

BOOKS="./data/bench-books"
export MONGO_DATABASE=search_benchmark
export INGESTION_SOURCE=local INGESTION_LOCAL_HOST_DIR="$BOOKS"
export DATALAKE_HOST_DIR="./data/bench-mixed/datalake"
export NGINX_CPUS="${NGINX_CPUS:-2}"           # as in run-load.sh, so the two can be compared
mkdir -p "$OUT/monitor"

cleanup() {
    local status=$?
    [ "$status" -eq 0 ] || echo "FAILED (exit $status). See the messages above and $OUT/compose.log" >&2
    $COMPOSE exec -T mongo mongosh --quiet "$MONGO_DATABASE" --eval 'db.dropDatabase()' > /dev/null 2>&1 || true
    $COMPOSE down > /dev/null 2>&1 || true
}
trap cleanup EXIT

# The books: the first BASE ids build the base index, the next EXTRA arrive during the load.
TOTAL=$((BASE + EXTRA))
if [ ! -f "$BOOKS/manifest.json" ] || ! grep -q "\"count\": $TOTAL\b" "$BOOKS/manifest.json" 2>/dev/null; then
    if [ -f "$BOOKS/manifest.json" ]; then
        have="$(grep -o '"count": [0-9]*' "$BOOKS/manifest.json" | grep -o '[0-9]*')"
        [ "$have" -ge "$TOTAL" ] || { echo "ERROR: $BOOKS has $have books and $TOTAL are needed: delete it" >&2; exit 1; }
    else
        echo "Generating $TOTAL synthetic books into $BOOKS"
        "$PYTHON" scripts/dataset/generate_dataset.py --count "$TOTAL" --out "$BOOKS"
    fi
fi
grep -E '^[0-9]+$' "$BOOKS/book_ids.txt" | head -n "$BASE" > "$OUT/base-ids.txt"
grep -E '^[0-9]+$' "$BOOKS/book_ids.txt" | sed -n "$((BASE + 1)),$((BASE + EXTRA))p" > "$OUT/extra-ids.txt"

echo "Building the images"
$COMPOSE build >> "$OUT/compose.log" 2>&1

{
    echo "date_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "commit: $(git rev-parse --short HEAD)$(git diff --quiet HEAD -- . ':!benchmarks/search/results' || echo ' + uncommitted changes')"
    echo "uname: $(uname -srmo 2>/dev/null || uname -srm)"
    echo "logical_cpus: $(getconf _NPROCESSORS_ONLN 2>/dev/null || echo unknown)"
    if [ -r /proc/cpuinfo ]; then
        echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')"
        echo "memory: $(grep -m1 MemTotal /proc/meminfo | cut -d: -f2- | sed 's/^ *//')"
    fi
    echo "docker: $(docker info --format '{{.NCPU}} CPUs, {{.MemTotal}} bytes, {{.OperatingSystem}}')"
    java -version 2>&1 | sed 's/^/java: /'
    echo "dataset: synthetic DR-16, base $BASE books + $EXTRA during the load ($BASE books)"
    echo "synthetic dataset digest: $(grep -o '"digest": "[0-9a-f]*"' "$BOOKS/manifest.json")"
    echo "clients: $CLIENTS; warmup ${WARMUP}s, duration ${DURATION}s, repetitions $REPETITIONS"
    echo "deployment: 1 replica of each service, nginx with $NGINX_CPUS CPUs; limits (.env):"
    grep -E '^[A-Z]+_(CPUS|MEM_LIMIT)=|^(PUBLISH_BATCH|PUBLISH_INTERVAL_S|INGESTION_WORKERS)=' .env | sed 's/^/  /'
} > "$OUT/host.txt"

echo "repetition,phase,books,seconds,books_per_second" > "$OUT/pipeline.csv"

# One load point; the pipeline keeps working during it in the mixed phase.
load() {
    local phase="$1" clients="$2" repetition="$3" label="$1-r1-t250-p50-c$2-rep$3"
    echo "$(date -u +%H:%M:%S) $label"
    bash scripts/monitor.sh -l "$label" -o "$OUT/monitor" -d $((WARMUP + DURATION)) > /dev/null 2>&1 &
    local monitor=$!
    java -Xmx1g benchmarks/search/LoadGenerator.java --url "$URL" --clients "$clients" --live true \
        --warmup "$WARMUP" --duration "$DURATION" --out "$OUT/summary.csv" --experiment "$phase" \
        --replicas 1 --http-threads 250 --mongo-pool 50 --repetition "$repetition" | tail -1 | cut -d, -f9-18
    wait "$monitor" || echo "WARNING: the monitor failed for $label" >&2
}

for repetition in $(seq 1 "$REPETITIONS"); do
    echo "== Repetition $repetition: from nothing"
    $COMPOSE down >> "$OUT/compose.log" 2>&1 || true
    wipe_folder "$DATALAKE_HOST_DIR"
    $COMPOSE up -d --wait mongo >> "$OUT/compose.log" 2>&1
    $COMPOSE exec -T mongo mongosh --quiet "$MONGO_DATABASE" --eval 'db.dropDatabase()' > /dev/null
    $COMPOSE up -d --wait >> "$OUT/compose.log" 2>&1

    start=$SECONDS
    control_seed_and_wait "$OUT/base-ids.txt" 7200
    seconds=$((SECONDS - start))
    echo "$repetition,alone,$PUBLISHED_NOW,$seconds,$(awk "BEGIN{printf \"%.2f\", $PUBLISHED_NOW/$seconds}")" >> "$OUT/pipeline.csv"
    echo "  base index: $PUBLISHED_NOW books in $seconds s"
    sleep 10                                     # Search loads the last version
    java -Xmx1g benchmarks/search/LoadGenerator.java --url "$URL" --clients 32 --warmup 0 --duration 20 > /dev/null

    for clients in $CLIENTS; do
        load idle "$clients" "$repetition"
    done

    # The new books enter while Search answers: the same clients, now with the pipeline busy.
    ids="$(paste -sd, "$OUT/extra-ids.txt")"
    before="$(control_count PUBLISHED)"
    start=$SECONDS
    curl -sf -m 120 -X POST localhost:7000/control/jobs -H 'Content-Type: application/json' \
        -d "{\"book_ids\":[$ids]}" > /dev/null
    sleep 5
    for clients in $CLIENTS; do
        load mixed "$clients" "$repetition"
    done
    seconds=$((SECONDS - start))
    after="$(control_count PUBLISHED)"
    done_books=$((after - before))
    echo "$repetition,with_search_load,$done_books,$seconds,$(awk "BEGIN{printf \"%.2f\", $done_books/$seconds}")" >> "$OUT/pipeline.csv"
    echo "  during the load: $done_books of $EXTRA books PUBLISHED in $seconds s"
    if [ "$done_books" -ge "$EXTRA" ]; then
        echo "WARNING: the pipeline finished during the load, so part of the mixed window had it idle." >&2
        echo "         Repeat with a larger EXTRA (now $EXTRA)." >&2
    fi
done

echo "Results in $OUT"
