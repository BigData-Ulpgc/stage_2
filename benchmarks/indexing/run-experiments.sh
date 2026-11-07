#!/usr/bin/env bash
# The indexing experiments of DR-13 (enunciado §6), with the real services in docker compose and the
# limits of .env. One factor changes at a time:
#
#   threads   INDEXING_THREADS = 1, 2, 4, 8 on THREADS_SIZE books (2,000), REPETITIONS times
#   scale     200, 10,000 and 30,000 books (SCALE_SIZES), REPETITIONS times; the sizes from
#             BIG_SIZE on only BIG_REPETITIONS times (1), because each one takes hours
#   publish   POST /index/publish with 1, 10 and 50 new books (PUBLISH_SIZES), PUBLISH_REPETITIONS
#             times each (10), on an index of PUBLISH_BASE books (10,000)
#   probe     adding 100 ids to a posting list of 1,000, 10,000 and 100,000 ids, with $addToSet (what
#             the index does) and with $push: why the rate falls as the index grows
#
# Not 100,000 books: the rate falls from about 18 to 3 books/s between 0 and 10,000 books, because
# $addToSet compares each new id with the whole list, so 100,000 would take about 20 h. The curve
# of the scale runs and the probe give the estimate instead (benchmarks/indexing/README.md).
#
#   benchmarks/indexing/run-experiments.sh           all of it: about 6 h, on an idle machine
#   benchmarks/indexing/run-experiments.sh --smoke   about 5 minutes, to check it works; the numbers mean nothing
#
# How a run is measured: Ingestion takes the books of the synthetic dataset of DR-16
# (INGESTION_SOURCE=local) to the datalake once. Before each run, the database and Hazelcast start
# empty, the run's books are seeded and Ingestion brings them to index-queue, with Indexing stopped.
# Then Indexing starts and IndexingExperiment counts the INDEXED books every second, from the moment
# just before the start, until all of them are indexed. scripts/monitor.sh records CPU and memory.
# The Control is not started: it would publish and requeue books in the middle of the measure.
#
# It uses its own database (indexing_benchmark), deletes it at the end, and finishes with
# docker compose down, like the load tests of Search. The datalake and the books stay in
# benchmarks/indexing/data/experiments/ (ignored by git) for the next run; those of --smoke in
# data/experiments-smoke/.
# Results: benchmarks/indexing/results/experiments/ (the --smoke ones in results/experiments-smoke/).
# See benchmarks/indexing/README.md.
#
# Variables: EXPERIMENTS, REPETITIONS, THREAD_LEVELS, THREADS_SIZE, SCALE_SIZES, BIG_SIZE,
# BIG_REPETITIONS, PUBLISH_SIZES, PUBLISH_REPETITIONS, PUBLISH_BASE, OUT, MVN, COMPOSE.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
# Decimal points everywhere: with a Spanish locale, printf and awk write 1,5.
export LC_ALL=C

MVN="${MVN:-mvn}"
COMPOSE="${COMPOSE:-docker compose}"
EXPERIMENTS="${EXPERIMENTS:-probe threads scale publish}"
OUT_OVERRIDE="${OUT:-}"
FIRST_ID=1000001        # the first id of generate_dataset.py
BATCH=100               # INDEX_WRITE_BATCH, the default of .env

SMOKE=false
[ "${1:-}" = "--smoke" ] && SMOKE=true

if $SMOKE; then
    DATA="benchmarks/indexing/data/experiments-smoke"
    OUT="${OUT_OVERRIDE:-benchmarks/indexing/results/experiments-smoke}"
    REPETITIONS="${REPETITIONS:-1}"; THREAD_LEVELS="${THREAD_LEVELS:-1 4}"; THREADS_SIZE="${THREADS_SIZE:-200}"
    SCALE_SIZES="${SCALE_SIZES:-200 400}"; BIG_SIZE="${BIG_SIZE:-400}"; BIG_REPETITIONS="${BIG_REPETITIONS:-1}"
    PUBLISH_SIZES="${PUBLISH_SIZES:-1 10}"; PUBLISH_REPETITIONS="${PUBLISH_REPETITIONS:-2}"; PUBLISH_BASE="${PUBLISH_BASE:-200}"
    rm -rf "$OUT"
else
    DATA="benchmarks/indexing/data/experiments"
    OUT="${OUT_OVERRIDE:-benchmarks/indexing/results/experiments}"
    REPETITIONS="${REPETITIONS:-3}"; THREAD_LEVELS="${THREAD_LEVELS:-1 2 4 8}"; THREADS_SIZE="${THREADS_SIZE:-2000}"
    SCALE_SIZES="${SCALE_SIZES:-200 10000 30000}"; BIG_SIZE="${BIG_SIZE:-30000}"; BIG_REPETITIONS="${BIG_REPETITIONS:-1}"
    PUBLISH_SIZES="${PUBLISH_SIZES:-1 10 50}"; PUBLISH_REPETITIONS="${PUBLISH_REPETITIONS:-10}"; PUBLISH_BASE="${PUBLISH_BASE:-10000}"
    if [ -e "$OUT" ]; then
        echo "ERROR: $OUT already exists. Move it away first: results of two runs must not be mixed." >&2
        exit 1
    fi
fi

for tool in java "$MVN" git python3 docker; do
    if ! command -v "$tool" > /dev/null; then
        echo "ERROR: $tool not found. Requirements: JDK 17 or newer, Maven 3.9, git, Python 3 and Docker with Compose." >&2
        exit 1
    fi
done
if ! $COMPOSE version > /dev/null 2>&1; then
    echo "ERROR: '$COMPOSE' does not work. Is Docker running? For the old command: COMPOSE=docker-compose" >&2
    exit 1
fi

selected() {
    case " $EXPERIMENTS " in *" $1 "*) return 0 ;; *) return 1 ;; esac
}

# How many books the dataset needs: the largest run, and the base of the publications plus the
# books they add (they come right after the base).
NEEDED=0
need() { [ "$1" -le "$NEEDED" ] || NEEDED="$1"; }
selected threads && need "$THREADS_SIZE"
if selected scale; then for size in $SCALE_SIZES; do need "$size"; done; fi
if selected publish; then
    extra=0
    for k in $PUBLISH_SIZES; do extra=$((extra + k * PUBLISH_REPETITIONS)); done
    need $((PUBLISH_BASE + extra))
fi

# --- the system -----------------------------------------------------------------------------

# Everything compose needs, for every command below: the database of the experiments, the books
# of the dataset as the source of Ingestion, and a datalake of their own.
export MONGO_DATABASE=indexing_benchmark
export INGESTION_SOURCE=local
export INGESTION_LOCAL_HOST_DIR="./$DATA/books"
export DATALAKE_HOST_DIR="./$DATA/datalake"
export INDEX_WRITE_BATCH="$BATCH"
export INDEXING_THREADS=4

# The driver runs on the host: Hazelcast and MongoDB through the ports compose publishes on 127.0.0.1.
env_value() { grep -E "^$1=" .env | tail -1 | cut -d= -f2-; }
HZ_HOST_MEMBERS="127.0.0.1:${HZ1_HOST_PORT:-$(env_value HZ1_HOST_PORT)},127.0.0.1:${HZ2_HOST_PORT:-$(env_value HZ2_HOST_PORT)}"
MONGO_HOST_URI="mongodb://127.0.0.1:${MONGO_HOST_PORT:-$(env_value MONGO_HOST_PORT)}"
JAR="indexing-service/target/indexing-benchmarks.jar"

# MONGO_URI and HZ_MEMBERS only for the driver: compose also reads MONGO_URI, and in a container
# 127.0.0.1 is the container itself (the command that measure runs takes them out again).
driver() {
    HZ_MEMBERS="$HZ_HOST_MEMBERS" HZ_CLUSTER_NAME="${HZ_CLUSTER_NAME:-$(env_value HZ_CLUSTER_NAME)}" \
        MONGO_URI="$MONGO_HOST_URI" MONGO_DATABASE="$MONGO_DATABASE" \
        java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn -cp "$JAR" \
        es.ulpgc.bigdata.indexing.benchmark.IndexingExperiment "$@"
}

drop_database() {
    $COMPOSE exec -T mongo mongosh --quiet "$MONGO_DATABASE" --eval 'db.dropDatabase()' > /dev/null
}

cleanup() {
    local status=$?
    [ "$status" -eq 0 ] || echo "FAILED (exit $status). See the messages above and $OUT/compose.log" >&2
    echo "Cleaning up: deleting the database $MONGO_DATABASE and stopping the system"
    drop_database > /dev/null 2>&1 || true
    $COMPOSE down > /dev/null 2>&1 || true
}

# Before each run: no index, no Hazelcast state, Indexing stopped. A Hazelcast member keeps its data
# only in memory, so restarting both empties book-state and the queues; Ingestion reconnects alone.
reset_system() {
    $COMPOSE rm -sf indexing >> "$OUT/compose.log" 2>&1
    drop_database
    $COMPOSE restart hz-1 hz-2 >> "$OUT/compose.log" 2>&1
    $COMPOSE up -d --wait hz-1 hz-2 ingestion >> "$OUT/compose.log" 2>&1
}

# The first $1 books of the dataset in index-queue, with Indexing stopped. The first time Ingestion
# copies them into the datalake (about 200 books/s); afterwards it only checks they are there.
queue_books() {
    local count="$1"
    driver seed "$FIRST_ID" "$count" > /dev/null
    driver wait DOWNLOADED "$count" $((600 + count / 20))
}

# One measured run: $1 experiment, $2 INDEXING_THREADS, $3 books, $4 repetition, $5 "stats" to
# measure MongoDB afterwards. Indexing stays up afterwards, with its books INDEXED.
run_indexing() {
    local experiment="$1" threads="$2" size="$3" repetition="$4" with_stats="${5:-}"
    local label="${experiment}-t${threads}-n${size}-rep${repetition}"
    local dir="$OUT/runs/$label"
    mkdir -p "$dir"
    reset_system
    queue_books "$size"

    bash scripts/monitor.sh -l "$label" -o "$dir/monitor" -c "$($COMPOSE ps -q mongo)" > /dev/null 2>&1 &
    local monitor=$!
    export INDEXING_THREADS="$threads"
    local summary
    summary="$(driver measure "$size" $((600 + size)) "$dir/timeline.csv" -- \
        sh -c "unset MONGO_URI HZ_MEMBERS; date +%s.%N > '$dir/started' && $COMPOSE up -d --no-deps indexing >> '$OUT/compose.log' 2>&1")"
    kill -TERM "$monitor" 2> /dev/null || true
    wait "$monitor" 2> /dev/null || true
    echo "$summary" > "$dir/summary.txt"

    local stats_args=()
    if [ "$with_stats" = "stats" ]; then
        driver stats "$dir/stats.csv"
        stats_args=(--stats "$dir/stats.csv")
    fi
    python3 benchmarks/indexing/experiment_rows.py --experiment "$experiment" --threads "$threads" \
        --size "$size" --repetition "$repetition" --summary "$summary" --timeline "$dir/timeline.csv" \
        --started "$(cat "$dir/started")" --monitor "$dir/monitor" "${stats_args[@]}" >> "$OUT/summary.csv"
    local failed rate
    failed="$(echo "$summary" | cut -d, -f2)"
    rate="$(echo "$summary" | cut -d, -f6)"
    echo "  $(date -u +%H:%M:%S) $label: ${rate:-?} books/s (steady), $(echo "$summary" | cut -d, -f4) s in total"
    if [ "$failed" != "0" ]; then
        echo "WARNING: $failed books FAILED in $label; see $OUT/compose.log and the logs of indexing" >&2
    fi
}

# The time of one POST /index/publish, from inside the container of Indexing (its port is not published).
publish_seconds() {
    local answer
    answer="$($COMPOSE exec -T indexing curl -s -o /dev/null -w '%{http_code} %{time_total}' -X POST \
        http://localhost:7002/index/publish)"
    if [ "${answer%% *}" != "200" ]; then
        echo "ERROR: POST /index/publish answered ${answer%% *}" >&2
        return 1
    fi
    echo "${answer#* }"
}

# On an index of PUBLISH_BASE books, already indexed and not published: the first publication (all
# of them), then PUBLISH_REPETITIONS publications of k new books for each k of PUBLISH_SIZES.
publish_experiment() {
    local seconds next=$((FIRST_ID + PUBLISH_BASE)) indexed="$PUBLISH_BASE"
    seconds="$(publish_seconds)"
    python3 benchmarks/indexing/experiment_rows.py --experiment "index_publish_${PUBLISH_BASE}" --threads 4 \
        --size "$PUBLISH_BASE" --repetition 1 --publish-seconds "$seconds" >> "$OUT/summary.csv"
    echo "  first publication, $PUBLISH_BASE books: $seconds s"
    for k in $PUBLISH_SIZES; do
        for repetition in $(seq 1 "$PUBLISH_REPETITIONS"); do
            driver seed "$next" "$k" > /dev/null
            next=$((next + k))
            indexed=$((indexed + k))
            # After a publication its books go from INDEXED to PUBLISHED in the background
            driver wait INDEXED,PUBLISHED "$indexed" 600
            seconds="$(publish_seconds)"
            python3 benchmarks/indexing/experiment_rows.py --experiment "index_publish_${k}" --threads 4 \
                --size "$PUBLISH_BASE" --repetition "$repetition" --publish-seconds "$seconds" >> "$OUT/summary.csv"
        done
        echo "  publications of $k books: done ($PUBLISH_REPETITIONS)"
    done
}

# --- the run --------------------------------------------------------------------------------

mkdir -p "$OUT" "$DATA"
trap cleanup EXIT

echo "The dataset: $NEEDED books of DR-16 in $DATA/books"
if [ -e "$DATA/books/manifest.json" ]; then
    have="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["count"])' "$DATA/books/manifest.json")"
    if [ "$have" -lt "$NEEDED" ]; then
        echo "ERROR: $DATA/books has $have books and $NEEDED are needed. Delete $DATA and run again." >&2
        exit 1
    fi
else
    python3 scripts/dataset/generate_dataset.py --count "$NEEDED" --out "$DATA/books" | tail -1
fi
python3 scripts/dataset/generate_dataset.py --verify "$DATA/books" > /dev/null
mkdir -p "$DATA/datalake"

echo "Building the driver and the images"
"$MVN" -q -B -P benchmarks -pl indexing-service -am package -DskipTests
$COMPOSE build ingestion indexing >> "$OUT/compose.log" 2>&1

# Host, limits and dataset of this run: the report must state them (enunciado §6.1, plan §7.1).
{
    echo "date_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "commit: $(git rev-parse --short HEAD)$(git diff --quiet HEAD -- indexing-service common benchmarks/indexing scripts docker-compose.yml .env || echo ' + uncommitted changes')"
    echo "os: $(uname -srmo)"
    echo "logical_cpus: $(getconf _NPROCESSORS_ONLN 2>/dev/null || echo unknown)"
    if [ -r /proc/cpuinfo ]; then
        echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')"
        echo "memory: $(grep -m1 MemTotal /proc/meminfo | cut -d: -f2- | sed 's/^ *//')"
    fi
    echo "disk: $(df -h "$DATA" | awk 'NR == 2 { print $2 " (" $4 " free) on " $1 }')"
    echo "docker: $(docker info --format '{{.ServerVersion}}, {{.NCPU}} CPUs, {{.MemTotal}} bytes, {{.OperatingSystem}}')"
    java -version 2>&1 | sed 's/^/java (driver): /'
    echo "dataset: $DATA/books, digest $(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["digest"])' "$DATA/books/manifest.json")"
    echo "experiments: $EXPERIMENTS"
    echo "threads: $THREAD_LEVELS on $THREADS_SIZE books; scale: $SCALE_SIZES ($BIG_SIZE and more: $BIG_REPETITIONS repetitions)"
    echo "publish: $PUBLISH_SIZES new books, $PUBLISH_REPETITIONS times each, on $PUBLISH_BASE books"
    echo "repetitions: $REPETITIONS, INDEX_WRITE_BATCH: $BATCH, INDEXING_THREADS outside the threads experiment: 4"
    echo "limits (.env):"
    grep -E '^(INDEXING|MONGO|HZ|INGESTION)_(CPUS|MEM_LIMIT)=|^(MONGO_CACHE_GB|HZ_HEAP|JAVA_HEAP_PERCENT)=' .env | sed 's/^/  /'
} > "$OUT/host.txt"
python3 benchmarks/indexing/experiment_rows.py --header --experiment x --threads 1 --size 1 --repetition 1 \
    --publish-seconds 0 | head -1 > "$OUT/summary.csv"

echo "Starting Hazelcast, MongoDB and Ingestion"
$COMPOSE up -d --wait hz-1 hz-2 mongo ingestion >> "$OUT/compose.log" 2>&1
echo "Ingesting $NEEDED books into the datalake (only the first time: then it only checks them)"
reset_system
queue_books "$NEEDED"

if selected probe; then
    echo "Experiment probe: \$addToSet and \$push on posting lists of 1,000 to 100,000 ids"
    driver probe "$OUT/probe.csv"
    python3 benchmarks/indexing/experiment_rows.py --experiment index_posting_update --threads 1 --size 0 \
        --repetition 1 --probe "$OUT/probe.csv" >> "$OUT/summary.csv"
fi

if selected threads; then
    echo "Experiment threads: $THREAD_LEVELS threads, $THREADS_SIZE books"
    for repetition in $(seq 1 "$REPETITIONS"); do
        for threads in $THREAD_LEVELS; do
            run_indexing index_threads "$threads" "$THREADS_SIZE" "$repetition"
        done
    done
fi

publish_done=false
if selected scale; then
    echo "Experiment scale: $SCALE_SIZES books"
    for size in $SCALE_SIZES; do
        repetitions="$REPETITIONS"
        [ "$size" -lt "$BIG_SIZE" ] || repetitions="$BIG_REPETITIONS"
        for repetition in $(seq 1 "$repetitions"); do
            run_indexing index_scale 4 "$size" "$repetition" stats
            # The publications reuse this index instead of building another one
            if selected publish && ! $publish_done && [ "$size" = "$PUBLISH_BASE" ]; then
                echo "Experiment publish: $PUBLISH_SIZES new books on $PUBLISH_BASE"
                publish_experiment
                publish_done=true
            fi
        done
    done
fi
if selected publish && ! $publish_done; then
    echo "Experiment publish: $PUBLISH_SIZES new books on $PUBLISH_BASE (building its index first)"
    run_indexing index_publish_build 4 "$PUBLISH_BASE" 1
    publish_experiment
fi

echo "Done: $OUT/summary.csv ($(($(wc -l < "$OUT/summary.csv") - 1)) rows)"
