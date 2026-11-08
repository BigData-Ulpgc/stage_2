#!/usr/bin/env bash
# Integration benchmarks of Ingestion on Docker Compose (CF-13, enunciado §6.2 and §6.4, plan §9),
# changing one factor at a time, with the local source (no network) and the synthetic books of DR-16:
#
#   replicas   --scale ingestion=1, 2, 4 with INGESTION_WORKERS=4, 10,000 books
#   workers    INGESTION_WORKERS=1, 2, 8 with 1 replica, 10,000 books (4 is the replicas=1 point)
#   pipeline   seed -> PUBLISHED with Indexing running, 200 and 10,000 books, 1 replica of each
#
# Each point starts from an empty cluster, an empty datalake and an empty database of its own
# (ingestion_benchmark, deleted at the end), so it never touches the data of other work. It
# finishes with docker compose down.
#
#   benchmarks/ingestion/run-integration.sh           all of it: about 1 h 15 min, on an idle machine
#   benchmarks/ingestion/run-integration.sh --smoke   about 5 minutes, to check it works; the numbers mean nothing
#
# Results: benchmarks/ingestion/results/integration/ (the --smoke ones in results/integration-smoke/):
#   summary.csv   the common format of plan §7.1, one row per metric and point
#   timeline/     books ingested, indexed and published every 0.2 s of each point
#   status/       GET /status of every Ingestion replica at the end (coordination_cost, CF-12)
#   monitor/      docker stats of every container during each point (scripts/monitor.sh)
#   host.txt      host, Docker, limits and datasets
# See benchmarks/ingestion/README.md.
#
# Variables: EXPERIMENTS (default "replicas workers pipeline"), REPETITIONS, COMPOSE, OUT.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

COMPOSE="${COMPOSE:-docker compose}"
EXPERIMENTS="${EXPERIMENTS:-replicas workers pipeline}"
OUT_OVERRIDE="${OUT:-}"
SMOKE=false
[ "${1:-}" = "--smoke" ] && SMOKE=true

SMALL="benchmarks/ingestion/data/books"
LARGE="benchmarks/ingestion/data/books-10000"
# The digests of generate_dataset.py v1 with --count 200 and 10000 and the defaults (seed 42)
SMALL_DIGEST="7fcb0fb42ed8e8738e41298614573f3cde2b418db2ba61262d3834c30296d95c"
LARGE_DIGEST="d2bd9f45e701ed144922945ea67ad3bbe2cbfdf33eb0f3925db8ba97d864572f"

if $SMOKE; then
    OUT="${OUT_OVERRIDE:-benchmarks/ingestion/results/integration-smoke}"
    REPETITIONS="${REPETITIONS:-1}"
    REPLICA_LEVELS="1 2"; WORKER_LEVELS="2"; BIG="$SMALL"; PIPELINE_SETS="$SMALL"
    rm -rf "$OUT"
else
    OUT="${OUT_OVERRIDE:-benchmarks/ingestion/results/integration}"
    REPETITIONS="${REPETITIONS:-3}"
    REPLICA_LEVELS="1 2 4"; WORKER_LEVELS="1 2 8"; BIG="$LARGE"; PIPELINE_SETS="$SMALL $LARGE"
    if [ -e "$OUT" ]; then
        echo "ERROR: $OUT already exists. Move it away first: results of two runs must not be mixed." >&2
        exit 1
    fi
fi
BASE_WORKERS=4

for tool in docker curl python3 git; do
    if ! command -v "$tool" > /dev/null; then
        echo "ERROR: $tool not found. Requirements: Docker with Compose, curl, git and Python 3." >&2
        exit 1
    fi
done
if ! $COMPOSE version > /dev/null 2>&1; then
    echo "ERROR: '$COMPOSE' does not work. Is Docker running? For the old command: COMPOSE=docker-compose" >&2
    exit 1
fi

# The same books for everyone: generated once, then only checked
dataset() {
    local dir="$1" count="$2" digest="$3"
    if [ ! -e "$dir" ]; then
        echo "Generating $count synthetic books into $dir (once)"
        python3 scripts/dataset/generate_dataset.py --count "$count" --out "$dir" > /dev/null
    fi
    python3 scripts/dataset/generate_dataset.py --verify "$dir" > /dev/null
    if ! grep -q "\"digest\": \"$digest\"" "$dir/manifest.json"; then
        echo "ERROR: $dir is not the dataset of the benchmarks (digest $digest). Delete it and run again." >&2
        exit 1
    fi
}
dataset "$SMALL" 200 "$SMALL_DIGEST"
$SMOKE || dataset "$LARGE" 10000 "$LARGE_DIGEST"

# A datalake and a database of their own: the Control rebuilds book-state from every <id>._READY
# and every published book it finds (CF-10), so the books of other work would end up here.
export DATALAKE_HOST_DIR="./data/bench-ingestion/datalake"
export MONGO_DATABASE=ingestion_benchmark
export INGESTION_SOURCE=local

# Empties a bind-mounted folder from a container: on Linux the services write as root, so a
# plain rm would need sudo (as benchmarks/search/lib.sh does).
wipe_datalake() {
    local absolute
    mkdir -p "$DATALAKE_HOST_DIR"
    absolute="$(cd "$DATALAKE_HOST_DIR" && { pwd -W 2>/dev/null || pwd; })"
    MSYS_NO_PATHCONV=1 docker run --rm -v "$absolute:/wipe" --entrypoint sh "nginx:${NGINX_VERSION:-1.28-alpine}" \
        -c 'rm -rf /wipe/* /wipe/.[!.]* 2>/dev/null; true'
}

drop_database() {
    $COMPOSE exec -T mongo mongosh --quiet "$MONGO_DATABASE" --eval 'db.dropDatabase()' > /dev/null
}

cleanup() {
    local status=$?
    [ "$status" -eq 0 ] || echo "FAILED (exit $status). See the messages above and $OUT/compose.log" >&2
    echo "Cleaning up: deleting the database $MONGO_DATABASE and the datalake, stopping the system"
    drop_database 2>/dev/null || true
    $COMPOSE down > /dev/null 2>&1 || true
    wipe_datalake > /dev/null 2>&1 || true
}

# One point: a clean system with this configuration, the measure, the system down again.
run_point() {
    local experiment="$1" replicas="$2" workers="$3" books="$4" target="$5" repetition="$6"
    local services="mongo hz-1 hz-2 control ingestion" label monitor
    [ "$target" = "published" ] && services="$services indexing"
    label="$experiment-n$(grep -cE '^[0-9]+$' "$books/book_ids.txt")-r$replicas-w$workers-rep$repetition"
    echo "$(date -u +%H:%M:%S) $label"

    # Hazelcast keeps nothing across a down: book-state starts empty
    $COMPOSE down >> "$OUT/compose.log" 2>&1
    wipe_datalake
    export INGESTION_WORKERS="$workers" INGESTION_LOCAL_HOST_DIR="./$books"
    $COMPOSE up -d --wait mongo >> "$OUT/compose.log" 2>&1
    drop_database                                       # before the Control's recovery reads it
    # shellcheck disable=SC2086
    $COMPOSE up -d --wait --scale ingestion="$replicas" $services >> "$OUT/compose.log" 2>&1

    bash scripts/monitor.sh -l "$label" -o "$OUT/monitor" > /dev/null 2>&1 &
    monitor=$!
    python3 -I benchmarks/ingestion/integration.py --ids "$books/book_ids.txt" --target "$target" \
        --experiment "$experiment" --replicas "$replicas" --workers "$workers" --repetition "$repetition" \
        --out "$OUT" --compose "$COMPOSE"
    kill -INT "$monitor" 2>/dev/null || true
    wait "$monitor" 2>/dev/null || true
}

# The levels of an experiment, or nothing if EXPERIMENTS does not include it.
selected() {
    case " $EXPERIMENTS " in
        *" $1 "*) echo "$2" ;;
    esac
}

mkdir -p "$OUT"
trap cleanup EXIT

echo "Building the images"
$COMPOSE build control ingestion indexing >> "$OUT/compose.log" 2>&1

# Host, limits and datasets of this run: the report must state them (enunciado §6.1).
{
    echo "date_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "commit: $(git rev-parse --short HEAD)$(git diff --quiet HEAD -- ingestion-service control-module common docker-compose.yml .env || echo ' + uncommitted changes')"
    echo "os: $(uname -srm)"                      # not -a: it would print the host name
    echo "logical_cpus: $(getconf _NPROCESSORS_ONLN 2>/dev/null || echo unknown)"
    if [ -r /proc/cpuinfo ]; then
        echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')"
        echo "memory: $(grep -m1 MemTotal /proc/meminfo | cut -d: -f2- | sed 's/^ *//')"
        echo "datalake_filesystem: $(df -PT "$DATALAKE_HOST_DIR" 2>/dev/null | awk 'NR == 2 {print $1, $2}')"
    fi
    echo "docker: $(docker info --format '{{.NCPU}} CPUs, {{.MemTotal}} bytes, {{.OperatingSystem}}')"
    echo "datasets: $SMALL (200, digest $SMALL_DIGEST)$($SMOKE || echo ", $LARGE (10000, digest $LARGE_DIGEST)")"
    echo "source: local; seeding: POST /control/jobs in requests of 10,000 ids; polling every 0.2 s"
    echo "repetitions: $REPETITIONS; workers per replica unless varied: $BASE_WORKERS"
    echo "limits (.env, per replica):"
    grep -E '^(INGESTION|CONTROL|INDEXING|MONGO|HZ)_(CPUS|MEM_LIMIT)=|^(HZ_HEAP|MONGO_CACHE_GB|PUBLISH_BATCH|PUBLISH_INTERVAL_S|INDEX_WRITE_BATCH|INDEXING_THREADS|LEASE_SECONDS)=' .env | sed 's/^/  /'
} > "$OUT/host.txt"

for repetition in $(seq 1 "$REPETITIONS"); do
    for replicas in $(selected replicas "$REPLICA_LEVELS"); do
        run_point replicas "$replicas" "$BASE_WORKERS" "$BIG" ingested "$repetition"
    done
    for workers in $(selected workers "$WORKER_LEVELS"); do
        run_point workers 1 "$workers" "$BIG" ingested "$repetition"
    done
    for books in $(selected pipeline "$PIPELINE_SETS"); do
        run_point pipeline 1 "$BASE_WORKERS" "$books" published "$repetition"
    done
done

echo "Results in $OUT"
