#!/usr/bin/env bash
# Runs the JMH benchmarks of Ingestion on the host (CF-13, enunciado §6.3) and keeps the results as CSV.
#
#   benchmarks/ingestion/run-jmh.sh           all of it: about 10 minutes, on an idle machine
#   benchmarks/ingestion/run-jmh.sh --smoke   about 2 minutes, to check it works; the numbers mean nothing
#
# The books are the synthetic dataset of DR-16 (200 books, seed 42, the same as the Indexing
# benchmarks), generated into benchmarks/ingestion/data/books (ignored by git) and checked by
# their digest. The datalakes the benchmarks write go to benchmarks/ingestion/data/work and are
# deleted as they go. Docker, MongoDB and Hazelcast are not needed: the Hazelcast benchmark starts
# its own two members inside the JVM (ports 5901-5903).
# Results: benchmarks/ingestion/results/jmh/ (the --smoke ones in results/jmh-smoke/, not to be reported).
# See benchmarks/ingestion/README.md.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

MVN="${MVN:-mvn}"
SMOKE=false
[ "${1:-}" = "--smoke" ] && SMOKE=true

BOOKS="benchmarks/ingestion/data/books"
WORK="benchmarks/ingestion/data/work"
# The digest of generate_dataset.py v1 with --count 200 and the defaults (seed 42, first id 1,000,001)
DIGEST="7fcb0fb42ed8e8738e41298614573f3cde2b418db2ba61262d3834c30296d95c"

for tool in java "$MVN" git python3; do
    if ! command -v "$tool" > /dev/null; then
        echo "ERROR: $tool not found. Requirements: JDK 17 or newer, Maven 3.9, git and Python 3." >&2
        exit 1
    fi
done

if $SMOKE; then
    OUT="benchmarks/ingestion/results/jmh-smoke"
    OPTIONS=(-wi 1 -i 1 -f 1 -w 200ms -r 200ms)
else
    OUT="benchmarks/ingestion/results/jmh"
    OPTIONS=()
    if [ -e "$OUT" ]; then
        echo "ERROR: $OUT already exists. Move it away first: results of two runs must not be mixed." >&2
        exit 1
    fi
fi

# The same books for everyone: generated once, then only checked
if [ ! -e "$BOOKS" ]; then
    python3 scripts/dataset/generate_dataset.py --count 200 --out "$BOOKS"
fi
python3 scripts/dataset/generate_dataset.py --verify "$BOOKS" > /dev/null
if ! grep -q "\"digest\": \"$DIGEST\"" "$BOOKS/manifest.json"; then
    echo "ERROR: $BOOKS is not the dataset of the benchmarks (digest $DIGEST). Delete it and run again." >&2
    exit 1
fi

# Left over by a run that was stopped: it only takes disk
rm -rf "$WORK"
mkdir -p "$WORK"

"$MVN" -q -B -P benchmarks -pl ingestion-service -am package -DskipTests
JAR="ingestion-service/target/ingestion-benchmarks.jar"
mkdir -p "$OUT"
export INGESTION_BENCH_BOOKS="$ROOT/$BOOKS"
export INGESTION_BENCH_WORK="$ROOT/$WORK"

# Fixed locale: otherwise the CSV has decimal commas on a machine in Spanish.
run_jmh() {
    java -Duser.language=en -Duser.country=US -jar "$JAR" "$@"
}

# Host and JVM of this run: the report must state them (enunciado §6.1).
{
    echo "date_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "commit: $(git rev-parse --short HEAD)$(git diff --quiet HEAD -- ingestion-service common || echo ' + uncommitted changes')"
    echo "os: $(uname -srm)"                      # not -a: it would print the host name
    echo "logical_cpus: $(getconf _NPROCESSORS_ONLN 2>/dev/null || echo unknown)"
    if [ -r /proc/cpuinfo ]; then
        echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')"
        echo "memory: $(grep -m1 MemTotal /proc/meminfo | cut -d: -f2- | sed 's/^ *//')"
    elif command -v sysctl > /dev/null; then
        echo "cpu: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown)"
        echo "memory: $(sysctl -n hw.memsize 2>/dev/null || echo unknown) bytes"
    fi
    # The write benchmarks go to this file system: the disk under the datalake matters
    if [ -r /proc/mounts ]; then
        echo "work_filesystem: $(df -PT "$WORK" | awk 'NR == 2 {print $1, $2}')"
    else
        echo "work_filesystem: $(df -P "$WORK" | awk 'NR == 2 {print $1}')"
    fi
    java -version 2>&1 | sed 's/^/java: /'
    echo "books: $BOOKS, digest $DIGEST"
} > "$OUT/host.txt"

# The books each benchmark uses, with their size: to turn a time per book into MB/s
java -cp "$JAR" es.ulpgc.bigdata.ingestion.benchmark.BenchmarkBooks > "$OUT/books.csv"

for benchmark in SplitterBenchmark DatalakeWriteBenchmark IngestBenchmark CoordinationBenchmark; do
    run_jmh "$benchmark" ${OPTIONS[@]+"${OPTIONS[@]}"} -foe true -rf csv -rff "$OUT/$benchmark.csv"
done

rm -rf "$WORK"
echo "Results in $OUT"
