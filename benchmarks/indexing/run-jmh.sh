#!/usr/bin/env bash
# Runs the JMH benchmarks of Indexing on the host (DR-12, enunciado §6.3) and keeps the results as CSV.
#
#   benchmarks/indexing/run-jmh.sh           all of it: about 7 minutes, on an idle machine
#   benchmarks/indexing/run-jmh.sh --smoke   about 1 minute, to check it works; the numbers mean nothing
#
# The books are the synthetic dataset of DR-16 (200 books, seed 42), generated into
# benchmarks/indexing/data/books (ignored by git) and checked by their digest.
# Results: benchmarks/indexing/results/jmh/ (the --smoke ones in results/jmh-smoke/, not to be reported).
# See benchmarks/indexing/README.md.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

MVN="${MVN:-mvn}"
SMOKE=false
[ "${1:-}" = "--smoke" ] && SMOKE=true

BOOKS="benchmarks/indexing/data/books"
# The digest of generate_dataset.py v1 with --count 200 and the defaults (seed 42, first id 1,000,001)
DIGEST="7fcb0fb42ed8e8738e41298614573f3cde2b418db2ba61262d3834c30296d95c"

for tool in java "$MVN" git python3; do
    if ! command -v "$tool" > /dev/null; then
        echo "ERROR: $tool not found. Requirements: JDK 17 or newer, Maven 3.9, git and Python 3." >&2
        exit 1
    fi
done

if $SMOKE; then
    OUT="benchmarks/indexing/results/jmh-smoke"
    OPTIONS=(-wi 1 -i 1 -f 1 -w 200ms -r 200ms)
else
    OUT="benchmarks/indexing/results/jmh"
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

"$MVN" -q -B -P benchmarks -pl indexing-service -am package -DskipTests
JAR="indexing-service/target/indexing-benchmarks.jar"
mkdir -p "$OUT"
export INDEXING_BENCH_BOOKS="$ROOT/$BOOKS"

# Fixed locale: otherwise the CSV has decimal commas on a machine in Spanish.
run_jmh() {
    java -Duser.language=en -Duser.country=US -jar "$JAR" "$@"
}

# Host and JVM of this run: the report must state them (enunciado §6.1).
{
    echo "date_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "commit: $(git rev-parse --short HEAD)$(git diff --quiet HEAD -- indexing-service common || echo ' + uncommitted changes')"
    echo "os: $(uname -srmo)"                     # not -a: it would print the host name
    echo "logical_cpus: $(getconf _NPROCESSORS_ONLN 2>/dev/null || echo unknown)"
    if [ -r /proc/cpuinfo ]; then
        echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')"
        echo "memory: $(grep -m1 MemTotal /proc/meminfo | cut -d: -f2- | sed 's/^ *//')"
    fi
    java -version 2>&1 | sed 's/^/java: /'
    echo "books: $BOOKS, digest $DIGEST"
} > "$OUT/host.txt"

# The books each benchmark measures, with their size: to turn a time per book into MB/s
java -cp "$JAR" es.ulpgc.bigdata.indexing.benchmark.BenchmarkBooks > "$OUT/books.csv"

for benchmark in TokenizerBenchmark MetadataParserBenchmark AddDocumentBenchmark PrepareBatchBenchmark; do
    run_jmh "$benchmark" ${OPTIONS[@]+"${OPTIONS[@]}"} -foe true -rf csv -rff "$OUT/$benchmark.csv"
done

echo "Results in $OUT"
