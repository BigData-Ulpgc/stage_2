#!/usr/bin/env bash
# Runs the JMH benchmarks of Search on the host (enunciado §6.3) and keeps the results as CSV.
#
#   benchmarks/search/run-jmh.sh           all of it: about 30 minutes, on an idle machine
#   benchmarks/search/run-jmh.sh --smoke   about 1 minute, to check it works; the numbers mean nothing
#
# Results: benchmarks/search/results/jmh/ (the --smoke ones in results/jmh-smoke/, not to be reported).
# See benchmarks/search/README.md.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

MVN="${MVN:-mvn}"
SMOKE=false
[ "${1:-}" = "--smoke" ] && SMOKE=true

for tool in java "$MVN" git; do
    if ! command -v "$tool" > /dev/null; then
        echo "ERROR: $tool not found. Requirements: JDK 17 or newer, Maven 3.9 and git." >&2
        exit 1
    fi
done

if $SMOKE; then
    OUT="benchmarks/search/results/jmh-smoke"
    OPTIONS=(-wi 1 -i 1 -f 1 -w 200ms -r 200ms)
    SINGLE_THREAD_BOOKS=(-p books=10000)
    SCALING_BOOKS=(-p books=10000)
else
    OUT="benchmarks/search/results/jmh"
    OPTIONS=()
    SINGLE_THREAD_BOOKS=()
    SCALING_BOOKS=(-p books=100000)
    if [ -e "$OUT" ]; then
        echo "ERROR: $OUT already exists. Move it away first: results of two runs must not be mixed." >&2
        exit 1
    fi
fi

"$MVN" -q -B -P benchmarks -pl search-service -am package -DskipTests
JAR="search-service/target/search-benchmarks.jar"
mkdir -p "$OUT"

# Fixed locale: otherwise the CSV has decimal commas on a machine in Spanish.
run_jmh() {
    java -Duser.language=en -Duser.country=US -jar "$JAR" "$@"
}

# Host and JVM of this run: the report must state them (enunciado §6.1).
{
    echo "date_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "commit: $(git rev-parse --short HEAD)$(git diff --quiet HEAD -- search-service || echo ' + uncommitted changes')"
    echo "uname: $(uname -a)"
    echo "logical_cpus: $(getconf _NPROCESSORS_ONLN 2>/dev/null || echo unknown)"
    if [ -r /proc/cpuinfo ]; then
        echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')"
        echo "memory: $(grep -m1 MemTotal /proc/meminfo | cut -d: -f2- | sed 's/^ *//')"
    fi
    java -version 2>&1 | sed 's/^/java: /'
} > "$OUT/host.txt"

# One thread per run: the effect of each factor on one core, without contention.
for benchmark in IntersectionBenchmark FilterBenchmark PaginationBenchmark; do
    run_jmh "$benchmark" ${OPTIONS[@]+"${OPTIONS[@]}"} ${SINGLE_THREAD_BOOKS[@]+"${SINGLE_THREAD_BOOKS[@]}"} \
        -foe true -rf csv -rff "$OUT/$benchmark.csv"
done

# The search is meant to run concurrently (immutable snapshot), so here the threads do change.
for threads in 1 2 4 8; do
    run_jmh IntersectionBenchmark ${OPTIONS[@]+"${OPTIONS[@]}"} "${SCALING_BOOKS[@]}" -p terms=common_frequent \
        -t "$threads" -foe true -rf csv -rff "$OUT/threads-$threads.csv"
done

echo "Results in $OUT"
