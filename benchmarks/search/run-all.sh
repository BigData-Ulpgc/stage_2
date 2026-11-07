#!/usr/bin/env bash
# Every benchmark of the Search Service, one after the other, unattended (benchmarks/search/RUNBOOK.md).
#
#   bash benchmarks/search/run-all.sh            all of it: about 6 h on an idle machine
#   bash benchmarks/search/run-all.sh --check    only the checks of the machine, 1 minute
#
# A step whose results are finished or already committed is skipped, so after a failure the same
# command goes on from where it stopped. At the end it commits the results; pushing them is left to you.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
RESULTS="benchmarks/search/results"
LOG="$RESULTS/run-all.log"
mkdir -p "$RESULTS"
PYTHON="${PYTHON:-$(command -v python3 || command -v python || true)}"

say() { echo "$(date -u +%H:%M:%S) $*" | tee -a "$LOG"; }
fail() { say "STOP: $*"; exit 1; }

# ---- 1. The machine: the results are only valid on a known, idle host (assignment §6.1) ----
check() {
    local ok=true
    say "Checking the machine"
    [ "$(uname -s)" = Linux ] || say "WARNING: not Linux. The valid measures of the group come from one Linux machine."
    for tool in git java mvn docker curl "$PYTHON"; do
        command -v "$tool" > /dev/null || { say "MISSING: $tool"; ok=false; }
    done
    docker compose version > /dev/null 2>&1 || { say "MISSING: docker compose (or Docker is not running)"; ok=false; }
    local java_major
    java_major="$(java -version 2>&1 | grep -o 'version "[0-9]*' | grep -o '[0-9]*$')"
    [ "${java_major:-0}" -ge 17 ] || { say "MISSING: JDK 17 or newer (found ${java_major:-none})"; ok=false; }
    "$PYTHON" -c 'import matplotlib' 2> /dev/null || say "NOTE: no matplotlib, the figures will be missing (pip install matplotlib); everything else works"
    local cpus mem_gb free_gb docker_mem_gb load
    cpus="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 0)"
    mem_gb="$(awk '/MemTotal/ {printf "%d", $2/1048576}' /proc/meminfo 2>/dev/null || echo 0)"
    docker_mem_gb="$(docker info --format '{{.MemTotal}}' 2>/dev/null | awk '{printf "%d", $1/1073741824}')"
    free_gb="$(df -Pk . | awk 'NR==2 {printf "%d", $4/1048576}')"
    load="$(cut -d' ' -f1 /proc/loadavg 2>/dev/null || echo '?')"
    say "Host: $cpus logical CPUs, ${mem_gb} GB of RAM, Docker ${docker_mem_gb} GB, ${free_gb} GB free on disk, load $load"
    [ "${free_gb:-0}" -ge 8 ] || { say "MISSING: at least 8 GB free on disk (the 10,000 synthetic books and their datalake)"; ok=false; }
    [ "${docker_mem_gb:-0}" -ge 7 ] || say "WARNING: Docker has under 7 GB; with 4 replicas of Search the limits add up to about 7 GB"
    [ "$cpus" -ge 4 ] || say "WARNING: under 4 CPUs; the assignment's reference machine has 4 cores"
    ss -ltn 2>/dev/null | grep -qE ':(8080|7000|27017|5701) ' && say "WARNING: a port the system needs is in use (8080, 7000, 27017 or 5701)"
    [ -z "$(git status --porcelain -- . ':!benchmarks/search/results' ':!data')" ] \
        || say "WARNING: uncommitted changes; host.txt will say so. Better: git stash, or a clean clone"
    [ -d "${DATASET_DIR:-../stage_1/sample_dataset/book}" ] || { say "MISSING: ../stage_1/sample_dataset (clone stage_1 next to stage_2)"; ok=false; }
    curl -s -m 10 -o /dev/null -w '%{http_code}' https://www.gutenberg.org/ | grep -q '^[23]' \
        || say "WARNING: Project Gutenberg does not answer; the 200 real books (step 4) will be skipped"
    for file in control-module/src/main/java/es/ulpgc/bigdata/control/ControlApi.java \
                indexing-service/src/main/java/es/ulpgc/bigdata/indexing/IndexingWriter.java \
                scripts/dataset/generate_dataset.py; do
        [ -f "$file" ] || say "NOTE: $file is not in this branch yet; steps 5 and 6 need it and will be skipped"
    done
    $ok || fail "fix the MISSING items above and run it again"
    say "The machine is ready. During the run: plugged in, no suspension, nothing else open."
}

pipeline_ready() {
    [ -f control-module/src/main/java/es/ulpgc/bigdata/control/ControlApi.java ] \
        && [ -f indexing-service/src/main/java/es/ulpgc/bigdata/indexing/IndexingWriter.java ] \
        && [ -f scripts/dataset/generate_dataset.py ]
}

step() {    # step NAME RESULT_DIR COMMAND...
    local name="$1" dir="$2"; shift 2
    if [ -f "$dir/.done" ] || git ls-files --error-unmatch "$dir" > /dev/null 2>&1; then
        say "$name: already done ($dir is finished or committed), skipped"
        return 0
    fi
    if [ -e "$dir" ]; then
        local old="$dir.failed-$(date -u +%Y%m%dT%H%M%S)"
        mv "$dir" "$old"
        say "$name: an unfinished run was moved to $old"
    fi
    say "$name: starting"
    local start=$SECONDS
    if "$@" >> "$LOG" 2>&1; then
        touch "$dir/.done"
        say "$name: done in $(( (SECONDS - start) / 60 )) min"
    else
        say "$name: FAILED, see $LOG. The other steps go on."
        FAILED_STEPS="$FAILED_STEPS $name"
    fi
}

check
[ "${1:-}" = "--check" ] && exit 0
FAILED_STEPS=""
export MVN="${MVN:-mvn}"

# ---- 2. JMH, the mandatory microbenchmarks (assignment §6.3) ----
step "JMH" "$RESULTS/jmh" bash benchmarks/search/run-jmh.sh

# ---- 3. Load on Search: clients, replicas, threads, pool, nginx (§6.2, §6.4) ----
step "Load, 15 books" "$RESULTS/load" bash benchmarks/search/run-load.sh

# ---- 4. The same with an index of 200 real books (plan DP-12: index size) ----
if curl -s -m 10 -o /dev/null -w '%{http_code}' https://www.gutenberg.org/ | grep -q '^[23]'; then
    step "Load, 200 real books" "$RESULTS/load-200" env INDEX_IDS=shared/book_ids.txt \
        OUT="$RESULTS/load-200" EXPERIMENTS=clients bash benchmarks/search/run-load.sh
else
    say "Load, 200 real books: skipped, Project Gutenberg does not answer"
fi

if pipeline_ready; then
    # ---- 5. Index of 10,000 synthetic books, built by the whole pipeline (plan: does MongoDB saturate first?) ----
    if [ ! -f data/bench-books/manifest.json ]; then
        say "Generating the synthetic books (DR-16)"
        "$PYTHON" scripts/dataset/generate_dataset.py --count 10000 --out data/bench-books >> "$LOG" 2>&1 \
            || fail "the synthetic dataset could not be generated, see $LOG"
    fi
    step "Load, 10,000 synthetic books" "$RESULTS/load-10000" env INDEX_IDS=data/bench-books/book_ids.txt \
        INGESTION_SOURCE=local INGESTION_LOCAL_HOST_DIR=./data/bench-books OUT="$RESULTS/load-10000" \
        EXPERIMENTS=clients bash benchmarks/search/run-load.sh

    # ---- 6. Search while the pipeline works (§6.4, integration) ----
    step "Search with the pipeline running" "$RESULTS/mixed" env PYTHON="$PYTHON" bash benchmarks/search/run-mixed.sh
else
    say "Steps 5 and 6 skipped: this branch does not have CF-09, DR-06b and DR-16 yet (git pull when they are in develop)"
fi

# ---- 7. Tables and figures, then commit ----
say "Analysis"
"$PYTHON" benchmarks/search/analyze.py >> "$LOG" 2>&1 || say "analyze.py failed, see $LOG (the measures are kept)"
git add -f benchmarks/search/results/jmh benchmarks/search/results/load* benchmarks/search/results/mixed \
    benchmarks/search/results/analysis benchmarks/search/results/run-all.log 2>/dev/null
git commit -q -m "test(search): benchmark results (DP-12)" \
    && say "Results committed. Now: git push origin $(git branch --show-current)" \
    || say "Nothing new to commit"
[ -z "$FAILED_STEPS" ] && say "All done" || say "Finished with failed steps:$FAILED_STEPS (see $LOG)"
