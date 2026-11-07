# Runbook: the benchmarks of the Search Service

For whoever runs the measures, a person or an assistant reading this file. **Run, do not change.** If
something fails, report it with `benchmarks/search/results/run-all.log`; do not edit code or limits to make
it pass, because that would change what is measured. The owner of this part is Daniel Perdomo.

## The short version

```bash
git fetch origin && git switch test/DP-12-benchmarks && git pull
bash benchmarks/search/run-all.sh --check     # 1 minute: is the machine ready?
bash benchmarks/search/run-all.sh             # about 6 hours, unattended
git push origin test/DP-12-benchmarks         # the script already made the commit
```

Before the long run: plugged in, no suspension when idle, and nothing else open (no builds, browsers,
other containers or calls). Then send the owner the RAM, the disk type and anything unusual.

If it stops (a crash, a reboot), **run the same command again**: finished steps are skipped and an
unfinished one starts again. A failed step does not stop the others.

## What it runs, and why

The assignment (§6) asks for two levels of benchmarks on **one machine with known, fixed resources**,
reported with its specification:

| Step | Results | Time | What the assignment asks |
|---|---|---|---|
| 1. Checks | — | 1 min | §6.1: a known host. It records CPU, RAM, Docker and Java in each `host.txt`, without the host name |
| 2. JMH | `results/jmh/` | 30 min | §6.3, **mandatory**: microbenchmarks on the host, in `src/test/java`, at least 5 warm-up and 10 measurement iterations, fixed heap, CSV. Intersection, filters, pagination and threads |
| 3. Load, 15 books | `results/load/` | 2 h | §6.2 and §6.4: query latency with p95 and concurrency; replicas, threads and client concurrency kept apart, one factor at a time, 3 repetitions. It runs `validate-api.sh` first and stops if it fails |
| 4. Load, 200 real books | `results/load-200/` | 1 h | Plan DP-12: the effect of the size of the index. It downloads the 200 books of `shared/book_ids.txt` once |
| 5. Load, 10,000 synthetic books | `results/load-10000/` | 1 h 30 min | Plan DP-12: does MongoDB saturate before Search? The index is built by the whole pipeline from the dataset of DR-16 |
| 6. Search with the pipeline running | `results/mixed/` | 1 h | §6.4: *"how services interact when running concurrently"*. The same load with nothing else running, and while 4,000 new books are being ingested, indexed and published |
| 7. Analysis and commit | `results/analysis/` | 1 min | §6.5: tables with mean and standard deviation, figures, and the common CSV of the plan |

Steps 5 and 6 need the Control (CF-09), the index writer (DR-06b) and the synthetic dataset (DR-16) in the
branch. If they are not there yet, the script says so and skips them; run it again after `git pull`.

## Requirements

Linux, JDK 17 or newer, Maven, Docker with Compose, git, curl, Python 3 (`matplotlib` for the figures,
optional), 8 GB free on disk, and `stage_1` cloned next to `stage_2`. Ports 8080, 7000, 27017, 5701 and 5702
must be free. `--check` tells what is missing.

## What must hold in the results

- `summary.csv`: `http_errors`, `wrong_results` and `connection_errors` are 0. If not, keep the results as
  they are and report it: it is a result too.
- `load/validation.txt` ends with `failed 0`.
- `mixed/pipeline.csv`: the `with_search_load` rows must not have finished every book (the log warns).
- `analysis/tables.md` lists its warnings at the top.

## What it does not do

It does not change `.env`, it uses its own database (`search_benchmark`, deleted at the end) and its own
data folders (`data/bench-*`), and it ends with `docker compose down`. The other benchmarks of the group
(Ingestion, Indexing, coordination) have their own scripts.
