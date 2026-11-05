# Search benchmarks

## JMH: the search code on its own

They measure `SearchEngine` on a synthetic index held in memory, without MongoDB and without HTTP:
intersection, filters and pagination with 200, 10,000 and 100,000 books, and the intersection with
1, 2, 4 and 8 threads. The load tests, which do include MongoDB, are a separate step.

**Requirements:** JDK 17 or newer, Maven 3.9, git and bash. Docker and MongoDB are not needed.

### How to run them

```bash
git fetch origin
git switch test/DP-12-benchmarks

# 1. Check that it works (about 1 minute; these numbers are not valid)
bash benchmarks/search/run-jmh.sh --smoke

# 2. The real run (about 30 minutes)
bash benchmarks/search/run-jmh.sh
```

Set `MVN=/path/to/mvn` if Maven is not in the `PATH`.

**During the real run,** the numbers are only valid if the machine is doing nothing else:

- Plugged in, in performance mode if the system has one, and without suspending when idle.
- Everything else closed, including other builds, Docker containers, browsers and video calls.

If the run stops or something else used the machine, rename `results/jmh/` and start again. The script
refuses to write over existing results, because two runs must not be mixed.

### What to hand in

Commit `benchmarks/search/results/jmh/` to this branch:

```bash
git pull origin test/DP-12-benchmarks
git add benchmarks/search/results/jmh
git commit -m "test(search): JMH results (DP-12)"
git push origin test/DP-12-benchmarks
```

Also state the RAM, the storage and anything unusual that happened during the run. `host.txt` records
the rest: date, commit, operating system, CPU, memory and JVM.

### Reading the results

| File | What changes | Fixed |
|---|---|---|
| `IntersectionBenchmark.csv` | Books and terms of the query | No filters, first page, 1 thread |
| `FilterBenchmark.csv` | Books and filter | Term `common`, first page, 1 thread |
| `PaginationBenchmark.csv` | Books, position of the page and its size | Term `common`, 1 thread |
| `threads-N.csv` | Threads | 100,000 books, terms `common` and `frequent` |

The score is the mean time per search in microseconds, with its 99.9 % error. Each configuration runs
5 warm-up and 10 measurement iterations of 1 s, in 2 separate JVMs with a 4 GB heap (enunciado §6.3).

**Limitation:** each term is assigned to books at random and independently, while real words tend to
appear together. The results compare the cost of each step; they do not predict the response time of
real queries.

## Load tests: the system under concurrent requests

`LoadGenerator.java` sends `GET /search` through nginx with the queries of `shared/queries.txt`.
`run-load.sh` starts the system, loads the dataset, repeats each configuration 3 times, records CPU and
memory with `scripts/monitor.sh`, and stops everything at the end. One factor changes at a time:

| Experiment | What changes | Fixed |
|---|---|---|
| `clients` | 1, 8, 32, 128, 256 clients with 1, 2 and 4 replicas | 250 threads, pool of 50, nginx with 2 CPUs |
| `threads` | 16 and 64 Jetty threads, with the same clients | 1 replica, pool of 50, nginx with 2 CPUs |
| `pool` | 10 and 100 MongoDB connections, with 32, 128 and 256 clients | 1 replica, 250 threads, nginx with 2 CPUs |
| `nginx0.5` | nginx with 0.5 CPUs, with 32, 128 and 256 clients | 1 replica, 250 threads, pool of 50 |

**Why nginx has 2 CPUs here:** the deployment gives it 0.5, and with that limit it saturates before
Search, which would hide the effect of the replicas. `nginx0.5` measures that limit on its own.

**Requirements:** the same as JMH, plus Docker with Compose and curl, and `stage_1` next to `stage_2`
(or `DATASET_DIR=/path/to/sample_dataset/book`). With 4 replicas, the limits add up to 14 CPUs and about
7 GB. Port 8080 must be free.

```bash
# 1. Check that it works (about 3 minutes; these numbers are not valid)
bash benchmarks/search/run-load.sh --smoke

# 2. The real run (about 1 h 45 min, on an idle machine)
bash benchmarks/search/run-load.sh
```

The loader **empties** the database it receives, so the script uses its own, `search_benchmark`, and
deletes it at the end. The script ends with `docker compose down`.

**What to hand in:** `git add benchmarks/search/results/load` and push, as with JMH. In
`summary.csv`, `http_errors`, `wrong_results` and `connection_errors` must be 0, or the run must be
reported as it is. `requests_per_replica` shows how nginx shared the load.

**Limitations:**
- The generator runs on the same machine as the system, so it competes with it for CPU.
- It is a closed loop: under saturation, clients wait instead of queuing more requests, so latencies are
  lower than with requests arriving at a fixed rate.
- With the sample dataset the index has 15 books. The large index depends on DR-16.
