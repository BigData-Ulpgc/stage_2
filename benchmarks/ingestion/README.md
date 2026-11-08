# Ingestion benchmarks

## JMH: the work of a worker on each book, on its own (CF-13)

They measure the code that an Ingestion worker runs for each book, without the network of Project
Gutenberg, Docker or MongoDB:
- splitting a raw book into header and body;
- writing a round of books into the range datalake, with 1, 2, 4 and 8 threads;
- the whole ingestion of a round from the local source, with 1, 2, 4 and 8 threads;
- coordinating a round through Hazelcast (claim and complete), with 1, 2, 4 and 8 threads.

The experiments with the services running (books per second with 1, 2 and 4 replicas, the 10,000
synthetic books) are the integration part below. Together they answer **where the time of an
ingestion goes**: the source, the datalake or the coordination.

**Requirements:** JDK 17 or newer, Maven 3.9, git, bash and Python 3 (for the dataset of DR-16).
Docker, MongoDB and the Hazelcast of the compose are not needed: `CoordinationBenchmark` starts its
own two members inside the JVM, on ports 5901-5903.

### How to run them

```bash
# 1. Check that it works (about 2 minutes; these numbers are not valid)
bash benchmarks/ingestion/run-jmh.sh --smoke

# 2. The real run (about 10 minutes)
bash benchmarks/ingestion/run-jmh.sh
```

Set `MVN=/path/to/mvn` if Maven is not in the `PATH`.

**The books** are 200 synthetic books of DR-16 (`generate_dataset.py --count 200`, seed 42):
- the first run generates them into `benchmarks/ingestion/data/books` (24 MB, ignored by git);
- every run checks their digest, so everyone measures the same bytes;
- they are the same books as the Indexing benchmarks (`benchmarks/indexing/`), same digest.

**The datalakes** that the benchmarks write go to `benchmarks/ingestion/data/work`, on the disk of
the repository and not in `/tmp`, which may be in memory. Each one is deleted after its invocation,
and the folder is removed at the end.

**During the real run,** the numbers are only valid if the machine is doing nothing else:
- Plugged in, in performance mode if the system has one, and without suspending when idle.
- Everything else closed, including other builds, Docker containers, browsers and video calls.

If the run stops or something else used the machine, rename `results/jmh/` and start again. The script
refuses to write over existing results, because two runs must not be mixed.

### What to hand in

Commit `benchmarks/ingestion/results/jmh/`. Also state the storage (SSD or HDD) and anything unusual
that happened during the run. `host.txt` records the rest: date, commit, operating system, CPU, memory,
the file system of the work folder, JVM and the digest of the books.

### Reading the results

| File | What it measures | What changes | Score |
|---|---|---|---|
| `SplitterBenchmark.csv` | `BookSplitter.split` of one raw book | The book: `short`, `median`, `long` (percentiles 10, 50 and 90 of the raw length) | Time per book |
| `DatalakeWriteBenchmark.csv` | `RangeBasedDatalake.save` of the 100 books of a round, already split, into an empty datalake: header, body and `_READY`, each through a temporary file and an atomic move | The threads that save (`INGESTION_WORKERS`): 1, 2, 4, 8 | Time per book |
| `IngestBenchmark.csv` | `IngestionService.ingest` of those 100 books: read the raw file from the local source, split it, check that it is not in the datalake, save it | The threads, as above | Time per book |
| `CoordinationBenchmark.csv` | `claim` (T2) and `complete` (T3) of 100 seeded books on a cluster of two members, the threads sharing one client as the workers of a replica do | The threads, as above | Time per book |
| `books.csv` | The books above: id, bytes of the raw file, of the header and of the body | | |

**What the scores are:**
- Each one is a mean time in microseconds, with its 99.9 % error.
- Each configuration runs 5 warm-up and 10 measurement iterations of 1 s, in 2 separate JVMs with a 4 GB
  heap (enunciado §6.3). This is the same configuration as the Indexing and Search benchmarks.
- Books per second: 1,000,000 divided by the score. For `IngestBenchmark` that is the rate of one replica
  with that many workers when the source costs nothing; the score of `CoordinationBenchmark` is what
  Hazelcast adds to every book on top.
- Bytes per second of the splitter or the write: `raw_bytes` (or `header_bytes + body_bytes`) from
  `books.csv` divided by the score. The round is the row `batch_of_100`, divided by 100 for one book.

**Limitations:**
- The raw files are read from the page cache, so `IngestBenchmark` does not include reading the disk,
  and nothing measures the download from Gutenberg, which in the service is the slowest part by far.
- Nothing is forced to disk (the datalake does not `fsync`, `api/contracts/datalake-readiness.md` §5): the
  writes measure up to the page cache of the operating system, not the disk itself.
- In `CoordinationBenchmark` the client and the members talk over loopback, without a real network, and
  the members use the CPU of the same JVM as the threads of the benchmark. In the compose each member
  is a container of its own.
- The ids of a round are consecutive, so the 100 books go to the same range folder, as in the service.

## Integration: the services running on Docker Compose (CF-13)

They measure the whole system on one host with the local source (no network), changing one factor at a
time (enunciado §6.2, plan §9):

| Experiment | What changes | Fixed | Books |
|---|---|---|---|
| `replicas` | `--scale ingestion=1, 2, 4` | 4 workers per replica | 10,000 |
| `workers` | `INGESTION_WORKERS=1, 2, 8` (4 is the point `replicas` 1) | 1 replica | 10,000 |
| `pipeline` | nothing: seed → `PUBLISHED`, with Indexing running | 1 replica of each, 4 workers | 200 and 10,000 |

Each point is repeated 3 times. It starts from an empty cluster (`docker compose down`), an empty
datalake (`data/bench-ingestion/datalake`) and an empty database of its own (`ingestion_benchmark`),
so the data of other work is never touched; the Control would otherwise rebuild book-state from it
(CF-10). The ids go in through `POST /control/jobs`, as in production.

**Requirements:** Docker with Compose (about 8 GB and 8 CPUs for Docker with 4 replicas), curl, git and
Python 3. Maven and the JDK are not needed: the images are built by Docker.

```bash
# 1. Check that it works (about 5 minutes, 200 books; these numbers are not valid)
bash benchmarks/ingestion/run-integration.sh --smoke

# 2. The real run (about 1 h 15 min), on an idle machine
bash benchmarks/ingestion/run-integration.sh
```

- `COMPOSE=docker-compose` for the old command; `EXPERIMENTS="replicas"` runs only some experiments;
  `REPETITIONS=1` fewer repetitions.
- The first real run generates the 10,000 books into `benchmarks/ingestion/data/books-10000`
  (1.2 GB, ignored by git, about 2 minutes) and every run checks their digest.
- The same rules as for JMH: nothing else running, plugged in, and the script refuses to write over
  existing results.

**What to hand in:** commit `benchmarks/ingestion/results/integration/` (without `compose.log` if it is
big) and state the storage (SSD or HDD) of the machine.

### Reading the results

`summary.csv` has the common format of plan §7.1 (`threads` is `INGESTION_WORKERS`, `replicas` the
Ingestion replicas, `client_concurrency` 1: only the Control seeds). Per point:

| Metric | What it is |
|---|---|
| `ingest_time`, `throughput` | From the first `POST /control/jobs` until every book is past ingestion (`DOWNLOADED` or later), polled every 0.2 s; books/s over that time |
| `queue_wait_*`, `claim_*`, `work_*`, `complete_*` | `coordination_cost` of the replicas (CF-12) in ms: the mean over every book; p95 and p99 of the worst replica (percentiles of different processes cannot be merged, so this is an upper bound) |
| `coordination_per_book` | mean `claim` + mean `complete`: what Hazelcast adds to each book |
| `coordination_share` | that cost as a % of claim + work + complete |
| `duplicate_downloads` | downloads beyond one per book; anything but 0 stops the run |
| `time_to_indexed`, `time_to_published`, `pipeline_throughput` | `pipeline` only: until every book is `INDEXED` or `PUBLISHED`, and until every one is `PUBLISHED`. The last includes the wait for the next publication (`PUBLISH_INTERVAL_S`), so the rate is taken over `time_to_indexed` |

Also:
- `timeline/` has the books ingested, indexed and published every 0.2 s of each point, to see whether
  the rate falls as the datalake grows.
- `status/` has `GET /status` of each replica at the end.
- `monitor/` has the CPU and memory of every container, the Hazelcast members included (`scripts/monitor.sh`).

A point is only a result if it is correct: a `FAILED` book, a duplicate download or a timeout stops
the run with an error instead of writing numbers.

**Limitations:**
- One machine: the replicas share its CPUs, its disk and the two Hazelcast members. More replicas
  show how this host is used, not how the system would scale on the Stage 3 cluster.
- The local source reads from the page cache after the first point. With Gutenberg the download
  dominates, and its own throttling (429/403) limits the rate.
- `work` includes the time the worker waits for the CPU of its container (1 CPU, `.env`).
