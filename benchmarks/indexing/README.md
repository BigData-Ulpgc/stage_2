# Indexing benchmarks

## JMH: the CPU part of indexing on its own (DR-12)

They measure the code that indexing runs in the JVM, without MongoDB, Hazelcast or HTTP:
- tokenizing a body;
- parsing a header;
- adding the terms of a batch to the index;
- preparing a whole batch with 1, 2, 4 and 8 threads.

The experiments with MongoDB (books per second, batch size, scale) are DR-13. Together they answer
**where the time of indexing goes**. Compare the time per book here with the 73 ms per book measured
end to end in DR-09 (13.7 books/s).

**Requirements:** JDK 17 or newer, Maven 3.9, git, bash and Python 3 (for the dataset of DR-16).
Docker and MongoDB are not needed.

### How to run them

```bash
# 1. Check that it works (about 30 s; these numbers are not valid)
bash benchmarks/indexing/run-jmh.sh --smoke

# 2. The real run (about 7 minutes)
bash benchmarks/indexing/run-jmh.sh
```

Set `MVN=/path/to/mvn` if Maven is not in the `PATH`.

**The books** are 200 synthetic books of DR-16 (`generate_dataset.py --count 200`, seed 42):
- the first run generates them into `benchmarks/indexing/data/books` (24 MB, ignored by git);
- every run checks their digest, so everyone measures the same bytes.

**During the real run,** the numbers are only valid if the machine is doing nothing else:
- Plugged in, in performance mode if the system has one, and without suspending when idle.
- Everything else closed, including other builds, Docker containers, browsers and video calls.

If the run stops or something else used the machine, rename `results/jmh/` and start again. The script
refuses to write over existing results, because two runs must not be mixed.

### What to hand in

Commit `benchmarks/indexing/results/jmh/`. Also state the RAM, the storage and anything unusual that
happened during the run. `host.txt` records the rest: date, commit, operating system, CPU, memory, JVM
and the digest of the books.

### Reading the results

| File | What it measures | What changes | Score |
|---|---|---|---|
| `TokenizerBenchmark.csv` | `Tokenizer.uniqueTerms` of one body | The book: `short`, `median`, `long` (percentiles 10, 50 and 90 of the body length) | Time per body |
| `MetadataParserBenchmark.csv` | `MetadataParser.parse` of the 200 headers, with the mix of the real ones | Nothing | Time per header |
| `AddDocumentBenchmark.csv` | `addDocument` of the 100 books of a batch into an empty index | `memory` (`InMemoryInvertedIndex`) or `mongo` (the pending map of `MongoInvertedIndex`, without its flush) | Time per book |
| `PrepareBatchBenchmark.csv` | `IndexingService.indexBatch` of those 100 books: read, parse and tokenize on the pool, then write into in-memory stores | The threads of the pool (`INDEXING_THREADS`): 1, 2, 4, 8 | Time per book |
| `books.csv` | The books above: id, bytes of the body, tokens without stopwords, distinct terms | | |

**What the scores are:**
- Each one is a mean time in microseconds, with its 99.9 % error.
- Each configuration runs 5 warm-up and 10 measurement iterations of 1 s, in 2 separate JVMs with a 4 GB heap (enunciado §6.3). This is the same configuration as the Search benchmarks (DP-12, `benchmarks/search/`).
- Bytes per second of the tokenizer: `body_bytes` from `books.csv` divided by the score.

**Limitations:**
- The books are read from the page cache, so `PrepareBatchBenchmark` measures the CPU of preparing a book, not the disk.
- The write in it is to memory. In the service it goes to MongoDB, and in DR-09 that is what limits the rate.
- The bodies are synthetic text with real word frequencies (DR-16), so the tokenizer sees realistic words and lengths. Their punctuation is simpler than in real books: almost only full stops, no quotes, dashes or non-ASCII letters.

## Experiments: indexing with the real services (DR-13)

They measure what JMH leaves out, with the real services in `docker compose` and the limits of
`.env`:
- **How fast it indexes.** Books per second and seconds per book, with 1, 2, 4 and 8 threads
  (`INDEXING_THREADS`).
- **What a publication costs.** The time of `POST /index/publish` with 1, 10 and 50 new books.
- **How it scales.** 200, 10,000 and 30,000 books: the rate as the index grows, its size in
  MongoDB, and the length and read time of the longest posting lists.
- **Why it slows down.** Adding 100 ids to one posting list of 1,000, 10,000 and 100,000 ids, with
  `$addToSet` (what the index does) and with `$push`.
- **Where it saturates.** The CPU and memory of Indexing, MongoDB and Hazelcast during each run.

**Why 30,000 and not 100,000 books.**
- The rate falls as the index grows. In the trial runs it went from 18.5 books/s in the first
  1,000 books to 3.2 books/s in the last 1,000 of 10,000 books.
- MongoDB stayed at one CPU the whole time, and Indexing at 5 %. `$addToSet` compares every new id
  with the whole list so as not to repeat one, so updating a posting list costs more the longer it
  is: 5 ms with 1,000 ids and 226 ms with 100,000.
- At that pace, 100,000 books would take about 20 hours.
- The curve up to 30,000 books and the probe are enough to estimate it, and they explain it. Larger
  batches do not help: with 500 books per batch it was only 20 % faster, and with 1,000 Indexing ran
  out of memory.

**Requirements:**
- JDK 17 or newer, Maven 3.9, git, bash, Python 3, and Docker with Compose.
- About 10 GB of free disk for the 30,000 books: the dataset, the datalake and MongoDB.
- The ports of Hazelcast and MongoDB (`HZ1_HOST_PORT`, `HZ2_HOST_PORT`, `MONGO_HOST_PORT`) must be
  free.
- The containers of the group's stack are recreated with the same names, so stop it first
  (`docker compose down`). Its data stays: the script uses a database of its own.

### How to run them

```bash
# 1. Check that it works (about 5 minutes; these numbers are not valid)
bash benchmarks/indexing/run-experiments.sh --smoke

# 2. The real run (about 6 hours, on an idle machine: start it in the evening)
bash benchmarks/indexing/run-experiments.sh
```

`EXPERIMENTS="probe threads"` runs only some of them, and `SCALE_SIZES`, `REPETITIONS` and the
other variables at the top of the script change the levels. During the real run, the same rules as
for JMH apply: plugged in, in performance mode, and nothing else running.

**How each run is measured:**
1. The first time, Ingestion copies the books of the dataset of DR-16 into the datalake
   (`INGESTION_SOURCE=local`). Every later run reuses it.
2. Before each run:
   - the database is deleted and both Hazelcast members are restarted, so nothing is left from
     the previous run;
   - the run's books are seeded, and Ingestion takes them to `index-queue` while Indexing is
     stopped.
3. `IndexingExperiment` starts Indexing and counts the `INDEXED` books every second until all of
   them are indexed. Time 0 is just before the start, so the start-up of the service is counted.
4. `scripts/monitor.sh` records CPU and memory during the run. After a scale run,
   `IndexingExperiment stats` measures the collections in MongoDB.

The Control is not started, because it would publish and requeue books in the middle of a
measurement. Publications are triggered by the script itself.

### What to hand in

Commit `benchmarks/indexing/results/experiments/`. Also state anything unusual that happened during
the run. `host.txt` records the rest: date, commit, OS, CPU, memory, disk, Docker, dataset digest,
levels and the limits of `.env`.

### Reading the results

`summary.csv` uses the common format of plan §7.1, with `scope=integration` and `deployment=docker`:

| `experiment` | What changes | Fixed |
|---|---|---|
| `index_threads` | `threads`: 1, 2, 4, 8 | 2,000 books, batches of 100 |
| `index_scale` | `dataset_size`: 200, 10,000, 30,000 (30,000 once) | 4 threads, batches of 100 |
| `index_publish_<k>` | `k` new books per publication: 1, 10, 50 (and the first publication of the 10,000) | 4 threads, index of 10,000 books |
| `index_posting_update` | `dataset_size` here is the length of the list: 1,000, 10,000, 100,000 | 100 new ids per update, median of 20 |

| `metric` | Meaning |
|---|---|
| `throughput` | Books per second between 10 % and 90 % of the books: no start-up and no last batch |
| `throughput_total` | Books per second over the whole run, start-up included |
| `seconds_per_book` | 1 / `throughput` |
| `startup`, `duration` | Seconds until the first and the last book was indexed |
| `failed` | Books that ended `FAILED`; must be 0 |
| `throughput_pct_A_B` | Books per second between A % and B % of the books. If it falls along the run, the index slows down as it grows |
| `cpu_<service>` | Mean % of one CPU while indexing; 200 is all of a container with `cpus: 2` |
| `mem_max_<service>` | The most memory it used, MiB |
| `index_terms`, `index_postings`, `index_mean_postings` | Terms of the index, ids in all their posting lists, and ids per term |
| `index_disk`, `index_data` | Bytes on disk (data and indexes) and uncompressed bytes of the inverted index. Also `books_disk`, `index_log_disk` and `index_books_disk` |
| `posting_longest_*` | The longest posting list: ids, bytes (16 MB is the limit of a document), and median and maximum read time from the host, in ms |
| `publish_latency` | Time of one `POST /index/publish`, ms |
| `addtoset_latency`, `push_latency` | Time of one update of the probe, ms |

`runs/<run>/` has the details of each run:
- `timeline.csv`: the books indexed at each second;
- `stats.csv`: the 5 longest posting lists, and every collection;
- `monitor/`: the CPU and memory of each container.

**Limitations:**
- Everything runs on one machine: the driver, `docker stats` and the services share its CPUs.
- The rate is counted from Hazelcast every second, so a run of 200 books is too short for its
  steady rate to be precise. It shows the start-up.
- The books are synthetic (DR-16). Their vocabulary has the frequencies of real books, so the
  posting lists of the frequent terms grow as they would.

## Figures for the report (DR-14)

```bash
python3 benchmarks/plot.py            # needs matplotlib; writes benchmarks/indexing/results/figures/
```

It reads only what is committed (the results of DR-12 and DR-13), so it runs without Docker in a
few seconds. Same colors, ink and size as the figures of `benchmarks/search/analyze.py`.

| Figure | What it shows | Source |
|---|---|---|
| `index_threads.png` | Books/s against `INDEXING_THREADS`: the CPU part alone (JMH) goes from 233 to 487, the whole path into MongoDB stays at about 15 | `jmh/PrepareBatchBenchmark.csv`, `summary.csv` (`index_threads`) |
| `index_threads_cpu.png` | Mean CPU of Indexing (14–18 %) and MongoDB (about 100 %) in those runs | `summary.csv` (`cpu_indexing`, `cpu_mongo`) |
| `index_rate.png` | Books/s against the books already in the index, in blocks of 500: the 3 runs of 10,000 and the run of 30,000 lie on one curve | `runs/index_scale-*/timeline.csv` |
| `index_probe.png` | Adding 100 ids to one posting list: `$addToSet` against `$push` (log-log) | `probe.csv` |
| `index_publish.png` | Each `POST /index/publish` of 1, 10 and 50 new books on 10,000, and the median | `summary.csv` (`index_publish_*`) |
