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
