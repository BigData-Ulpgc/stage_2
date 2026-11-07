# Synthetic dataset (DR-16)

The scale experiments need 10,000 and 100,000 books. Project Gutenberg has about 75,000, and
downloading that many is neither viable nor respectful of its service. So `generate_dataset.py`
writes N books with the format of `sample_dataset/raw`, and Ingestion reads them with its local
source (`INGESTION_SOURCE=local`), without network. The run is reproducible: with the same
seed, every member of the group gets the same bytes.

```bash
# From the repository root: 10,000 books into data/books, the INGESTION_LOCAL_HOST_DIR of .env
python3 scripts/dataset/generate_dataset.py --count 10000
python3 scripts/dataset/generate_dataset.py --verify data/books     # checks the files and the digest
python3 -m unittest discover -s scripts/dataset -v                   # the tests (about 20 s)
```

Then, with Docker:
1. Set `INGESTION_SOURCE=local` in `.env`.
2. Seed the ids of `data/books/book_ids.txt`, through `POST /control/jobs` or with the Control's `CONTROL_SEED_FILE`.

Only Python 3 is needed, with nothing beyond its standard library. `data/` is in `.gitignore`: the
dataset is not committed, because the seed regenerates it.

## What is generated

| File | Content |
|---|---|
| `pg<id>.txt` | One book, like a raw Gutenberg plain-text file, written atomically (a temporary file and a rename). |
| `book_ids.txt` | The ids, one per line, with a `#` comment first. It is the format of `CONTROL_SEED_FILE`. |
| `SHA256SUMS` | The SHA-256 of each book, in the format of `sample_dataset/SHA256SUMS`. |
| `manifest.json` | The parameters, the Python version, the size, and the **digest** of the whole dataset. |

**Each book has the structure of a real raw file:**
- CRLF line ends;
- the `Title:`, `Author:`, `Release date: … [eBook #<id>]` and `Language:` lines;
- the `*** START OF THE PROJECT GUTENBERG EBOOK … ***` and `*** END OF … ***` markers that `BookSplitter` looks for.

Its `Credits:` line says that it is synthetic. The body is made of sentences and paragraphs wrapped at 72 columns.

**Ids** start at **1,000,001**. Real Gutenberg ids stay below about 75,000, so synthetic and real books never collide, even in the same datalake and database. The ids are contiguous, so they fill the ranges of 1000 of the datalake evenly.

**The headers follow the mix of the 200 real headers** (DR-04 survey):

| Header | Share |
|---|---|
| `Original publication:` | 4.5 %, with a release in 2022–2025, like the real ones |
| No `Author:` | 1.5 %; of those, two thirds have `Creator:` |
| A language other than English | 10 %, so that the `language` filter of Search has something to filter (the 200 real books are all English) |
| Release years | 1992–2016, with the real median of 2004 |

## How the words are drawn

`vocabulary.txt` lists the 30,442 words of the 15 books of `sample_dataset`, most frequent first.
`build_vocabulary.py` builds it with the rules of `Tokenizer`, and keeps the stopwords on purpose:
the tokenizer drops them, as it does with a real book. Behind the real words come deterministic
pseudo-words (`baka`, `tobemi`…), up to 2,000,000 ranks.

A word is drawn with two regimes, as in large real corpora (Ferrer i Cancho and Solé, 2001):
- **the first 3,000 ranks keep their real frequency.** These are the words seen 31 times or more
  in the sample, so their frequency is well measured;
- **beyond, Zipf with slope 2**, joined to the frequency of rank 3,000.

**Calibration, against the 15 real books** (the tests check it):

| | Real books | Synthetic |
|---|---|---|
| Distinct words, with the same number of words as the 15 books (1.14 M) | 30,442 | 29,900 |
| Distinct words with 4 times as many words | (Heaps, exponent ≈ 0.5) | × 2.0 |
| Share of stopwords in the text | 42 % | 43 % |

**Two simpler models failed:**
- **One slope for every rank** (1.23, the one measured between ranks 10 and 10,000) put 69 % of stopwords in the text: "the" alone was 21 %, against 5.5 % in the real books.
- **The real frequencies with a tail of slope 1.23** grew the vocabulary far too fast: 200 books had 204,501 distinct terms instead of about 60,000.

**Lengths:** a lognormal with the spread measured on the real books (σ = 1.34 of the log), kept within median/20 … median×20.
- The real median is 71,000 words. The default is **10,000** (`--median-words`), so that 100,000 books fit on a laptop. The mean is about 2.5 times the median, about 120 KB a file.

**Reproducibility:**
- A book depends only on `(seed, id)`. It does not depend on N, on the order of generation, or on the number of processes.
- So the 10,000-book dataset is exactly the first 10,000 books of the 100,000 one, and the generation is parallel (one process per CPU).
- `generate_dataset.py` refuses to mix two datasets in one folder.
- A change in how books are made must change `GENERATOR` (v2…) and the reference digest of the tests, in the same commit.

## Sizes and digests (defaults: seed 42, first id 1,000,001)

Measured on the development machine (12 CPUs, Python 3.12):

| Books | Generation | `pg<id>.txt` | Digest of `manifest.json` |
|---|---|---|---|
| 10,000 | 46 s | 1.2 GB | `d2bd9f45e701ed144922945ea67ad3bbe2cbfdf33eb0f3925db8ba97d864572f` |
| 100,000 | 6 min 51 s | 12.2 GB | `46d3798c247f3fdd2061bc4ea1044c512a752df279e22aaf3dc325d36b68173b` |

**The rest of the pipeline adds about as much again:**
- the datalake keeps a header and a body for each book;
- with 1,000 books, MongoDB stored 15 MB, for 133,603 terms.

So plan about 2.5 times the size of `pg<id>.txt` in free disk.

**The real run with 1,000 books** went through the jars of Ingestion (local source) and Indexing (batches of DR-09):
- the 1,000 books became `INDEXED`, then `PUBLISHED`, with none `FAILED` and no error in the logs;
- 91.5 % are in English, 4.6 % have the year of `Original publication`, and 1.1 % have no author.

## Known limits

- **No topics.** Each word is drawn on its own, so a word of medium frequency is spread over almost every book: "whale" was in 960 of 1,000 books, and in a real corpus it is in a few.
  - The posting lists of the most common words are realistic ("said": 997 of 1,000).
  - The posting lists of medium-frequency words are much longer than real ones. Keep that in mind when you read the length of posting lists and the cost of a search.
- **The body is English whatever `Language:` says.** The language only matters for the metadata and the filter.
- **The license text at the end is a short stub**, not the 18 KB of the real files. `BookSplitter` drops everything after the END marker anyway.
- **The same bytes need the same generator and the same `random` of Python.** Python has kept `random` stable for a fixed seed across versions, and `manifest.json` records the version, so a different digest says where to look.
