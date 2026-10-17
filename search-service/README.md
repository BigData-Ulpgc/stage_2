# Search Service

Owner: Daniel Perdomo. Front-end-facing search API (contract: `../api/openapi.yaml`).

## Build and test

From the repository root (the module depends on `common`, built in the same reactor):

```bash
mvn -B -pl search-service -am package   # runs the tests and builds search-service/target/search-service.jar (fat jar)
DATA_DIR=<data root> java -jar search-service/target/search-service.jar
```

`DATA_DIR` is the root of the shared data volume; the service reads `DATA_DIR/datamarts`
(read-only).

## Test fixture (DP-03)

`src/test/resources/fixtures/sample-datamarts/` is a published index built from the 15 books
of `stage_1/sample_dataset`, in the layout the Indexing Service publishes (contracts §6.3 and §6.5):

```
metadata.db                                books(book_id, title, author, language, year,
                                                 year_source, release_date, body_path, header_path)
index/CURRENT                              "v000001"
index/versions/v000001/inverted_index.json {"term": [book ids ascending], ...}   (30396 terms)
index/versions/v000001/manifest.json       {"version": 1, "books": 15, ...}
```

It lets Search be developed and tested before Indexing exists. It was cross-checked against an
independent implementation of the SPEC tokenizer (identical index). Facts worth knowing for tests:

- All 15 books are English (`language = "en"`): a `language=es` search is a valid query with no matches.
- None of their headers has an "Original publication" line, so `year` is the Gutenberg release year
  (`year_source = "release_date"`), e.g. Pride and Prejudice → 1998.
- Reference results (AND semantics): `whale` → [76, 84, 345, 2701];
  `adventure` → [46, 74, 76, 84, 174, 345, 1342, 1661, 2701].

Regenerate it (needs a clone of `stage_1` next to `stage_2`):

```bash
mvn -q -pl common -am install -DskipTests   # once, so that search-service finds common on its own
cd search-service
mvn -q test-compile exec:java -Dexec.args="../../stage_1/sample_dataset/book src/test/resources/fixtures/stopwords.txt src/test/resources/fixtures/sample-datamarts"
```

The output is deterministic (fixed `published_at`), so regenerating gives identical files.

## Temporary code

`es.ulpgc.bigdata.datamart.index.Tokenizer` is an unchanged copy of the Stage 1 tokenizer. It will
be deleted when the `common` module exists (DR-02); the package and class name are already the
ones `common` will use.
