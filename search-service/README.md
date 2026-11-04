# Search Service

Owner: Daniel Perdomo. Front-end-facing search API (contract: `../api/openapi.yaml`).

Search mounts no volume and never writes: it reads the published index version from MongoDB
(plan v3.1, contract §6.5) and keeps it in memory as an immutable snapshot.

## Build, test and run

From the repository root (the module depends on `common`, built in the same reactor):

```bash
docker compose up -d mongo                      # the MongoDB tests are skipped without it
mvn -B -pl search-service -am package           # tests + search-service/target/search-service.jar (fat jar)
java -jar search-service/target/search-service.jar
curl localhost:7003/status                      # {"service":"search-service","status":"running","index_version":1,"books_indexed":15}
```

| Variable | Default | Meaning |
|---|---|---|
| `SEARCH_PORT` | `7003` | HTTP port |
| `SEARCH_THREADS` | `250` (Javalin's default, made explicit) | maximum Jetty request threads |
| `MONGO_URI` | `mongodb://localhost:27017` | in Docker: `mongodb://mongo:27017` |
| `MONGO_DATABASE` | `search_engine` | database with the index and the metadata |

The service starts even if MongoDB is down or nothing is published yet: `/status` answers with
`index_version: null` and the API returns 503 until a version is loaded.

## How a search is answered (DP-04)

`GET /search?q=river&author=twain&offset=0&limit=20` (full contract in `../api/openapi.yaml`):

1. `SearchQuery.parse` validates every parameter (400 with the common error format otherwise) and
   tokenizes `q` with `Tokenizer.standard()`, the same tokenizer Indexing uses.
2. The request takes the current snapshot once (503 `INDEX_NOT_READY` if there is none).
3. `MongoPostingSource` reads the posting lists of all the terms with ONE query
   (`{term: {$in: [...]}}`) on the snapshot's collection.
4. `SearchEngine` walks the shortest list and keeps the ids every other list has (AND), drops
   the ids not published in the snapshot, applies `author` / `language` / `year` on the snapshot
   metadata in memory, and cuts the page. Results are sorted by `book_id`; `total` counts all
   matches.

If MongoDB does not answer, the response is 503 `STORAGE_UNAVAILABLE`.

`GET /books/{book_id}` and `GET /search/filters` (DP-05) do not touch MongoDB at all: they answer
from the snapshot metadata. A book that is indexed but not published is a 404. The languages and
year range are computed once, when the snapshot is built.

## What Search reads from MongoDB

```
index_meta    {_id: "CURRENT", version: V, collection: "inverted_index", ...}   which version is visible
index_books   {_id: <book_id>, version: <n>}                                  published books (n <= V are visible)
books         {_id: <book_id>, title, author, language, year, year_source, ...} metadata (common, DR-04)
<collection>  {term: "whale", postings: [...]}                                 posting lists (common, DR-02)
```

The snapshot holds the version, the collection and the metadata of the published books; the
posting lists stay in MongoDB and each request reads only those of its terms. A book that is
indexed but not published is never returned.

## Test data (DP-03)

There is no Indexing Service yet, so the tests write a published index themselves with the same
`common` code Indexing will use (`Tokenizer.standard()`, `MetadataParser`, `MongoInvertedIndex`,
`MongoMetadataRepository`): `src/test/java/.../fixture/MongoFixtureLoader`. Each test class uses a
database of its own (`search_test_*`) and drops it afterwards.

- **`TestBooks`**: 9 books with short bodies written for the tests. CURRENT is version 2; book 11
  is published with version 3 (a publication stopped before switching CURRENT) and book 2701 is
  indexed but not published, so neither may ever appear in a result.
- **`SampleDatasetLoader`**: loads the 15 real books of `stage_1/sample_dataset` into
  `search_engine` as version 1, to run the service by hand (needs a clone of `stage_1` next to
  `stage_2`):

```bash
mvn -q -pl search-service -am install -DskipTests   # once, so that search-service finds common
cd search-service
mvn -q test-compile exec:java -Dexec.args="../../stage_1/sample_dataset/book"
```

With the 15 books the index has 30396 terms. Reference results (AND semantics):
`whale` → [76, 84, 345, 2701]; `adventure` → [46, 74, 76, 84, 174, 345, 1342, 1661, 2701].
All 15 are English and none has an "Original publication" line, so `year` is the ebook year
(Pride and Prejudice → 1998).
