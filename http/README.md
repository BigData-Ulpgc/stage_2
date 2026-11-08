# API validation

How to check the APIs without a front-end, and what each check must answer (assignment §3.4).

| What | Where | For |
|---|---|---|
| Script with every check | `scripts/validate-api.sh` | One command, and the evidence to keep |
| Automated tests | `search-service/.../validation/StackValidationTest.java` | The same contract in JUnit, also the shape of the JSON |
| Examples to run by hand | `http/*.http`, each with its expected result | IntelliJ HTTP Client or VS Code REST Client (environment `docker`) |

## 1. Start the stack and load the sample dataset

The expected results are those of the 15 books of `stage_1/sample_dataset`. The loader **empties the
index collections of the database** and publishes those books as version 1. `stage_1` must be next to
`stage_2`.

```bash
docker compose -f docker-compose.yml -f docker-compose.validation.yml up -d --build
mvn -q install -DskipTests -pl common -am
cd search-service
mvn -q test-compile exec:java -Dexec.args="../../stage_1/sample_dataset/book"
cd ..
```

`docker-compose.validation.yml` publishes the ports of Ingestion and Indexing, which only the
automated tests and the `.http` files need. Search loads the new version by itself within
`SEARCH_POLL_SECONDS`.

## 2. Run the checks

```bash
bash scripts/validate-api.sh | tee validation.txt

VALIDATE_URL=http://localhost:8080 VALIDATE_INDEXING_URL=http://localhost:7002 \
  mvn -B -pl search-service -am test -Dtest=StackValidationTest -Dsurefire.failIfNoSpecifiedTests=false
```

Without `VALIDATE_URL`, `StackValidationTest` is skipped: a normal `mvn verify` needs no stack.

Run it right after loading the dataset: if the pipeline has indexed other books, `POST /index/publish`
would create a new version. The script ends with `passed N, failed 0, skipped M` and exit code 0. **SKIP** is an endpoint of
the contract that answers 501: it starts running when it is implemented. Any other answer is a PASS
or a FAIL, so a route that disappears does not go unnoticed. With `docker-compose` instead of
`docker compose`, it is detected, or set with `COMPOSE="docker-compose"`.

`theRepliesOfEveryReplicaAreTheSame` runs only with several replicas (`--scale search=2` or more).

## 3. Expected results

| Case | Request | Expected |
|---|---|---|
| Keyword | `/search?q=frankenstein` | 200, book 84 |
| Several terms (AND) | `/search?q=whale+monster` | 200, books 84, 345, 2701 |
| Filter: author | `/search?q=river&author=twain` | 200, books 74, 76 |
| Filter: year | `/search?q=river&year=2004` | 200, books 46, 74, 76 |
| Filter: language | `/search?q=frankenstein&language=en` | 200, book 84 |
| Combined filters | `/search?q=river&author=dickens&year=1994&language=en` | 200, book 98 |
| Empty | `/search?q=xyzzyqqq` | 200, `total: 0`, `results: []` |
| Page 1 | `/search?q=river&limit=3` | 200, `total: 10`, books 46, 74, 76 |
| Page 2 | `/search?q=river&limit=3&offset=3` | 200, `total: 10`, books 84, 98, 174 |
| Beyond the end | `/search?q=river&offset=10` | 200, `total: 10`, `results: []` |
| Invalid: no `q` | `/search` | 400 `MISSING_PARAMETER` |
| Invalid: parameter | `q=the`, `language=xx`, `year=99`, `offset=-1`, `limit=0`, `limit=101`, `q=%ZZ` | 400 `INVALID_PARAMETER` |
| A book | `/books/1342` | 200, Pride and Prejudice |
| Not published | `/books/999999` | 404 `NOT_FOUND` |
| Invalid `book_id` | `/books/0`, `+84`, `0084`, `abc` | 400 `INVALID_PARAMETER` |
| Filter values | `/search/filters` | 200, 15 books, `en`, years 1975 to 2008 |
| Indexing: status | `GET /index/status` | 200, `books_indexed` and `books_published` 15 |
| Indexing: update | `POST /index/update/abc`, `/999999` | 400 `INVALID_PARAMETER`, 404 `NOT_FOUND` |
| Indexing: publish | `POST /index/publish` | 501 until DR-08; after it, 200, `books_published` 0 and still version 1 |
| Indexing: rebuild | `POST /index/rebuild` | 501 until DR-10 |
| Ingestion: errors | `POST /ingest/0084`, `GET /ingest/status/0084` | 400 `INVALID_PARAMETER` |
| Ingestion: status | `GET /ingest/status/999999` | 200, `not_available` |
| Ingestion: list | `GET /ingest/list` | 200, `count` and `books` |
| Control | `GET /status` | 200, `coordination` connected |
| Control | `GET /control/status` | 200, books per state and queues |
| Control | `GET /control/tasks/0084`, `/999999` | 400 `INVALID_PARAMETER`, 404 `NOT_FOUND`, field `book_id` |
| Control | `POST /control/jobs` with an invalid id, an empty list or a body that is not JSON | 400 `INVALID_PARAMETER`, and nothing is seeded |
| Control | `POST /control/retry-failed` | 200 with `requeued`. It does change data if there are `FAILED` books: they go back to their queue |

**Interactions that are not HTTP**, checked by the script: the 2 Hazelcast members form one cluster,
Search serves the version of `CURRENT`, and Indexing and the Control cannot write in the datalake.
The transitions of the coordination contract are checked by `CoordinationTest` in `common`, with
`mvn -B -pl common -am test -Dtest=CoordinationTest -Dsurefire.failIfNoSpecifiedTests=false`.

The words were chosen so that the answer can be checked against the original texts: a book matches
if its body contains the word as a token of letters and digits.
