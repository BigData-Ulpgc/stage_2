# Stage 2 — Service-Oriented Search Engine

Big Data · Grado en Ciencia e Ingeniería de Datos · ULPGC · 2026-2027

Ingestion, Indexing and Search microservices (Java 17, Javalin) coordinated with Hazelcast
and deployed on one host with Docker Compose.

**Repository:** <https://github.com/BigData-Ulpgc/stage_2>

| I want to... | Go to |
|---|---|
| See how it works | [Architecture](#architecture) |
| Run it in five minutes | [Quick start](#quick-start) |
| Compile each service | [Build](#build-maven-multi-module) |
| Change ports, data paths, replicas or threads | [Run without Docker](#run-a-service-without-docker), [Run with Docker](#run-the-whole-system-with-docker) |
| Find the API specifications and check them | [APIs and validation](#apis-and-validation) |
| Repeat the benchmarks | [Benchmarks](#benchmarks) |

## Architecture

All the processes run on one host. nginx is the only public entry; the services coordinate through
Hazelcast and share their results in the datalake and MongoDB.

| Piece | Role | Talks to |
|---|---|---|
| **Control Module** | Seeds the books, requeues expired leases, decides when to publish, rebuilds the state if Hazelcast loses it | Hazelcast, Indexing (`POST /index/publish`), MongoDB and datalake (read-only) |
| **Ingestion** (1-4 replicas) | Downloads each book and stores it in the datalake, with `<id>._READY` last | Hazelcast, datalake |
| **Indexing** (1-2 replicas) | Indexes the complete books and publishes versions. Only the replica holding the lock `index-writer` writes; the others wait to take over | Hazelcast, datalake (read-only), MongoDB |
| **Search** (1-4 replicas) | Answers the front-end from an immutable snapshot of the published version | MongoDB (read-only), Hazelcast (`IndexPublished`) |
| **nginx** | Public entry on port 8080, round-robin over the Search replicas | Search |
| **Hazelcast** (2 members) | Book states, queues, leases, events and the lock of the index writer | The services |
| **MongoDB** | Inverted index, metadata and published versions | The services |

The route of a book: `PENDING` → `INGESTING` → `DOWNLOADED` → `INDEXING` → `INDEXED` → `PUBLISHED`,
or `FAILED`. Each state is only written once its evidence is persistent (`_READY`, `index_log`,
`CURRENT`), so the Control can rebuild the coordination state if Hazelcast loses it. Search only shows
what a published version contains.

## Quick start

Requirements: Docker with Compose and about 6 GB of memory for Docker. Nothing else is needed.

```bash
docker compose up -d --build                # first time: some minutes
docker compose ps                           # wait until every service is "(healthy)"

# Ask for three books: they are downloaded from Project Gutenberg, indexed and published
curl -X POST localhost:7000/control/jobs -H "Content-Type: application/json" -d "{\"book_ids\":[1342,84,11]}"
curl localhost:7000/control/status          # books per state, queues and last publication

# Within about 30 s (PUBLISH_INTERVAL_S) they can be searched, through nginx
curl "localhost:8080/search?q=elizabeth&limit=5"
curl "localhost:8080/search?q=monster&language=en&offset=0&limit=2"
curl "localhost:8080/search/filters"
curl "localhost:8080/books/84"

docker compose up -d --scale search=3       # three replicas: see the header X-Served-By
curl -si "localhost:8080/search?q=monster" | grep -i x-served-by
docker compose down                         # stop; down -v also wipes MongoDB
```

To seed the 200 books of the common dataset when the Control starts:
`CONTROL_SEED_FILE=/shared/book_ids.txt docker compose up -d`.

## Build (Maven multi-module)

Requirements: JDK 17 or newer, Maven 3.8+. From the repository root:

```bash
mvn -B verify                              # builds every module and runs all the tests
mvn -B -pl indexing-service -am package    # one service and the modules it depends on (common)
```

| Module | What it is | Jar |
|---|---|---|
| `common` | Code shared by the services: configuration and HTTP conventions, plus the Stage 1 model, datalake, tokenizer, and the inverted index and metadata in MongoDB (see below) | library |
| `ingestion-service` | Ingestion Service | `ingestion-service/target/ingestion-service.jar` |
| `indexing-service` | Indexing Service | `indexing-service/target/indexing-service.jar` |
| `search-service` | Search Service | `search-service/target/search-service.jar` |
| `control-module` | Control Module | `control-module/target/control-module.jar` |

All versions (Java 17, Javalin 6.1.3, Gson, Hazelcast, JUnit, JMH) are fixed once in the parent
`pom.xml`. The Hazelcast version there must match `HAZELCAST_VERSION` in `.env`.

### What `common` contains

| Package | Classes | Origin |
|---|---|---|
| `config` | `Env` (environment variables), `DataLayout` (paths below `DATA_DIR`), `MongoSettings` (`MONGO_*`) | Stage 2 (replace Stage 1 `AppConfig`) |
| `http` | `ServiceServer` (Javalin + Gson, thread pool, `GET /status`) | Stage 2 |
| `model` | `RawBook`, `BookLocation`, `BookMetadata`, `YearSource` | Stage 1; `year` and `yearSource` added in DR-04 |
| `datalake` | `Datalake`, `AbstractFileDatalake` (atomic write), `RangeBasedDatalake` | Stage 1, unchanged; only the `range` structure |
| `datamart.index` | `Tokenizer` (+ `standard()` with the packaged `stopwords.txt`), `InvertedIndex`, `InMemoryInvertedIndex`, `MongoInvertedIndex` | Stage 1, unchanged |
| `datamart.metadata` | `MetadataParser`, `LanguageCodes`, `MetadataRepository`, `MongoMetadataRepository` | Stage 1 parser and interface; ISO language, year rule, `Creator` fallback and MongoDB in DR-04 |
| `coordination` | `Coordination` (every transition of `api/contracts/coordination.md`), `CoordinationSettings` (`HZ_*`, leases), `BookTask`, `IndexPublished`, `IndexWriterLock` | Stage 2 (CF-04) |

Storage chosen for Stage 2 (goal: hundreds of thousands of books):

| Store | Structure | Where |
|---|---|---|
| Datalake | `range`: folders of 1000 ids; a book exists when its `<id>._READY` marker is there, written last (contract §6.2). Datalakes written before DR-06 have no markers: their books are ingested again | `<DATA_DIR>/datalake/range/<START>-<END>/<id>.header.txt`, `<id>.body.txt`, `<id>._READY` |
| Metadata | MongoDB, one document per book with the book id as `_id` ([`index-publication.md`](api/contracts/index-publication.md) §3) | `MONGO_URI` / `MONGO_DATABASE` (`search_engine`) / collection `books` |
| Inverted index | MongoDB, one document `{"term", "postings"}` per term; a book is visible to Search once it is published ([`index-publication.md`](api/contracts/index-publication.md)) | `MONGO_URI` / `MONGO_DATABASE` (`search_engine`) / `MONGO_COLLECTION` (`inverted_index`) |

Tests: the MongoDB ones run against `MONGO_URI` (default `mongodb://localhost:27017`, database
`search_engine_test`) and are skipped, not failed, when no MongoDB answers:

```bash
docker run -d --rm --name mongo-test -p 127.0.0.1:27017:27017 mongo:8.2.12
mvn -B verify
docker stop mongo-test
```

## Run a service without Docker

Each jar is self-contained (fat jar) and is configured with environment variables:

```bash
java -jar control-module/target/control-module.jar          # :7000
java -jar ingestion-service/target/ingestion-service.jar    # :7001
INDEXING_PORT=7012 java -jar indexing-service/target/indexing-service.jar
curl localhost:7012/status    # {"service":"indexing-service","status":"running"}
```

| Variable | Default | Meaning |
|---|---|---|
| `CONTROL_PORT`, `INGESTION_PORT`, `INDEXING_PORT` | 7000, 7001, 7002 | HTTP port of each process |
| `<SERVICE>_HTTP_THREADS` (`CONTROL_`, `INGESTION_`, `INDEXING_`, `SEARCH_`) | 250 | Maximum Jetty request threads of the process (minimum 8) |
| `INGESTION_SOURCE` | `gutenberg` | Where Ingestion gets the books: `gutenberg` (download) or `local` (files `pg<id>.txt` of a folder: the sample and synthetic datasets, offline benchmarks) |
| `INGESTION_GUTENBERG_URL` | `https://www.gutenberg.org/cache/epub` | Base of the download URL `<base>/<id>/pg<id>.txt` |
| `INGESTION_CONNECT_TIMEOUT_SECONDS`, `INGESTION_REQUEST_TIMEOUT_SECONDS` | 10, 30 | Timeouts of one download |
| `INGESTION_LOCAL_DIR` | `sample_dataset/raw` | Folder of the `local` source; it must exist, or Ingestion does not start |
| `INGESTION_WORKERS` | 4 | `ingest-queue` worker threads of one Ingestion replica; `0`: HTTP API only, no Hazelcast |
| `INGESTION_FETCH_RETRIES` | 2 | Retries of a transient source failure inside one attempt (after 1 s, 2 s…); a 429 or 403 is not retried, it pauses the replica |
| `INDEXING_THREADS` | 4 | Threads of Indexing that read, parse and tokenize the books of one batch; the writing to MongoDB stays in one writer thread |
| `INDEX_WRITE_BATCH` | 100 | The most `index-queue` books Indexing claims and writes together (one bulk write per collection) |
| `HZ_MEMBERS`, `HZ_CLUSTER_NAME` | `localhost:5701,localhost:5702`, `search-engine` | Hazelcast cluster of the workers (inside Docker: `hz-1:5701,hz-2:5701`) |
| `LEASE_SECONDS`, `MAX_ATTEMPTS`, `LEASE_CHECK_SECONDS` | 120, 3, 10 | Leases and attempts of the coordination contract (`api/contracts/coordination.md` §8) |
| `PUBLISH_BATCH`, `PUBLISH_INTERVAL_S` | 100, 30 | The Control publishes (`POST /index/publish`) when this many books are `INDEXED`, or this long after the last publication if any is |
| `INDEXING_URL` | `http://localhost:7002` | Where the Control calls Indexing (inside Docker: `http://indexing:7002`) |
| `CONTROL_SEED_FILE` | (none) | A `book_ids.txt` the Control seeds once connected: one id per line, `#` comments and blank lines skipped; a bad line stops the start |
| `MONGO_URI`, `MONGO_DATABASE`, `DATA_DIR` (Control) | as above | Where the Control finds the evidence to rebuild `book-state` when the cluster loses it (`index_books`, `index_log`, the `_READY` markers; the datalake is only read) |

The request thread pool is always set explicitly (`common` → `ServiceServer`): a Jetty
`QueuedThreadPool` with max `<SERVICE>_HTTP_THREADS`, min 8, idle timeout 60 s. These are the
values Javalin 6.1.3 would use by default, but here they are a documented setting. An invalid
value (`INDEXING_PORT=abc`, `INDEXING_HTTP_THREADS=2`) stops the process at startup with a
message naming the variable.

## Start Hazelcast and MongoDB (development)

Requirements: Docker Desktop running.

```bash
docker compose up -d          # starts hz-1, hz-2 (cluster "search-engine") and mongo
docker compose ps             # the three must show "(healthy)"
docker logs hz-1 | grep -A3 "Members {size"   # expected: Members {size:2, ...}
docker compose down           # stop (MongoDB data is kept in the volume mongo-data)
docker compose down -v        # stop and wipe MongoDB data (clean benchmark runs)
```

| Setting | Default (`.env`) |
|---|---|
| Hazelcast version (image and Maven client must match) | `HAZELCAST_VERSION=5.7.0` |
| Cluster name | `HZ_CLUSTER_NAME=search-engine` |
| Members | `hz-1:5701`, `hz-2:5701` inside Docker; published on the host as `localhost:5701` and `localhost:5702`, on 127.0.0.1 only |
| Discovery | TCP/IP with a fixed member list; multicast disabled |
| Cluster configuration | `hazelcast/hazelcast.yaml`, mounted in both members: backups, the index of `book-state`, queue and topic settings (CF-03). Restart the members after changing it: `docker compose up -d --force-recreate hz-1 hz-2` |
| Heap per member | `HZ_HEAP=512m` |
| MongoDB image | `MONGO_VERSION=8.2.12` (same as Stage 1; driver version in the parent `pom.xml`) |
| MongoDB memory | `MONGO_MEM_LIMIT=1g` container limit, `MONGO_CACHE_GB=0.25` WiredTiger cache |
| MongoDB address | `mongo:27017` inside Docker (`MONGO_URI=mongodb://mongo:27017`); `127.0.0.1:27017` on the host only (`MONGO_HOST_PORT`) |
| MongoDB data | named volume `mongo-data` |

Services running **inside** Docker connect to `hz-1:5701,hz-2:5701` and `mongodb://mongo:27017`
(service names, never `localhost`). A service started from the IDE on the host connects to
`localhost:5701` and `mongodb://localhost:27017` (the `MongoSettings` default). For Hazelcast it
should use the client in single-member routing mode (`ClusterRoutingConfig` with
`RoutingMode.SINGLE_MEMBER`; the old `smartRouting=false` is deprecated in 5.7), because the members
announce their Docker-internal addresses.

The MongoDB cache is fixed on purpose: by default mongod sizes it from the container limit
(50% of `MONGO_MEM_LIMIT` minus 1 GB, at least 256 MB), so raising the limit would also change the
cache and the benchmark results would not be comparable. The `mongo` service
also serves the MongoDB tests (`mvn -B verify` with it running), so the separate `mongo-test`
container of the Build section is not needed; do not run both, they use the same host port.

## Run the whole system with Docker

```bash
docker compose up -d --build                                # everything, one replica of each
docker compose ps                                           # all must show "(healthy)"
curl "localhost:8080/search?q=whale"                        # the public API, through nginx
docker compose up -d --scale search=3 --scale ingestion=2   # more replicas
docker compose --profile monitoring up -d                   # also the Management Center (:8081)
docker compose down                                         # stop; down -v also wipes MongoDB
```

| Service | Replicas | Published on the host | Datalake | Limits (`.env`) |
|---|---|---|---|---|
| `nginx` | 1 | 8080: the public API | none | `NGINX_CPUS`, `NGINX_MEM_LIMIT` |
| `search` | 1-4 | none, only behind nginx | none | `SEARCH_CPUS`, `SEARCH_MEM_LIMIT` |
| `indexing` | 1-2 (one writes, the others wait) | none | read-only | `INDEXING_CPUS`, `INDEXING_MEM_LIMIT` |
| `ingestion` | 1-4 | none | read-write | `INGESTION_CPUS`, `INGESTION_MEM_LIMIT` |
| `control` | 1 | 7000, only on 127.0.0.1 | read-only | `CONTROL_CPUS`, `CONTROL_MEM_LIMIT` |
| `hz-1`, `hz-2` | 2 | 5701, 5702, only on 127.0.0.1 | none | `HZ_CPUS`, `HZ_MEM_LIMIT` |
| `mongo` | 1 | 27017, only on 127.0.0.1 | none (volume `mongo-data`) | `MONGO_CPUS`, `MONGO_MEM_LIMIT` |
| `management-center` | 0-1 (profile `monitoring`) | 8081, only on 127.0.0.1 | none | `MC_CPUS`, `MC_MEM_LIMIT` |

Every default is in `.env`; override one with a variable (`SEARCH_HTTP_THREADS=64 docker compose up -d`)
or by editing the file. One replica of each adds up to about 5.7 GB of memory limits, so Docker needs
at least that much. A new variable of a service goes in its `environment` block and in `.env`.

On Linux, the services run as root in their containers, so `./data` belongs to root: delete it with
`sudo rm -rf data`.

nginx (`scripts/nginx.conf`) does round-robin over the replicas of `search`, found with the DNS of
Docker, so scaling needs no other change. Each response has an `X-Served-By` header with the replica
that answered. If a replica dies, nginx sends the request to another one; a 503 is passed on to the client, not retried.

**Without network: the sample and the synthetic dataset.** With `INGESTION_SOURCE=local`, Ingestion reads the `pg<id>.txt` files of `INGESTION_LOCAL_HOST_DIR`
(`./data/books`) instead of downloading them. Two datasets have that format:

- **The sample dataset:** its `raw/` folder, the 15 real books, as `stage_1/sample_dataset/raw`.
- **The synthetic dataset of DR-16:** for the scale experiments, 10,000 or 100,000 books with the format of Gutenberg and the word distribution of the real books. It is reproducible with its seed; see `scripts/dataset/README.md`.

```bash
python3 scripts/dataset/generate_dataset.py --count 10000   # into data/books, about 1.2 GB, 1 min
INGESTION_SOURCE=local docker compose up -d
```

Then seed the ids of `data/books/book_ids.txt`. The synthetic ids start at 1,000,001, so they never
collide with a real book.

To record the CPU and memory of each container during an experiment, see `scripts/monitor.sh`.

To check every API on the running stack, with the expected results, see `http/README.md`.

## Service images

All Java services use one multi-stage template, `docker/service.Dockerfile`, built from the
repository root with the Maven module as an argument (Compose does it for you):

```bash
docker build -f docker/service.Dockerfile --build-arg MODULE=search-service -t stage2/search-service .
```

## APIs and validation

| What | Where |
|---|---|
| Inventory: front-end, internal and operational | [`api/endpoints.md`](api/endpoints.md) |
| Search (front-end API) | [`api/openapi.yaml`](api/openapi.yaml) |
| Indexing, Ingestion, Control | [`api/indexing-openapi.yaml`](api/indexing-openapi.yaml), [`api/ingestion-openapi.yaml`](api/ingestion-openapi.yaml), [`api/control-openapi.yaml`](api/control-openapi.yaml) |
| Contracts that are not HTTP | [`api/contracts/`](api/contracts) |
| Examples to run by hand, with their expected results | [`http/`](http) |

One command checks every API on the running stack, with the expected results. It needs JDK 17, Maven and
`stage_1` next to `stage_2`, because it uses the 15 books of its sample dataset; the steps are in
[`http/README.md`](http/README.md):

```bash
docker compose -f docker-compose.yml -f docker-compose.validation.yml up -d --build
mvn -q install -DskipTests -pl common -am
(cd search-service && mvn -q test-compile exec:java -Dexec.args="../../stage_1/sample_dataset/book")
bash scripts/validate-api.sh                # PASS, FAIL or SKIP per check; exit code 1 if one failed
```

## Tests

```bash
docker compose up -d mongo                  # without it, the MongoDB tests are skipped
mvn -B clean verify                         # every module
```

## Benchmarks

| What | Where | Run it |
|---|---|---|
| JMH of Search, on the host: intersection, filters, pagination, threads | [`benchmarks/search/`](benchmarks/search/README.md) | `bash benchmarks/search/run-jmh.sh` |
| Load through nginx: clients, replicas, Jetty threads, MongoDB connections, nginx | [`benchmarks/search/`](benchmarks/search/README.md) | `bash benchmarks/search/run-load.sh` |
| Tables, figures and the common CSV | [`benchmarks/search/`](benchmarks/search/README.md) | `python benchmarks/search/analyze.py` |
| CPU and memory of each container | [`scripts/monitor.sh`](scripts/monitor.sh) | `bash scripts/monitor.sh -l name -d 120` |

All the valid measures must come from one idle machine, with the limits of `.env` recorded in `host.txt`.

## Repository layout

| Path | Content |
|---|---|
| `common/` | Code shared by the services: configuration, HTTP conventions, datalake, MongoDB, coordination |
| `ingestion-service/`, `indexing-service/`, `search-service/`, `control-module/` | The microservices |
| `hazelcast/hazelcast.yaml` | Configuration of the Hazelcast members |
| `docker-compose.yml`, `.env`, `docker/`, `scripts/nginx.conf` | Deployment on one host |
| `api/` | OpenAPI of each service, inventory and contracts |
| `http/`, `scripts/validate-api.sh` | Validation of the APIs |
| `benchmarks/`, `scripts/monitor.sh` | Benchmarks, results and monitoring |
| `shared/` | The common lists of books and queries |

## Contributing: branches, pull requests and protection rules

### Branches

| Branch | Role |
|---|---|
| `main` | Stable. Only receives a PR from `develop` when a milestone closes (v0.1 … v1.0), then it is tagged. Always builds and starts. |
| `develop` | Integration. Every work branch starts here and comes back through a PR. |
| `feature/<area>/<ID>-<desc>` | One task of the plan, e.g. `feature/common/DR-02-migrate-stage1`, `feature/search/DP-04-filters`. |
| `infra/…`, `contract/…`, `test/…`, `bench/…`, `docs/…`, `fix/…` | Docker/Hazelcast/MongoDB config, changes in `api/`, validation, experiments, report, fixes. |

### Rules enforced by GitHub on `develop` and `main`

Configured in *Settings → Branches*. They apply to everyone, administrators included.

| Rule | What it means for you |
|---|---|
| Require a pull request before merging | `git push` directly to `develop` or `main` is rejected: every change goes through a PR. |
| Required approvals: 1 | The PR cannot be merged until **another** member clicks *Approve* (the author's own approval does not count). |
| Dismiss stale approvals when new commits are pushed | If you push a new commit after the approval, the PR has to be approved again: what is merged is exactly what was reviewed. |
| Do not allow bypassing the above settings | Nobody, not even an admin, can merge skipping the rules. |
| No force pushes, no deletions | Nobody can rewrite the history of `develop`/`main` or delete them. |
| Require status checks to pass *(when CI exists)* | Only PRs with `mvn -B verify` green can be merged. |

### Team agreements (not enforced by GitHub)

- A PR that touches `common/`, the parent `pom.xml`, `api/`, `hazelcast/`, the MongoDB collections or
  `docker-compose.yml` needs the approval of **both** other members. On this plan GitHub lets us request
  only one reviewer: request one and mention the other (`@user`) in the PR; any member can approve.
- PRs to `main` only come from `develop`.
- Merge with **Create a merge commit** (never *Squash*), so the history shows each person's commits.
- Do not delete `develop` when GitHub offers *Delete branch* after a merge (feature branches, yes).
- Conventional Commits with scope: `feat(search): …`, `build(docker): …`, `test(chaos): …`.

### Daily workflow

```bash
git checkout develop && git pull                       # start from the latest develop
git checkout -b feature/indexing/DR-05-api             # one branch per task
# ... commits ...
git pull origin develop                                # keep it up to date (every morning; a merge, no force push needed)
git push -u origin feature/indexing/DR-05-api          # then open the PR against develop
```

Review: in the PR, *Files changed* → comment on lines if needed → *Review changes* →
*Approve* / *Request changes*. Changes are made with new commits on the same branch.
