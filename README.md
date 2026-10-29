# Stage 2 — Service-Oriented Search Engine

Big Data · Grado en Ciencia e Ingeniería de Datos · ULPGC · 2026-2027

Ingestion, Indexing and Search microservices (Java 17, Javalin) coordinated with Hazelcast
and deployed on one host with Docker Compose.

> Work in progress. The full README (build, run, configuration, tests and benchmarks)
> is task DP-13.

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

Storage chosen for Stage 2 (goal: hundreds of thousands of books):

| Store | Structure | Where |
|---|---|---|
| Datalake | `range`: folders of 1000 ids | `<DATA_DIR>/datalake/range/<START>-<END>/<id>.header.txt`, `<id>.body.txt` |
| Metadata | MongoDB, one document per book with the book id as `_id` (contract §6.3) | `MONGO_URI` / `MONGO_DATABASE` (`search_engine`) / collection `books` |
| Inverted index | MongoDB, one document `{"term", "postings"}` per term | `MONGO_URI` / `MONGO_DATABASE` (`search_engine`) / `MONGO_COLLECTION` (`inverted_index`) |

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
| `<SERVICE>_HTTP_THREADS` (`CONTROL_`, `INGESTION_`, `INDEXING_`) | 250 | Maximum Jetty request threads of the process (minimum 8) |

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
| Members | `hz-1:5701`, `hz-2:5701` inside Docker; published on the host as `localhost:5701` and `localhost:5702` |
| Discovery | TCP/IP with a fixed member list; multicast disabled |
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

The MongoDB cache is fixed on purpose: by default WiredTiger takes half of the RAM it sees,
ignoring the container limit, and benchmark results would depend on the host. The `mongo` service
also serves the MongoDB tests (`mvn -B verify` with it running), so the separate `mongo-test`
container of the Build section is not needed; do not run both, they use the same host port.

## Service images

All Java services use one multi-stage template, `docker/service.Dockerfile`, built from the
repository root with the Maven module as an argument:

```bash
docker build -f docker/service.Dockerfile --build-arg MODULE=search-service -t stage2/search-service .
```

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
