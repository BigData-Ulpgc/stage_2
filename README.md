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
| `common` | Code shared by the services: configuration and HTTP conventions, plus the Stage 1 model, datalake, tokenizer, monolithic index and SQLite metadata (see below) | library |
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
| `model` | `RawBook`, `BookLocation`, `BookMetadata` | Stage 1, unchanged |
| `datalake` | `Datalake`, `AbstractFileDatalake` (atomic write), `RangeBasedDatalake` | Stage 1, unchanged; only the `range` structure |
| `datamart.index` | `Tokenizer` (+ `standard()` with the packaged `stopwords.txt`), `InvertedIndex`, `InMemoryInvertedIndex`, `MongoInvertedIndex` | Stage 1, unchanged |
| `datamart.metadata` | `MetadataParser`, `MetadataRepository`, `SqliteMetadataRepository`, `SqliteSchema` | Stage 1, unchanged (year / ISO language come in DR-04) |

Storage chosen for Stage 2 (goal: hundreds of thousands of books):

| Store | Structure | Where |
|---|---|---|
| Datalake | `range`: folders of 1000 ids | `<DATA_DIR>/datalake/range/<START>-<END>/<id>.header.txt`, `<id>.body.txt` |
| Metadata | SQLite, table `books` | `<DATA_DIR>/datamarts/metadata.db` |
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

## Start the Hazelcast cluster (development)

Requirements: Docker Desktop running.

```bash
docker compose up -d          # starts hz-1 and hz-2 (cluster "search-engine")
docker compose ps             # both must show "(healthy)"
docker logs hz-1 | grep -A3 "Members {size"   # expected: Members {size:2, ...}
docker compose down           # stop
```

| Setting | Default (`.env`) |
|---|---|
| Hazelcast version (image and Maven client must match) | `HAZELCAST_VERSION=5.7.0` |
| Cluster name | `HZ_CLUSTER_NAME=search-engine` |
| Members | `hz-1:5701`, `hz-2:5701` inside Docker; published on the host as `localhost:5701` and `localhost:5702` |
| Discovery | TCP/IP with a fixed member list; multicast disabled |
| Heap per member | `HZ_HEAP=512m` |

Services running **inside** Docker connect to `hz-1:5701,hz-2:5701` (service names, never
`localhost`). A service started from the IDE on the host connects to `localhost:5701`; it
should use the client in single-member routing mode (`ClusterRoutingConfig` with
`RoutingMode.SINGLE_MEMBER`; the old `smartRouting=false` is deprecated in 5.7), because the members announce
their Docker-internal addresses.

## Service images

All Java services use one multi-stage template, `docker/service.Dockerfile`, built from the
repository root with the Maven module as an argument:

```bash
docker build -f docker/service.Dockerfile --build-arg MODULE=search-service -t stage2/search-service .
```
