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
| `common` | Code shared by the services (configuration, HTTP server conventions; Stage 1 code in DR-02) | library |
| `ingestion-service` | Ingestion Service | `ingestion-service/target/ingestion-service.jar` |
| `indexing-service` | Indexing Service | `indexing-service/target/indexing-service.jar` |
| `search-service` | Search Service | `search-service/target/search-service.jar` |
| `control-module` | Control Module | `control-module/target/control-module.jar` |

All versions (Java 17, Javalin 6.1.3, Gson, Hazelcast, JUnit, JMH) are fixed once in the parent
`pom.xml`. The Hazelcast version there must match `HAZELCAST_VERSION` in `.env`.

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
