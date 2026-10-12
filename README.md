# Stage 2 — Service-Oriented Search Engine

Big Data · Grado en Ciencia e Ingeniería de Datos · ULPGC · 2026-2027

Ingestion, Indexing and Search microservices (Java 17, Javalin) coordinated with Hazelcast
and deployed on one host with Docker Compose.

> Work in progress. The full README (build, run, configuration, tests and benchmarks)
> is task DP-13.

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
