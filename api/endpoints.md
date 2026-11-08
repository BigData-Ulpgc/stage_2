# Endpoint inventory

All the HTTP endpoints of the Stage 2 system, separated into front-end, internal and
operational, as the assignment asks (§3.3). Each provider documents its endpoints in its own
OpenAPI file: every service has a `GET /status`, and one OpenAPI document cannot hold the same
path twice.

| Provider | Specification | Owner |
|---|---|---|
| Search Service | [`openapi.yaml`](openapi.yaml) | Daniel Perdomo |
| Indexing Service | [`indexing-openapi.yaml`](indexing-openapi.yaml) | Daniel Rodríguez |
| Ingestion Service | [`ingestion-openapi.yaml`](ingestion-openapi.yaml) | Carlos Falcón |
| Control Module | [`control-openapi.yaml`](control-openapi.yaml) | Carlos Falcón |

**Completion** says what a successful answer means: *done* (the work is finished when the
response arrives) or *accepted* (the work continues in the background; the row says how to
follow it). Rows marked *501 until DR-xx* already have their route and final contract, but
answer 501 `NOT_IMPLEMENTED` until that task. Every service answers errors with the same `Error`
schema, also for a route that does not exist (404 `NOT_FOUND`).

Only nginx (port 8080) and the Control Module (port 7000) are published outside Docker; the
rest of the services are reached by name inside the Docker network.

## Front-end

Consumed by the search front-end, always through nginx.

| Provider | Method and route | Consumers | Purpose | Completion |
|---|---|---|---|---|
| Search | `GET /search` | front-end | Keyword search with author, language and year filters, paginated | done (read-only) |
| Search | `GET /books/{book_id}` | front-end | Metadata of one published book | done (read-only) |
| Search | `GET /search/filters` | front-end | Languages and year range present in the published index | done (read-only) |

## Internal

Consumed by another service or by an operator, never by the front-end.

| Provider | Method and route | Consumers | Purpose | Completion |
|---|---|---|---|---|
| Indexing | `POST /index/update/{book_id}` | operators, validation | Index one complete book (metadata + posting lists + `index_log`) | done: indexed, visible after the next publish |
| Indexing | `POST /index/publish` | Control Module | Publish the indexed books as a new index version (`CURRENT`) | done: `CURRENT` points to the returned version; visible `INDEXED` books become `PUBLISHED` and a new version is announced with `IndexPublished` (without Hazelcast it still publishes) |
| Indexing | `POST /index/rebuild` | operators | Rebuild the whole index from the datalake into a new collection | done (DR-10): accepted (202), follow `rebuild` in `GET /index/status` of any replica |
| Indexing | `GET /index/status` | Control Module, operators | Books indexed and published, index size, rebuild progress | done (read-only) |
| Ingestion | `POST /ingest/{book_id}` | operators, validation | Download a book, split it into header and body and store it in the datalake | done: stored (`downloaded`), or it already was (`already_available`) |
| Ingestion | `GET /ingest/status/{book_id}` | operators, validation | Whether a book is complete in the datalake | done (read-only) |
| Ingestion | `GET /ingest/list` | operators, validation | Every book complete in the datalake | done (read-only) |
| Control | `POST /control/jobs` | operators, validation, benchmarks | Seed books to ingest, index and publish (`{"book_ids": [...]}`, up to 100,000) | accepted (202): follow `GET /control/status` and `GET /control/tasks/{book_id}` |
| Control | `GET /control/status` | operators, validation, benchmarks | Books per state, queue lengths, last publication, expired-lease check | done (read-only) |
| Control | `GET /control/tasks/{book_id}` | operators, validation | State of one book (its `book-state` entry) | done (read-only) |
| Control | `POST /control/retry-failed` | operators | Put the FAILED books back in the queue of the stage that failed (T11) | accepted: the books are back in their queue |

## Operational

| Provider | Method and route | Consumers | Purpose | Completion |
|---|---|---|---|---|
| every service | `GET /status` | Docker healthcheck, Control Module | Liveness of the process (it does not depend on MongoDB or Hazelcast) | done (read-only) |

Search adds the loaded `index_version` to its `GET /status`, and Ingestion its book `source`,
its `coordination` (`connecting` until the cluster answers: `/status` never waits for it) and
what its `ingest-queue` workers did. The Control adds its `coordination` too; its `/control`
routes answer 503 `COORDINATION_UNAVAILABLE` while it is not `connected`.

## Interactions that are not HTTP

Most of the work between services goes through Hazelcast and MongoDB instead of HTTP calls.
Those contracts are documented separately:

| Contract | Between | Where |
|---|---|---|
| `book-state`, `ingest-queue`, `index-queue` (consumed by the Indexing writer that holds `index-writer`), `index-events` (`IndexPublished`), lock `index-writer` (`IMap.lock`, not CP), `coordination-meta` | Control, Ingestion, Indexing, Search | [`api/contracts/coordination.md`](contracts/coordination.md) (CF-02) |
| Datalake by ranges and `<id>._READY` | Ingestion → Indexing, Control | [`api/contracts/datalake-readiness.md`](contracts/datalake-readiness.md) (CF-07) |
| Collection `books` (metadata) | Indexing → Search | plan §6.3 |
| `inverted_index`, `index_books`, `index_meta` (`CURRENT`), `index_publications` | Indexing → Search, Control | plan §6.5 |
