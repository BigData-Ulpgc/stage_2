# Endpoint inventory

All the HTTP endpoints of the Stage 2 system, separated into front-end, internal and
operational, as the assignment asks (§3.3). Each provider documents its endpoints in its own
OpenAPI file: every service has a `GET /status`, and one OpenAPI document cannot hold the same
path twice.

| Provider | Specification | Owner |
|---|---|---|
| Search Service | [`openapi.yaml`](openapi.yaml) | Daniel Perdomo |
| Indexing Service | [`indexing-openapi.yaml`](indexing-openapi.yaml) | Daniel Rodríguez |
| Ingestion Service | *pending (CF-06)* | Carlos Falcón |
| Control Module | *pending (CF-09)* | Carlos Falcón |

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
| Indexing | `POST /index/publish` | Control Module | Publish the indexed books as a new index version (`CURRENT`) | *501 until DR-08*; then done: `CURRENT` points to the new version |
| Indexing | `POST /index/rebuild` | operators | Rebuild the whole index from the datalake into a new collection | *501 until DR-10*; then accepted (202): follow `rebuild` in `GET /index/status` |
| Indexing | `GET /index/status` | Control Module, operators | Books indexed and published, index size, rebuild progress | done (read-only) |
| Ingestion | `POST /ingest/{book_id}` | *pending (CF-06)* | Download a book into the datalake | |
| Ingestion | `GET /ingest/status/{book_id}` | *pending (CF-06)* | Whether a book is in the datalake | |
| Ingestion | `GET /ingest/list` | *pending (CF-06)* | Books stored in the datalake | |
| Control | `POST /control/jobs` | *pending (CF-09)* | Seed books to ingest and index | |
| Control | `GET /control/status` | *pending (CF-09)* | Queue lengths and books per state | |
| Control | `GET /control/tasks/{book_id}` | *pending (CF-09)* | State of one book | |
| Control | `POST /control/retry-failed` | *pending (CF-09)* | Put the FAILED books back in their queue | |

## Operational

| Provider | Method and route | Consumers | Purpose | Completion |
|---|---|---|---|---|
| every service | `GET /status` | Docker healthcheck, Control Module | Liveness of the process (it does not depend on MongoDB or Hazelcast) | done (read-only) |

Search adds the loaded `index_version` to its `GET /status`.

## Interactions that are not HTTP

Most of the work between services goes through Hazelcast and MongoDB instead of HTTP calls.
Those contracts are documented separately:

| Contract | Between | Where |
|---|---|---|
| `book-state`, `ingest-queue`, `index-queue`, `index-events` (`IndexPublished`), lock `index-writer` | Control, Ingestion, Indexing, Search | `api/contracts/coordination.md` *(pending, CF-02)*; plan §6.1 |
| Datalake by ranges and `<id>._READY` | Ingestion → Indexing | plan §6.2 |
| Collection `books` (metadata) | Indexing → Search | plan §6.3 |
| `inverted_index`, `index_books`, `index_meta` (`CURRENT`), `index_publications` | Indexing → Search, Control | plan §6.5 |
