# Kill the Control Module: PASS

Run 2026-11-08 18:16 UTC, commit c06ff6a, 2000 synthetic books (data/chaos/books-2000), 1 Ingestion replica(s) with 4 worker(s), Indexing. LEASE_SECONDS=15, LEASE_CHECK_SECONDS=3, PUBLISH_INTERVAL_S=5.

## Expected

The Control only seeds, checks leases and triggers publications; the workers do not need it to
consume the queues. It is killed with SIGKILL while Indexing is indexing. Meanwhile Ingestion
and Indexing keep working (their counters grow), but nothing new is published (`index_books`
does not grow) and no lease is checked. When it starts again it finds the `initialized` marker
(the cluster kept its state), so it rebuilds nothing, and it publishes the rest. Seeding is
idempotent. In the end every book is `PUBLISHED`, once (contract §7, "the Control dies").

## What happened

t = 0 is the POST /control/jobs.

- `before` system up: 1 Ingestion replica(s) with 4 worker(s), Indexing; 2000 books
- `t =    7.1 s` POST /control/jobs: 2000 added, 0 already known
- `t =   14.1 s` the first publication: PENDING 852, INGESTING 3, DOWNLOADED 966, INDEXING 82, INDEXED 94, PUBLISHED 8
- `t =   17.4 s` docker kill of the Control: 100 books in index_books, 100 indexed by the writer
- `t =   38.3 s` 20 s later, without the Control: 100 books in index_books, 500 indexed by the writer
- `t =   45.2 s` the Control started again and healthy
- `t =  160.7 s` every book PUBLISHED: PUBLISHED 2000

## Evidence

| Check | Result | Detail |
|---|---|---|
| Indexing kept working without the Control | PASS | writer.indexed 100 -> 500 |
| nothing was published without the Control | PASS | index_books 100 -> 100 |
| the new Control rebuilt nothing (marker found) | PASS | recovery.runs = 0 |
| every book PUBLISHED | PASS | 2000 of 2000 (PUBLISHED 2000) |
| no book FAILED | PASS | FAILED = 0 |
| book-state has each book once | PASS | total = 2000, books = 2000 |
| every book complete in the datalake | PASS | 2000 of 2000 with header, body and _READY; 0 without _READY |
| index_log has each book once | PASS | 2000 documents, books = 2000 |
| index_books has each book once | PASS | 2000 documents, books = 2000 |

Files: `control-*.json` (GET /control/status), `status-*.json` (GET /status of the services at the end), `timeline.csv` (books by state every 0.5 s), `logs/` (every service, with timestamps).
