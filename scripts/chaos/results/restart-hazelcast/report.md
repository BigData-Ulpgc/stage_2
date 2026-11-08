# Restart the whole Hazelcast cluster: PASS

Run 2026-11-08 18:20 UTC, commit c06ff6a, 2000 synthetic books (data/chaos/books-2000), 1 Ingestion replica(s) with 1 worker(s), Indexing. LEASE_SECONDS=15, LEASE_CHECK_SECONDS=3, PUBLISH_INTERVAL_S=5.

## Expected

A first wave of books is seeded and gets published; then the rest is seeded and, while it is
being ingested, both members are killed at once and started again: `book-state`, the queues, the lock and the
`initialized` marker are lost (Hazelcast keeps nothing on disk). Every client reconnects by itself.
The Control finds no marker and rebuilds `book-state` from the persistent evidence (CF-10, contract
§7): `index_books` up to CURRENT → `PUBLISHED`, `index_log` → `INDEXED`, `<id>._READY` →
`DOWNLOADED` (offered again to `index-queue`). The books with no evidence yet (`PENDING`, or
`INGESTING` without `_READY`) are lost from the cluster, as the contract says, and are sent again
with `POST /control/jobs`, which leaves the recovered ones as they are. In the end every book is
`PUBLISHED`, once; a book with `_READY` is never downloaded again, and the index writer takes
`index-writer` again.

## What happened

t = 0 is the POST /control/jobs.

- `before` system up: 1 Ingestion replica(s) with 1 worker(s), Indexing; 2000 books
- `t =    0.9 s` POST /control/jobs, first wave of 400 books: 400 added, 0 already known
- `t =    5.1 s` the first publication: PENDING 197, DOWNLOADED 100, INDEXING 100, PUBLISHED 3
- `t =    8.8 s` POST /control/jobs, second wave of 1600 books: 1600 added, 0 already known
- `t =    8.9 s` the second wave being ingested: PENDING 1586, DOWNLOADED 311, INDEXING 100, PUBLISHED 3
- `t =    9.9 s` docker kill hz-1 hz-2, the whole cluster (PENDING 1571, DOWNLOADED 326, INDEXING 100, PUBLISHED 3 just before)
- `t =   30.4 s` hz-1 and hz-2 started again and healthy, empty
- `t =   30.4 s` the Control rebuilt book-state in 1903 ms: 3 PUBLISHED, 100 INDEXED, 336 DOWNLOADED
- `t =   34.1 s` POST /control/jobs again (contract §7): 1561 added, 439 already known
- `t =  172.6 s` every book PUBLISHED: PUBLISHED 2000

## Evidence

| Check | Result | Detail |
|---|---|---|
| the failure happened with books still to ingest | PASS | 1571 books not yet past ingestion right after it (PENDING 1571, DOWNLOADED 326, INDEXING 100, PUBLISHED 3) |
| POST /control/jobs added exactly the books without evidence, and there were some | PASS | 1561 added, 439 already known; evidence for 439 |
| no book with _READY was ingested again | PASS | in the whole run 1999 books downloaded and 0 found already in the datalake, for 2000 books; the recovered DOWNLOADED went to index-queue, not ingest-queue |
| every book PUBLISHED | PASS | 2000 of 2000 (PUBLISHED 2000) |
| no book FAILED | PASS | FAILED = 0 |
| book-state has each book once | PASS | total = 2000, books = 2000 |
| every book complete in the datalake | PASS | 2000 of 2000 with header, body and _READY; 0 without _READY |
| index_log has each book once | PASS | 2000 documents, books = 2000 |
| index_books has each book once | PASS | 2000 documents, books = 2000 |

Files: `control-*.json` (GET /control/status), `status-*.json` (GET /status of the services at the end), `timeline.csv` (books by state every 0.5 s), `logs/` (every service, with timestamps).
