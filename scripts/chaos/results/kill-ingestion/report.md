# Kill an Ingestion replica in the middle of its books: PASS

Run 2026-11-08 18:12 UTC, commit c06ff6a, 2000 synthetic books (data/chaos/books-2000), 2 Ingestion replica(s) with 1 worker(s). LEASE_SECONDS=15, LEASE_CHECK_SECONDS=3, PUBLISH_INTERVAL_S=5.

## Expected

Two Ingestion replicas share `ingest-queue`. One is killed with SIGKILL (`docker kill`) while it is
working: its process dies without releasing anything. The books it had claimed stay `INGESTING`
with its lease; their files may be half written, without `_READY`, so they do not exist for anyone
(datalake-readiness §4). When the lease expires (LEASE_SECONDS, 15 s here) the Control requeues
them (T4) and the other replica downloads them again, whole. Nothing is lost, nothing ends
`FAILED`, and every book is in the datalake once (contract §7, "an Ingestion replica dies").

## What happened

t = 0 is the POST /control/jobs.

- `before` system up: 2 Ingestion replica(s) with 1 worker(s); 2000 books
- `t =    5.7 s` POST /control/jobs: 2000 added, 0 already known
- `t =    5.9 s` a twentieth of the books ingested: PENDING 1824, INGESTING 1, DOWNLOADED 179
- `t =    7.3 s` docker kill of replica af21a4f75cec, frozen first with docker pause while it held a claimed book (try 1); right after: PENDING 1725, INGESTING 1, DOWNLOADED 274
- `t =   23.9 s` every book past ingestion: DOWNLOADED 2000
- `t =   23.9 s` the Control requeued 1 books with an expired lease

## Evidence

| Check | Result | Detail |
|---|---|---|
| the failure happened with books still to ingest | PASS | 1726 books not yet past ingestion right after it (PENDING 1725, INGESTING 1, DOWNLOADED 274) |
| the lost books were requeued by their lease | PASS | lease_check.requeued_or_failed = 1 |
| every book DOWNLOADED | PASS | 2000 of 2000 (DOWNLOADED 2000) |
| no book FAILED | PASS | FAILED = 0 |
| book-state has each book once | PASS | total = 2000, books = 2000 |
| every book complete in the datalake | PASS | 2000 of 2000 with header, body and _READY; 0 without _READY |

Files: `control-*.json` (GET /control/status), `status-*.json` (GET /status of the services at the end), `timeline.csv` (books by state every 0.5 s), `logs/` (every service, with timestamps).
