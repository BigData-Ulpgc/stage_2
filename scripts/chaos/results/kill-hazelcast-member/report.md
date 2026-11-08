# Kill one Hazelcast member: PASS

Run 2026-11-08 18:13 UTC, commit c06ff6a, 2000 synthetic books (data/chaos/books-2000), 1 Ingestion replica(s) with 1 worker(s). LEASE_SECONDS=15, LEASE_CHECK_SECONDS=3, PUBLISH_INTERVAL_S=5.

## Expected

`book-state` and the queues have one backup on the other member (hazelcast.yaml, backup-count 1).
Member hz-1 is killed with SIGKILL during the ingestion. The clients (Control and Ingestion) carry
on with hz-2, which promotes its backups; a transaction caught by the death is rolled back or
committed whole (TWO_PHASE), so no book is left half moved. The Control does NOT rebuild anything:
the `initialized` marker survives with the rest. When hz-1 starts again it rejoins and the
partitions migrate back. Nothing is lost and nothing ends `FAILED` (contract §7, "one member dies").

## What happened

t = 0 is the POST /control/jobs.

- `before` system up: 1 Ingestion replica(s) with 1 worker(s); 2000 books
- `t =    4.1 s` POST /control/jobs: 2000 added, 0 already known
- `t =    4.6 s` a twentieth of the books ingested: PENDING 1892, INGESTING 1, DOWNLOADED 108
- `t =   30.7 s` docker kill hz-1; right after, with hz-2 alone: PENDING 1848, INGESTING 2, DOWNLOADED 152
- `t =   37.8 s` every book past ingestion, with hz-2 alone: DOWNLOADED 2000
- `t =   48.1 s` hz-1 started again and healthy

## Evidence

| Check | Result | Detail |
|---|---|---|
| the failure happened with books still to ingest | PASS | 1848 books not yet past ingestion right after it (PENDING 1848, INGESTING 2, DOWNLOADED 152) |
| the Control did not rebuild book-state | PASS | recovery.runs 1 before, 1 after |
| every book DOWNLOADED | PASS | 2000 of 2000 (DOWNLOADED 2000) |
| no book FAILED | PASS | FAILED = 0 |
| book-state has each book once | PASS | total = 2000, books = 2000 |
| every book complete in the datalake | PASS | 2000 of 2000 with header, body and _READY; 0 without _READY |

Files: `control-*.json` (GET /control/status), `status-*.json` (GET /status of the services at the end), `timeline.csv` (books by state every 0.5 s), `logs/` (every service, with timestamps).
