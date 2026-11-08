# Failure tests (CF-11)

Documented failure tests of the coordination (plan §10, CF-11). Each one causes a failure in the
middle of real work and checks that the system ends where the coordination contract
(`api/contracts/coordination.md` §7) says it must: every book in its final state, exactly once,
nothing `FAILED`, every book complete in the datalake and, with Indexing, in the index.

| Scenario | Failure | What must hold |
|---|---|---|
| `kill-ingestion` | `docker kill` (SIGKILL) of one of 2 Ingestion replicas while it ingests | Its books are requeued when their lease expires and the other replica finishes them; half-written books (no `_READY`) do not count |
| `kill-hazelcast-member` | `docker kill hz-1` during the ingestion | Nothing is lost (backup on hz-2), the clients carry on, the Control rebuilds nothing; hz-1 rejoins |
| `kill-control` | `docker kill` of the Control while Indexing indexes | Ingestion and Indexing keep working, nothing is published until it is back; the new Control rebuilds nothing and publishes the rest |
| `restart-hazelcast` | `docker kill hz-1 hz-2`, then both started again (empty) | The Control rebuilds `book-state` from `index_books`, `index_log` and `_READY` (CF-10); the books without evidence are sent again; no book with `_READY` is downloaded again; all end `PUBLISHED` |

## How to run them

**Requirements:** Docker with Compose (about 6 GB for Docker), curl, git and Python 3.

```bash
python3 scripts/chaos/chaos.py all                  # the four, about 20 minutes with 2,000 books
python3 scripts/chaos/chaos.py kill-control         # only one
python3 scripts/chaos/chaos.py all --books 400      # quicker, the same checks
```

- `COMPOSE=docker-compose` for the old command; `--no-build` uses the images already built.
- The first run generates the synthetic books (DR-16) into `data/chaos/books-<n>` (ignored by git).
- **It stops and replaces whatever runs in the compose project** (`docker compose down` at the start of
  every scenario and at the end). It uses a datalake (`data/chaos/datalake`) and a database
  (`chaos_test`) of its own and deletes both at the end, so the data of other work is not touched.
- Settings shorter than in production, so that a lost lease is requeued in seconds:
  `LEASE_SECONDS=15`, `LEASE_CHECK_SECONDS=3`, `PUBLISH_INTERVAL_S=5`. The rest comes from `.env`.
- Before killing a container its restart policy is removed (`docker update --restart=no`): otherwise
  `restart: unless-stopped` could bring it back by itself, and the scenario decides when it comes back.

The exit code is 0 only if every check of every scenario passed.

## What each scenario leaves

In `scripts/chaos/results/<scenario>/`, to commit with the report:

| File | What |
|---|---|
| `report.md` | **Expected** (the rule of the contract), **what happened** (each step with its time from the seed) and the **evidence** (each check, PASS or FAIL, with its numbers) |
| `control-*.json` | `GET /control/status` before the failure, after the recovery and at the end |
| `status-*.json` | `GET /status` of the Ingestion replicas (counters, `coordination_cost` when CF-12 is there) |
| `timeline.csv` | The books by state every 0.5 s: the stall and the recovery can be seen in it |
| `logs/` | `docker compose logs --timestamps` of every service: the lease check, the reconnections, the recovery |

`report.md` is the text of the failure tests in the report: the expected behaviour, what happened and
where the evidence is.
