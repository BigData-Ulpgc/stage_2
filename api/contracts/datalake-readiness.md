# Datalake readiness contract

**Owner:** Carlos Falcón (CF-07). **Writer:** Ingestion Service. **Readers:** Indexing Service,
Control Module (recovery, CF-10). **Implemented by:** `RangeBasedDatalake` in `common/datalake`
(Daniel Rodríguez, DR-06 adds the `_READY` marker).

**Status: v1.1, proposal for review.** v1.1 follows the code of DR-06 (#24): unique temporary
names, and "ready" is the marker together with header and body. Changing anything in this file
needs the approval of the other two members (plan §7).

This contract covers what goes through the files of the datalake: how a book is stored and
when it is ready to be indexed. It answers assignment §3.2.1 (*"expose a book as ready only
after its header and body files have been written completely"*) and §3.3 (*"readiness markers"*
as the contract of a non-HTTP interaction). What goes through Hazelcast is in
[`coordination.md`](coordination.md); the inventory of all interactions is
[`../endpoints.md`](../endpoints.md).

## 1. Layout

One folder per range of 1,000 ids below `DATA_DIR/datalake/range/`:

```
DATA_DIR/datalake/range/<START>-<END>/<id>.header.txt    header of the book (UTF-8)
DATA_DIR/datalake/range/<START>-<END>/<id>.body.txt      body of the book (UTF-8)
DATA_DIR/datalake/range/<START>-<END>/<id>._READY        readiness marker, written last (DR-06)
```

- `START = (id / 1000) * 1000`, `END = START + 999`, each with at least 5 digits:
  1342 → `01000-01999/`, 84 → `00000-00999/`, 123456 → `123000-123999/`.
- Ids follow `BookIds` (1 to 999,999,999, canonical form), as everywhere else.
- `header` and `body` are the split of the raw Gutenberg text (`BookSplitter`): the header is
  everything before the `*** START OF` line, the body everything between that line and the
  `*** END OF` marker, both trimmed, with `\n` line endings.
- Only `RangeBasedDatalake` builds these paths, from `DataLayout` (`DATA_DIR`). No service writes
  `"datalake/..."` by hand.

Why ranges and not one folder per book (Stage 1's `book`): with hundreds of thousands of books
the root holds one entry per 1,000 ids and a range folder 3,000 files (plus any temporary file
left by a crash), and the folder of
a book is still computed from its id without listing anything.

## 2. Writing (Ingestion only)

A book is written in this order, each file **atomically**: the content goes to a temporary file
with a unique name in the same folder (`<file>.<uuid>.tmp`), which is then moved onto `<file>`
(`ATOMIC_MOVE`):

1. `<id>.header.txt`
2. `<id>.body.txt`
3. `<id>._READY`, whose content is one line: the UTC instant of the ingestion in ISO-8601 to the
   second, e.g. `2026-11-04T10:15:00Z`. The recovery of CF-10 and the metrics of CF-12 read it
   through `RangeBasedDatalake` (a `readyAt(id)` added when needed), never by its path.

Rules:

- **A ready book (marker, header and body) is never modified.** Saving it again writes nothing
  and returns where it is (DR-06). The books of the datalake are immutable once ready: the index
  publication relies on it (`index-publication.md` §1).
- A failed or interrupted write leaves the book not ready, so it does not exist (§3). Writing it
  again replaces the half-written files. A leftover `.tmp` is never reused: every reader ignores
  it.
- The temporary name is unique per write so that two writes of the same file at once never
  share one: with a fixed name, one could move the other's half-written file (found and fixed in
  DR-06).
- If the file system does not support an atomic move, the move is still done, non-atomically.
  This is the only case in which a reader could see a half-written header or body, and it is
  still never `_READY` before both are complete.

## 3. Readiness (everyone)

**A book is in the datalake if and only if its `<id>._READY` exists, together with its header
and body.** Header and body without `_READY` are a write in progress or an interrupted one, and
a marker whose header or body was lost (deleted by hand, for instance) is a damaged book: both
count as absent for every reader, and the next `save` writes the book again.

| Who | Uses | Sees |
|---|---|---|
| Ingestion | `locate(id)`: `already_available` of `POST /ingest/{id}`, `GET /ingest/status/{id}`; `listBookIds()`: `GET /ingest/list` | only ready books |
| Indexing | `locate(id)` before indexing a book; `listBookIds()` in a rebuild (DR-10) | only ready books; a book without `_READY` is 404 `NOT_FOUND` in `POST /index/update` and `NOT_READY` in the queue (coordination §4.3) |
| Control | `listBookIds()` in the recovery: ready books become `DOWNLOADED` (coordination §7) | only ready books |

Readers never read `.tmp` files and never write anything: in Docker the datalake is mounted
read-only in Indexing and in the Control (DP-08; plan §3.4, and coordination §7 for the Control).

**Order with the coordination contract.** Ingestion completes T3 (`INGESTING → DOWNLOADED`, and
offers the id to `index-queue`) only after `save` has returned, that is, after `_READY` exists
(coordination principle 2). So Indexing never takes a book from the queue that is not ready.

## 4. Failures

| Failure | State of the datalake | What happens next |
|---|---|---|
| Ingestion dies while writing the header or the body | a `.tmp` and maybe some of the files, no `_READY` | The book is absent; its lease expires and it is downloaded again, overwriting. The `.tmp` stays, ignored |
| It dies after the body and before `_READY` | header and body, no `_READY` | The same: absent, downloaded again |
| It dies after `_READY` and before T3 | ready book | Lease expires; the next worker finds it ready (`already_available`) and completes T3 without downloading |
| Two ingestions of the same book at once (`POST /ingest` and a worker; two workers cannot, the claim prevents it) | both may write before either writes `_READY` | Each write goes through its own temporary file, so every move is a complete file and the last one wins: one complete book of the same text. The second can rewrite a book the first already made ready, and its marker then has the later time |
| The disk is full or not writable | no `_READY` | `save` fails: 500 in `POST /ingest`, a transient failure of the attempt in the queue (`INTERNAL_ERROR`) |
| A datalake written before DR-06 (no `_READY` at all) | header and body only | Every book counts as absent and is ingested again (one-off, when upgrading) |

## 5. Limits

- Readiness depends on the local file system and on atomic moves within one folder: true on
  the single Stage 2 host and on a Docker volume. In Stage 3, with the datalake on several
  machines, the marker has to live in shared storage or be replaced (report §8).
- The marker protects against the death of a process, not against a power cut or a crash of the
  operating system: no file is forced to disk (`fsync`), so after such a crash a `_READY` could
  survive while its header or body did not. Accepted for Stage 2; to be stated in the report.
- `listBookIds()` lists every range folder of the datalake, so its cost grows with the number of
  books (measured in the scale experiments, DR-13): it is for administration, rebuilds and
  recovery, never for the path of a single book (`locate` is computed from the id).
