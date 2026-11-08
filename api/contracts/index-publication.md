# Index publication contract (MongoDB)

**Owner:** Daniel Rodríguez. **Writer:** Indexing Service. **Readers:** Search Service, Control
Module (publication trigger and recovery, CF-10). **Implemented by:** `common/datamart`
(`MongoMetadataRepository`, `MongoInvertedIndex`, `MongoIndexLog`, `MongoPublishedBooks`),
Indexing's `MongoPublicationRepository` and `Publisher` (DR-08), and Search's
`IndexSnapshotLoader` (DP-06).

**Status: v1.0, proposal for review.** It writes down plan §6.3 and §6.5, which are not in the
repository, as the code of `develop` implements them (DR-04 to DR-08b, DP-06, CF-10). Nothing
changes in the code. Changing anything in this file (collections, fields, the order of the
writes, the visibility rule) needs the approval of the other two members (plan §7).

This contract covers what goes through MongoDB: where the index and the metadata are stored,
and when a book becomes visible to Search. It answers assignment §3.2.2 (*"publish a completed
update before search can read it"*) and §3.3 (*"shared-state layout, ownership, and
synchronization rules"* as the contract of a non-HTTP interaction). What goes
through Hazelcast is in [`coordination.md`](coordination.md) and the files of the datalake in
[`datalake-readiness.md`](datalake-readiness.md); the inventory of all interactions is
[`../endpoints.md`](../endpoints.md).

## 1. Principles

- **Publishing does not copy the index.** With hundreds of thousands of books, copying the
  index on every publication would not scale. A ready book of the datalake never changes
  (`datalake-readiness.md` §2), so the entries of a published book in the index never change
  either: a publication only decides **which books are visible**.
- **A book is visible if and only if it is in `index_books` with a version up to the one of
  `CURRENT`.** Every reader applies this rule; nothing else makes a book visible.
- **One atomic switch.** A new version becomes visible when `CURRENT` changes, and `CURRENT` is
  one document, so a reader sees either all of version V or all of V+1, never part of V+1.
- **Only Indexing writes.** Search and the Control only read these collections.
- **Collections may run ahead of the publication.** `books`, `inverted_index` and `index_log`
  can hold books that are not visible yet (or never will be, if indexing them failed half-way).
  Readers always start from the visible ids.

## 2. Collections

All in the database `MONGO_DATABASE` (`search_engine`):

| Collection | One document | Written by | Read by | Indexes |
|---|---|---|---|---|
| `books` | per book: its metadata (§3) | Indexing, before the posting lists | Search, by `_id` only | `_id` |
| `inverted_index` (`MONGO_COLLECTION`) | per term: its posting list (§4) | Indexing, `bulkWrite` + `$addToSet` | Search, by `term` | unique on `term` |
| `index_log` | per book whose posting lists are complete (§5) | Indexing, after the posting lists | Indexing (`GET /index/status`, publication), Control (recovery) | `_id`, `indexed_at` |
| `index_books` | per published book: its first version (§6) | Indexing, `$setOnInsert` | Search, Control (recovery) | `_id`, `version` |
| `index_meta` | only `CURRENT`, the visible version (§6) | Indexing, one atomic update | Search, Indexing | `_id` |
| `index_publications` | per version: its history entry (§6) | Indexing | Indexing (repair), the report | `_id` |

Every service opens **one `MongoClient` per process** (`MongoSettings.openClient`) and shares its
database with all its repositories. No repository talks to MongoDB in its constructor, so a
service starts while MongoDB is down and its requests fail with 503 until it is back; the
indexes are created before the first write that needs them (`createIndex` is idempotent).

## 3. `books` (metadata, plan §6.3)

```
{_id: 1342, title: "Pride and Prejudice", author: "Jane Austen", language: "en",
 year: 1998, year_source: "release_date", release_date: "June 1, 1998",
 header_path: "...", body_path: "..."}
```

- `_id` is the book id (`BookIds`, an integer). It is unique and indexed; there are no other
  indexes because Search filters in memory and reads only by `_id` (DR-04).
- Always the 9 fields, in `snake_case`; a missing value is an explicit `null`.
- `author`: the `Author:` line or, if there is none, `Creator:` (`Translator:` never counts).
- `language`: ISO 639-1 code of the **first** language of the header (`LanguageCodes`), or
  `null` if it is not recognised.
- `year`: the `Original publication` year if the header has one, else the year of
  `Release date`, which is the date of the ebook, not of the work. `year_source` says which one
  (`original_publication`, `release_date` or `null`). Of the 200 real headers counted in DR-04,
  only 9 have `Original publication`.
- `header_path` and `body_path` always use `/` as separator.
- Written with `replaceOne(upsert)` per book (`MetadataParser` builds it from the header).
  Search reads it with `findByIds`, in blocks of 1,000 ids.

## 4. `inverted_index`

```
{term: "boat", postings: [10, 20, 1342]}
```

- One document per term (unique index on `term`). `postings` holds the ids of every book whose
  body contains the term, **published or not**: Search intersects it with its visible ids.
- Written in one unordered `bulkWrite` per flush, one upsert with `$addToSet` per term: indexing
  a book again adds nothing, so it is idempotent. MongoDB keeps the array in arrival order, so
  readers sort it.
- The terms come from the `Tokenizer` of `common` (ASCII letters and digits in lowercase, at
  least 2 characters, no stopwords), the same one Search applies to the query. Changing the
  tokenizer or the stopwords means rebuilding the index.
- The name is `CURRENT.collection` (§6), not a constant: Search reads the posting lists from the
  collection `CURRENT` names. Today it is always `MONGO_COLLECTION` (`inverted_index`).

## 5. `index_log`

```
{_id: 1342, indexed_at: ISODate("2026-11-04T17:02:13.517Z")}
```

- One document per book whose posting lists are **completely** written. It is the source of
  the books to publish (§7) and of `books_indexed` in `GET /index/status` (DR-07).
- Written with `$max` on `indexed_at` (upsert), so writes that reach MongoDB in a different
  order never move the time backwards.
- **Why not `books`:** the metadata is saved before the terms, so a book whose flush failed is in
  `books` without being in the index. Publishing from `books` would make an incomplete book
  visible.

Write order of one book (Indexing, `IndexingService.update`, also when the writer of
`index-queue` indexes it):

1. the book must be ready in the datalake (`_READY`); a book without terms is rejected before
   anything is written;
2. `books` (metadata);
3. `inverted_index` (posting lists, flushed synchronously);
4. `index_log`.

If any step fails, the book is not in `index_log`: it is neither counted nor published, and
indexing it again repeats the steps safely (upserts and `$addToSet`).

## 6. Publication collections

```
index_meta          {_id: "CURRENT", version: 42, collection: "inverted_index", books: 187,
                     published_at: "2026-11-03T10:15:00Z"}
index_books         {_id: 1342, version: 41}
index_publications  {_id: 42, books: [1500, 1501], books_published: 2, total_books: 187,
                     published_at: "2026-11-03T10:15:00Z", collection: "inverted_index"}
```

| Document | Field | Type | Meaning |
|---|---|---|---|
| `CURRENT` | `version` | int | The visible version; versions start at 1 and grow by 1 |
| | `collection` | string | Collection of the posting lists of this version (§4) |
| | `books` | int | Books visible in this version: always the number of `index_books` with `version` ≤ it |
| | `published_at` | string | ISO-8601 UTC to the second, the same text as the `IndexPublished` event and the API |
| `index_books` | `_id` | int | Book id |
| | `version` | int | The **first** version in which the book became visible. It never changes |
| `index_publications` | `_id` | int | The version |
| | `books` | int[] | Books that became visible in this version, sorted |
| | `books_published`, `total_books` | int | Size of `books`; `CURRENT.books` of this version |
| | `published_at`, `collection` | string | As in `CURRENT` |

**The version of a book never changes** (`$setOnInsert`), even if the book is indexed again. If
it were rewritten with V+1, a Search that has read `CURRENT` = V and then filters `version` ≤ V
would lose the book for a moment. The reload of Search also relies on it: a book that is already
visible keeps its metadata in memory (§8).

There is no `CURRENT` until the first publication: Search answers 503 `INDEX_NOT_READY`, and
the recovery of the Control finds no published book.
Numbers are read as `Number`, so another program may store them as int64.

## 7. Publishing version V+1 (Indexing, `Publisher`)

**Who triggers it.** The Control calls `POST /index/publish` when `PUBLISH_BATCH` books are
`INDEXED`, or `PUBLISH_INTERVAL_S` after the last publication if any book is `INDEXED`
(`indexing-openapi.yaml`). The call is safe to repeat: with nothing new it changes nothing and
describes `CURRENT` as it is.

On top of `CURRENT` = V (no `CURRENT` counts as V = 0):

1. **Repair** `CURRENT` if it does not match `index_books` (see §9, two publishers), and write
   its history entry if it is missing.
2. **Find the books to publish:** those of `index_log` that are not in `index_books` (one
   `$lookup` by `_id`), plus those of `index_books` with a version above V (a publication that
   died before step 4).
3. **Record them** in `index_books` with version V+1 (`$setOnInsert`, one unordered
   `bulkWrite`). Nobody sees them yet: `CURRENT` is still V.
4. **Switch `CURRENT`** to V+1 **only if it is still at V**: one `updateOne` filtered by
   `{_id: "CURRENT", version: V}`, or an `insertOne` for the first version, which the unique
   `_id` lets only one publisher do. This is the moment the books become visible, all at once.
   If the switch matches nothing, another replica published first: the publication starts again
   from step 1 on top of the new `CURRENT`, up to 3 attempts (then 500).
5. **Write the history entry** of V+1 in `index_publications` (`replaceOne(upsert)`, so it is
   idempotent).
6. **Tell the rest of the system** (DR-08b): T10 moves the published books from `INDEXED` to
   `PUBLISHED` in `book-state`, and the event `IndexPublished` wakes up Search. When and from
   which thread is in `coordination.md` (§4.1 and §6). If this step fails, the publication
   still happened: `CURRENT` has already switched, Search finds it with its periodic check
   (§8), and coordination §7 repairs T10.

One publication at a time per Indexing process (a lock of `Publisher`); between replicas only
the conditional switch of step 4 orders them (the fencing of `coordination.md` §5).

## 8. Reading (Search, `IndexSnapshotLoader`)

1. Read `CURRENT` (`version` and `collection`). Without `CURRENT` there is nothing to serve
   (503 `INDEX_NOT_READY`).
2. Read the ids of `index_books` with `version` ≤ `CURRENT.version` (only `_id`).
3. Read the metadata of those ids from `books`. When the version only moved forward on the same
   collection, the metadata already in memory is reused and only the new ids are read. A visible
   id without metadata stays visible with `null` fields.
4. Answer each search with the posting lists of `CURRENT.collection` intersected with the
   visible ids, filtered in memory by author, language and year.

Search reloads when `IndexPublished` arrives or when its periodic check of `CURRENT`
(`SEARCH_POLL_SECONDS`) finds a newer version, and swaps the snapshot at once. The order
`CURRENT` first, `index_books` after, is what makes the switch atomic for it: the books of V+1
are recorded before `CURRENT` points to V+1.

## 9. Failures and recovery

| Failure | What happens | Who fixes it |
|---|---|---|
| Indexing dies while writing a book (before `index_log`) | The book may be in `books` and partly in `inverted_index`, but not in `index_log`: invisible and not counted | Its lease expires and the book is indexed again (coordination §7) |
| It dies between steps 3 and 4 of a publication | The books are in `index_books` above `CURRENT`: invisible | At the start of Indexing (`Publisher.recover`, in the background) or at the next publication (step 2) |
| It dies between steps 4 and 5 | The books are visible, but the history entry of V+1 is missing | Step 1 of the next publication, or the start, writes it from `index_books` |
| It dies between steps 5 and 6 | Visible, but still `INDEXED` in `book-state` | Indexing applies T10 again (coordination §7) |
| Two replicas publish on top of the same V | Both record their books with V+1, only one switches `CURRENT`. The books of the other one are visible in V+1 too (they are complete in the index, so Search is right), but `CURRENT.books` did not count them | Step 1 of the next publication counts again (same conditional switch) and rewrites the history of V+1 |
| MongoDB is down | The services keep running and `GET /status` answers; `update`, `publish` and the searches answer 503 (Search reads the posting lists from MongoDB on every query). Nothing is half-published: the switch of step 4 either happened or not | Retried by whoever called (the Control, the queue, the client) |
| The Hazelcast cluster is lost | `book-state` is gone, MongoDB is not | The Control's recovery (coordination §7) rebuilds it from `index_books` up to `CURRENT` (`PUBLISHED`) and `index_log` (`INDEXED`) |

## 10. Configuration

| Variable | Default | Used by |
|---|---|---|
| `MONGO_URI` | `mongodb://localhost:27017` (`mongodb://mongo:27017` in Docker) | every service |
| `MONGO_DATABASE` | `search_engine` | every service |
| `MONGO_COLLECTION` | `inverted_index` | Indexing: `collection` of the first version |
| `PUBLISH_BATCH`, `PUBLISH_INTERVAL_S` | 100, 30 | Control: when to call `POST /index/publish` |
| `SEARCH_POLL_SECONDS` | 5 | Search: periodic check of `CURRENT` |

## 11. Limits

- A MongoDB document is at most 16 MB, so one posting list holds about a million ids, and one
  history entry about a million books.
- `$addToSet` compares each new id with the whole posting list, so writing a batch gets slower as
  the index grows; DR-13 measures it.
- One `inverted_index` for every version. A full rebuild into a new collection, switched through
  `CURRENT.collection`, is DR-10; that PR will add it to this contract.
