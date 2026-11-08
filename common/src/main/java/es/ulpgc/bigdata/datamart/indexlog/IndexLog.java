package es.ulpgc.bigdata.datamart.indexlog;

import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

/**
 * The indexing log (plan §6.5, DR-07): the persistent evidence that a book is COMPLETE in the
 * inverted index. Indexing writes it LAST, after the posting lists of the book are flushed, so
 * a book in the log always has its terms in the index. The metadata collection cannot play this
 * role: a book's metadata are saved before its terms, so a book whose flush failed is in
 * "books" without being in the index.
 *
 * Who relies on it:
 *  - Indexing: GET /index/status (books_indexed, last_update), and publish (DR-08) makes
 *    visible the books of the log that are not published yet;
 *  - the coordination contract: a book becomes INDEXED (T7) only once it is in the log;
 *  - the Control (CF-10): if the Hazelcast cluster is lost, a book in the log is INDEXED again.
 *
 * Rules every implementation follows:
 *  - markIndexed is idempotent: marking a book again never duplicates it, it only moves its
 *    indexed_at to the new time if that is later: never backwards, whatever order the writes
 *    arrive in. It is "the last time it was indexed", unlike the version of index_books, which
 *    is the first one and never changes.
 *  - markIndexed is NOT all or nothing: if it fails halfway, part of the ids may be marked.
 *    Repeating the call is always safe.
 *  - Times are kept with millisecond precision (what a MongoDB date holds).
 */
public interface IndexLog {

    /**
     * Records that these books are complete in the index, all at the same time (the end of the
     * batch that wrote them). An empty collection does nothing.
     *
     * @throws IllegalArgumentException if an id is not a valid book id (BookIds); nothing is
     *                                  written then
     */
    void markIndexed(Collection<Integer> bookIds, Instant indexedAt);

    /** How many different books are in the log. */
    long count();

    /** The most recent indexed_at of the log; empty if the log is empty. */
    Optional<Instant> lastIndexedAt();

    /**
     * Every book of the log. The Control reads it when it rebuilds the Hazelcast state after the
     * cluster is lost (CF-10, coordination contract §7): a book in the log is INDEXED again.
     */
    Set<Integer> indexedIds();
}
