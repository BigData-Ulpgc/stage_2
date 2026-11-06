package es.ulpgc.bigdata.indexing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Publishes the index (DR-08, plan §6.5): makes visible to Search, in one step, every book
 * indexed since the last publication. A publication on top of CURRENT = V:
 * <ol>
 *   <li>finds the books to publish (D1, D2): those of index_log that are not in index_books, and
 *       those of a publication that died before switching CURRENT (index_books above V);</li>
 *   <li>records them in index_books with version V+1 ($setOnInsert). Search does not show them
 *       yet: it shows the versions &lt;= CURRENT, and CURRENT is still V;</li>
 *   <li>switches CURRENT to V+1 only if it is still at V (CAS, the fencing of coordination
 *       contract §5): this is the moment the books become visible, all at once;</li>
 *   <li>writes the history entry of V+1 (D3).</li>
 * </ol>
 * If the process dies between 2 and 3, the books stay in index_books above CURRENT, invisible;
 * the next publication (or the start of the service) completes them (D2). If it dies between 3
 * and 4, the next publication (or the start) writes the history entry from index_books (D3).
 * The same if step 4 fails: the call still answers 200, because a 200 means that CURRENT points
 * to the new version, and it does (review of #32).
 *
 * If the CAS fails, another replica published first: the whole publication is repeated on top
 * of the new CURRENT, up to MAX_ATTEMPTS times (D4). The books that replica already published
 * are in index_books, so they are not published twice.
 *
 * Thread safety (D7): one publication at a time in this process, with a lock of its own, NOT
 * IndexingService's writeLock: publishing only reads index_log and writes the publication
 * collections, so it does not stop the updates. Between replicas only the CAS orders them.
 *
 * Two replicas that publish on top of the same V both record their books with V+1, and only
 * one switches CURRENT. The books of the other one become visible in that V+1 too (they are
 * complete in the index, so Search is right), but CURRENT.books and the history of V+1 did not
 * count them. So every publication first checks CURRENT against index_books (repair): the
 * books of CURRENT must be exactly those with a version <= it, as Search's fixture also writes.
 *
 * After every publish call, still inside the lock, the PublicationNotifier is handed what the
 * call answers (DR-08b), so the calls never overlap and the versions arrive in order. It must
 * return at once: the Hazelcast one only queues the announcement (T10 and the IndexPublished
 * event) for a thread of its own. If it fails, the call still answers what was published:
 * CURRENT has already switched.
 */
public final class Publisher {

    /** Publications attempted when the CAS fails because other replicas keep publishing (D4). */
    static final int MAX_ATTEMPTS = 3;

    private static final Logger LOG = LoggerFactory.getLogger(Publisher.class);

    private final PublicationRepository publications;
    private final String defaultCollection;
    private final Clock clock;
    private final PublicationNotifier notifier;
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * @param defaultCollection the collection of the inverted index for the first version (D8):
     *                          later versions keep the one of CURRENT
     */
    public Publisher(PublicationRepository publications, String defaultCollection, Clock clock,
                     PublicationNotifier notifier) {
        this.publications = Objects.requireNonNull(publications, "publications");
        this.defaultCollection = Objects.requireNonNull(defaultCollection, "defaultCollection");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.notifier = Objects.requireNonNull(notifier, "notifier");
    }

    /** Without notifications (PublicationNotifier.NONE). */
    public Publisher(PublicationRepository publications, String defaultCollection, Clock clock) {
        this(publications, defaultCollection, clock, PublicationNotifier.NONE);
    }

    /**
     * POST /index/publish. Idempotent: if there is nothing new it changes nothing and describes
     * CURRENT as it is (D5).
     *
     * @throws IllegalStateException if the CAS failed MAX_ATTEMPTS times (500), or CURRENT does
     *                               not follow the contract
     */
    public PublishResult publish() {
        return publishSelected(Unpublished::all);
    }

    /**
     * At the start of the service (D2, D3): repairs the history entry of CURRENT if it is
     * missing and completes a publication that died after recording its books and before
     * switching CURRENT. Only those books: the ones indexed afterwards wait for an explicit
     * publish, as always.
     *
     * @return what was published; empty if nothing was interrupted
     */
    public Optional<PublishResult> recover() {
        PublishResult result = publishSelected(Unpublished::interrupted);
        return result.booksPublished() == 0 ? Optional.empty() : Optional.of(result);
    }

    private PublishResult publishSelected(Function<Unpublished, List<Integer>> select) {
        lock.lock();
        try {
            for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
                Optional<PublishResult> result = attempt(select);
                if (result.isPresent()) {
                    announce(result.get());
                    return result.get();
                }
                LOG.info("CURRENT changed during the publication (attempt {} of {}): another replica published",
                        attempt, MAX_ATTEMPTS);
            }
            throw new IllegalStateException("CURRENT cambió en " + MAX_ATTEMPTS
                    + " intentos seguidos: otras réplicas están publicando a la vez");
        } finally {
            lock.unlock();
        }
    }

    /** DR-08b: never turns a publication that happened into an error. */
    private void announce(PublishResult result) {
        try {
            notifier.afterPublish(result);
        } catch (RuntimeException e) {
            LOG.warn("version {} is published, but announcing it failed: repaired later", result.version(), e);
        }
    }

    /** One publication on top of the CURRENT read now; empty if the CAS failed. */
    private Optional<PublishResult> attempt(Function<Unpublished, List<Integer>> select) {
        Optional<Publication> current = publications.current();
        if (current.isPresent()) {
            current = repair(current.get());
            if (current.isEmpty()) {
                return Optional.empty();                // CURRENT moved while repairing it
            }
        }
        Integer currentVersion = current.map(Publication::version).orElse(null);

        List<Integer> books = select.apply(publications.unpublished(currentVersion));
        if (books.isEmpty()) {
            return Optional.of(current
                    .map(c -> new PublishResult(c.version(), 0, c.books(), c.publishedAt()))
                    .orElse(new PublishResult(null, 0, 0, null)));
        }

        int next = currentVersion == null ? 1 : currentVersion + 1;
        publications.addBooks(books, next);
        long total = publications.countVisible(next);
        String collection = current.map(this::collectionOf).orElse(defaultCollection);
        String publishedAt = clock.instant().truncatedTo(ChronoUnit.SECONDS).toString();   // D6

        if (!publications.switchCurrent(currentVersion, new Publication(next, total, collection, publishedAt))) {
            return Optional.empty();
        }
        // CURRENT already points to next: the publication happened even if this write fails,
        // and the next publication or the next start writes the entry from index_books (D3).
        try {
            publications.recordHistory(new PublishedVersion(next, books, total, publishedAt, collection));
        } catch (RuntimeException e) {
            LOG.warn("version {} is published, but its history entry could not be written: repaired later", next, e);
        }
        LOG.info("published version {}: {} new books, {} visible", next, books.size(), total);
        return Optional.of(new PublishResult(next, books.size(), total, publishedAt));
    }

    /**
     * Makes CURRENT and its history entry agree with index_books, the source of truth: the books
     * visible in a version are those whose first version is <= it ($setOnInsert never changes
     * it). Two cases:
     * <ul>
     *   <li>CURRENT.books counts fewer books: another replica recorded its books with the
     *       version of CURRENT after it was switched (see the class). Fixed with the same CAS,
     *       and the history of CURRENT is rewritten with all its books;</li>
     *   <li>the history entry is missing: the publication died after the CAS (D3).</li>
     * </ul>
     *
     * @return CURRENT as it is now; empty if it moved (another replica published meanwhile)
     */
    private Optional<Publication> repair(Publication current) {
        int version = current.version();
        long visible = publications.countVisible(version);
        if (visible != current.books()) {
            Publication counted = new Publication(version, visible, collectionOf(current), current.publishedAt());
            if (!publications.switchCurrent(version, counted)) {
                return Optional.empty();
            }
            LOG.info("version {} counted again: {} visible books, CURRENT said {}", version, visible, current.books());
            writeHistoryFromIndexBooks(counted);
            return Optional.of(counted);
        }
        if (!publications.hasHistory(version)) {
            writeHistoryFromIndexBooks(current);
        }
        return Optional.of(current);
    }

    /** D8: the collection of CURRENT; MONGO_COLLECTION if whoever wrote CURRENT did not say (Search needs one). */
    private String collectionOf(Publication current) {
        return current.collection() == null ? defaultCollection : current.collection();
    }

    private void writeHistoryFromIndexBooks(Publication current) {
        List<Integer> books = publications.booksOfVersion(current.version());
        publications.recordHistory(new PublishedVersion(current.version(), books, current.books(),
                current.publishedAt(), current.collection()));
        LOG.info("history of version {} written from index_books: {} books", current.version(), books.size());
    }
}
