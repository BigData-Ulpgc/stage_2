package es.ulpgc.bigdata.coordination;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.ReliableMessageListener;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.transaction.TransactionalQueue;
import es.ulpgc.bigdata.model.BookIds;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The coordination contract (api/contracts/coordination.md) as code: every transition T1-T11
 * of §4.1 is one method here, so no service touches the Hazelcast structures directly.
 *
 * How each transition is made atomic:
 * <ul>
 *   <li>A map change is a compare-and-set, replace(id, expected, next): it succeeds only if the
 *       entry is still exactly the value the caller read. The members compare the serialized
 *       bytes, so they need no class of ours (no EntryProcessor).</li>
 *   <li>A map change that also moves the id to or from a queue runs in ONE transaction with
 *       that queue operation (seed + offer, poll + claim, completion + offer, retry + offer). If
 *       the process dies before the commit, neither happens: a polled id goes back to its queue
 *       and no book is left in a state with no queue entry.</li>
 * </ul>
 *
 * Thread-safe: one instance per process, shared by all its workers. The methods do not log
 * (common leaves logging to each service): they return what happened.
 */
public final class Coordination implements AutoCloseable {

    /** Books seeded per transaction: one transaction per book would make a 100,000-book seed slow. */
    static final int SEED_BATCH = 1_000;
    /** Ids read per call when marking a publication (T10). */
    static final int PUBLISH_BATCH = 1_000;
    /**
     * Longest wait of one poll inside a claim transaction. A claim that may wait longer polls
     * again in a new transaction, so no transaction gets near its 2 minute timeout.
     */
    static final Duration MAX_POLL = Duration.ofSeconds(10);
    /** TWO_PHASE (the default) survives the crash of a member during the commit. */
    private static final TransactionOptions TRANSACTIONS = new TransactionOptions()
            .setTransactionType(TransactionOptions.TransactionType.TWO_PHASE);

    private final HazelcastInstance hz;
    private final CoordinationSettings settings;
    private final Clock clock;
    private final boolean ownsClient;
    private final IMap<Integer, BookTask> books;
    /** Ids taken from a queue and dropped by this instance (see claim). */
    private final AtomicLong invalidIdsDropped = new AtomicLong();
    private final AtomicLong idsSkipped = new AtomicLong();

    /** What one poll of a claim did with the id it took, if any. */
    private enum Drop { NONE, INVALID, NOT_WAITING }

    private record Polled(Optional<Claim> claim, Drop drop) {
        static final Polled NOTHING = new Polled(Optional.empty(), Drop.NONE);
    }

    private Coordination(HazelcastInstance hz, CoordinationSettings settings, Clock clock, boolean ownsClient) {
        this.hz = Objects.requireNonNull(hz, "hz");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.ownsClient = ownsClient;
        this.books = hz.getMap(CoordinationNames.BOOK_STATE);
    }

    /**
     * Opens its own client (CoordinationSettings.openClient); close() shuts it down. Blocks until
     * the cluster answers: call it after starting the HTTP server, in the background.
     */
    public static Coordination connect(CoordinationSettings settings) {
        return new Coordination(settings.openClient(), settings, Clock.systemUTC(), true);
    }

    /** On a client the caller owns (shared, or a test cluster); close() leaves it open. */
    public static Coordination on(HazelcastInstance hz, CoordinationSettings settings, Clock clock) {
        return new Coordination(hz, settings, clock, false);
    }

    public CoordinationSettings settings() {
        return settings;
    }

    // --- Control Module ------------------------------------------------------------------

    /**
     * T1: puts every id that book-state does not know yet as PENDING and offers it to
     * ingest-queue. An id already present is left as it is, whatever its state, so seeding
     * again never duplicates anything. Runs in transactions of {@link #SEED_BATCH} books.
     *
     * @return how many ids were new
     * @throws IllegalArgumentException if an id is not a valid book id (nothing is seeded)
     */
    public int seed(Collection<Integer> ids) {
        List<Integer> distinct = new ArrayList<>(new LinkedHashSet<>(ids));
        for (int id : distinct) {
            if (!BookIds.isValid(id)) {
                throw new IllegalArgumentException("invalid book id: " + id);
            }
        }
        int added = 0;
        for (int from = 0; from < distinct.size(); from += SEED_BATCH) {
            List<Integer> batch = distinct.subList(from, Math.min(from + SEED_BATCH, distinct.size()));
            long now = now();
            added += inTransaction(tx -> {
                TransactionalMap<Integer, BookTask> map = tx.getMap(CoordinationNames.BOOK_STATE);
                TransactionalQueue<Integer> queue = tx.getQueue(CoordinationNames.INGEST_QUEUE);
                int n = 0;
                for (int id : batch) {
                    if (map.putIfAbsent(id, BookTask.pending(id, now)) == null) {
                        queue.offer(id);
                        n++;
                    }
                }
                return n;
            });
        }
        return added;
    }

    /**
     * Recovery after the cluster lost its state (contract §7, CF-10): puts these books back in
     * {@code state} (DOWNLOADED, INDEXED or PUBLISHED), from the evidence of the persistent
     * storage. A DOWNLOADED book is offered to index-queue again, in the same transaction.
     *
     * An id the cluster does not know is restored; one that is PENDING is moved forward too (it
     * was seeded while the recovery ran, and the worker that polls its old id from ingest-queue
     * drops it). Any other state is left as it is: that book is already moving again. Runs in
     * transactions of {@link #SEED_BATCH} books, like seed.
     *
     * @return how many books were restored or moved forward
     * @throws IllegalArgumentException if the state is not one of the three, or an id is not a
     *                                  valid book id (nothing is restored)
     */
    public int restore(Collection<Integer> ids, BookState state) {
        if (state != BookState.DOWNLOADED && state != BookState.INDEXED && state != BookState.PUBLISHED) {
            throw new IllegalArgumentException("only DOWNLOADED, INDEXED or PUBLISHED can be restored, not " + state);
        }
        List<Integer> distinct = new ArrayList<>(new LinkedHashSet<>(ids));
        for (int id : distinct) {
            if (!BookIds.isValid(id)) {
                throw new IllegalArgumentException("invalid book id: " + id);
            }
        }
        int restored = 0;
        for (int from = 0; from < distinct.size(); from += SEED_BATCH) {
            List<Integer> batch = distinct.subList(from, Math.min(from + SEED_BATCH, distinct.size()));
            long now = now();
            restored += inTransaction(tx -> {
                TransactionalMap<Integer, BookTask> map = tx.getMap(CoordinationNames.BOOK_STATE);
                TransactionalQueue<Integer> queue = tx.getQueue(CoordinationNames.INDEX_QUEUE);
                int n = 0;
                for (int id : batch) {
                    BookTask next = BookTask.restored(id, state, now);
                    BookTask existing = map.putIfAbsent(id, next);
                    if (existing != null && (existing.bookState() != BookState.PENDING
                            || !map.replace(id, existing, next))) {
                        continue;
                    }
                    if (state == BookState.DOWNLOADED) {
                        queue.offer(id);
                    }
                    n++;
                }
                return n;
            });
        }
        return restored;
    }

    /**
     * The expired-lease check of the Control (contract §4.2), every LEASE_CHECK_SECONDS:
     * books INGESTING or INDEXING whose lease has expired go back to their queue (T4, T8) or,
     * with no attempts left, to FAILED (T5, T9), with lastError LEASE_EXPIRED.
     *
     * @return how many books were requeued or failed
     */
    public int requeueExpiredLeases() {
        Predicate<Integer, BookTask> expired = Predicates.and(
                Predicates.in("state", BookState.INGESTING.name(), BookState.INDEXING.name()),
                Predicates.lessThan("leaseUntil", now()));
        int changed = 0;
        for (BookTask task : books.values(expired)) {
            String message = "lease of " + task.owner() + " expired at " + Instant.ofEpochMilli(task.leaseUntil());
            if (fail(Stage.ofWorking(task.bookState()), task, ErrorCode.LEASE_EXPIRED, message) != FailOutcome.LEASE_LOST) {
                changed++;
            }
        }
        return changed;
    }

    /**
     * T11, POST /control/retry-failed: every FAILED book goes back to the start of the stage
     * that failed, with its attempts reset (lastError is kept).
     *
     * @return how many books were requeued
     */
    public int retryFailed() {
        int requeued = 0;
        for (BookTask task : books.values(Predicates.equal("state", BookState.FAILED.name()))) {
            Stage stage = Stage.valueOf(task.failedStage());
            if (replaceAndOffer(task, task.retried(now()), stage.queue())) {
                requeued++;
            }
        }
        return requeued;
    }

    /** Books per state, every state present (0 if none): GET /control/status. */
    public Map<BookState, Long> countByState() {
        Map<BookState, Long> counts = new EnumMap<>(BookState.class);
        for (BookState state : BookState.values()) {
            counts.put(state, count(state));
        }
        return counts;
    }

    /**
     * Books in one state: one aggregation on the members, served by the index on state. The
     * Control asks for INDEXED every second to decide when to publish (CF-09).
     */
    public long count(BookState state) {
        return books.aggregate(Aggregators.count(), Predicates.equal("state", state.name()));
    }

    /**
     * Ids of the books in one state, e.g. INDEXED for the repair of an interrupted T10
     * (contract §7): Indexing compares them with index_books and calls markPublished.
     */
    public Set<Integer> idsIn(BookState state) {
        return books.keySet(Predicates.equal("state", state.name()));
    }

    /** Ids waiting in the queue of the stage. */
    public int queueSize(Stage stage) {
        return hz.getQueue(stage.queue()).size();
    }

    /** The state of one book: GET /control/tasks/{id}. */
    public Optional<BookTask> task(int bookId) {
        return Optional.ofNullable(books.get(bookId));
    }

    /** False when the cluster has lost its state and the Control must recover it (contract §7). */
    public boolean isInitialized() {
        return hz.getMap(CoordinationNames.COORDINATION_META).containsKey(CoordinationNames.INITIALIZED);
    }

    /** Written by the Control after a full seed or recovery. */
    public void markInitialized() {
        hz.<String, Long>getMap(CoordinationNames.COORDINATION_META).set(CoordinationNames.INITIALIZED, now());
    }

    // --- Rebuild of the index (DR-10) ----------------------------------------------------

    /**
     * The marker of a rebuild (contract §5): while it exists, no index writer claims from
     * index-queue, and POST /index/update and /index/publish answer 409 in every replica. Only
     * one rebuild at a time: putIfAbsent. It expires after {@code ttl} unless the rebuild renews
     * it, so a replica that dies in the middle of a rebuild does not stop indexing for ever.
     *
     * @param ttl at least 1 s; Hazelcast drops the fraction of a second
     * @return the token of this rebuild (the epoch ms it started), to renew and end it; empty if
     *         another rebuild holds the marker
     */
    public OptionalLong tryStartRebuild(Duration ttl) {
        requireWholeSeconds(ttl);
        long token = now();
        Long other = meta().putIfAbsent(CoordinationNames.REBUILD, token, ttl.toMillis(), TimeUnit.MILLISECONDS);
        return other == null ? OptionalLong.of(token) : OptionalLong.empty();
    }

    /**
     * Gives the marker of this rebuild another {@code ttl}.
     *
     * @return false if it is no longer this rebuild's (it expired, maybe another one started):
     *         the rebuild must stop, because the writers may be writing again
     */
    public boolean renewRebuild(long token, Duration ttl) {
        requireWholeSeconds(ttl);
        return isOurs(token) && meta().setTtl(CoordinationNames.REBUILD, ttl.toMillis(), TimeUnit.MILLISECONDS)
                && isOurs(token);
    }

    /** Removes the marker if it is still this rebuild's: never the one of a rebuild started after it expired. */
    public void endRebuild(long token) {
        meta().remove(CoordinationNames.REBUILD, token);
    }

    /** Whether a rebuild runs now, in any replica. */
    public boolean isRebuilding() {
        return meta().containsKey(CoordinationNames.REBUILD);
    }

    /** Hazelcast keeps a TTL in whole seconds and drops the rest: under 1 s the entry is born expired. */
    private static void requireWholeSeconds(Duration ttl) {
        if (ttl.compareTo(Duration.ofSeconds(1)) < 0) {
            throw new IllegalArgumentException("El TTL de la marca de reconstrucción debe ser de al menos 1 s: " + ttl);
        }
    }

    private boolean isOurs(long token) {
        return Long.valueOf(token).equals(meta().get(CoordinationNames.REBUILD));
    }

    private IMap<String, Long> meta() {
        return hz.getMap(CoordinationNames.COORDINATION_META);
    }

    // --- Workers (Ingestion, Indexing) ---------------------------------------------------

    /**
     * T2, T6: takes the next id of the stage's queue and claims it for {@code owner} with a
     * lease of LEASE_SECONDS. The poll and the claim are one transaction. An id whose entry
     * is not in the stage's waiting state (a duplicate, or a book someone else moved) is
     * dropped from the queue and the next one is tried. Both kinds of drop are counted:
     * {@link #invalidIdsDropped()} and {@link #idsSkipped()}.
     *
     * @param owner a worker name, see {@link #owner(String, int)}
     * @param wait  how long to wait for an id when the queue is empty
     * @return empty if no id could be claimed within {@code wait}
     */
    public Optional<Claim> claim(Stage stage, String owner, Duration wait) throws InterruptedException {
        Objects.requireNonNull(owner, "owner");
        long deadline = System.nanoTime() + wait.toNanos();
        while (true) {
            // Before opening a transaction; pollAndClaim sets the flag again if its poll is interrupted
            if (Thread.interrupted()) {
                throw new InterruptedException("interrupted while waiting for " + stage.queue());
            }
            long slice = Math.min(Math.max(deadline - System.nanoTime(), 0), MAX_POLL.toNanos());
            Polled polled = inTransaction(tx -> pollAndClaim(tx, stage, owner, slice));
            // Counted only here, after the commit: a rolled-back drop did not happen
            if (polled.drop() == Drop.INVALID) {
                invalidIdsDropped.incrementAndGet();
            } else if (polled.drop() == Drop.NOT_WAITING) {
                idsSkipped.incrementAndGet();
            }
            boolean timeIsUp = deadline - System.nanoTime() <= 0;
            if (polled.claim().isPresent() || (timeIsUp && !Thread.currentThread().isInterrupted())) {
                return polled.claim();
            }
        }
    }

    private Polled pollAndClaim(TransactionContext tx, Stage stage, String owner, long waitNanos) {
        Integer id;
        try {
            id = tx.<Integer>getQueue(stage.queue()).poll(waitNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();         // claim() sees it after the commit
            return Polled.NOTHING;
        }
        if (id == null) {
            return Polled.NOTHING;
        }
        if (!BookIds.isValid(id)) {
            return new Polled(Optional.empty(), Drop.INVALID);   // leaves the queue with the commit
        }
        TransactionalMap<Integer, BookTask> map = tx.getMap(CoordinationNames.BOOK_STATE);
        BookTask current = map.getForUpdate(id);        // locked for this transaction
        if (current == null || current.bookState() != stage.waiting()) {
            return new Polled(Optional.empty(), Drop.NOT_WAITING);
        }
        long now = now();
        BookTask claimed = current.claimed(stage, owner, now + leaseMillis(), now);
        map.set(id, claimed);
        return new Polled(Optional.of(new Claim(stage, claimed)), Drop.NONE);
    }

    /**
     * Ids that were not valid book ids, taken from a queue and dropped by this instance since
     * it was created. Not 0 means a bug somewhere: someone offered a bad id to a queue. The
     * services show it in GET /status (contract §2).
     */
    public long invalidIdsDropped() {
        return invalidIdsDropped.get();
    }

    /**
     * Ids taken from a queue whose book was not waiting (a duplicate, an unknown book, or one
     * someone else moved), dropped by this instance since it was created. Expected and harmless
     * (contract principle 3); a coordination metric (CF-12).
     */
    public long idsSkipped() {
        return idsSkipped.get();
    }

    /**
     * T3, T7: the stage is complete. Call it only when the evidence is persistent (contract
     * principle 2): {@code <id>._READY} exists (INGESTION), or index_log has the book
     * (INDEXING). A completed ingestion offers the id to index-queue in the same transaction.
     *
     * @return false if the worker had lost the book (requeued after its lease expired): it
     *         must drop it, whatever it wrote is harmless because every write is idempotent
     */
    public boolean complete(Claim claim) {
        Stage stage = claim.stage();
        if (stage == Stage.INGESTION) {
            return replaceAndOffer(claim.task(), claim.task().completed(stage, now(), true),
                    Stage.INDEXING.queue());
        }
        return books.replace(claim.bookId(), claim.task(), claim.task().completed(stage, now(), false));
    }

    /**
     * T4, T5, T8, T9: the attempt failed. PERMANENT codes and the last attempt send the book to
     * FAILED; other codes requeue it with one more attempt; OUTAGE requeues it without
     * counting (contract §4.3).
     *
     * <p>After an OUTAGE the caller must WAIT before its next {@link #claim}, with a growing
     * delay while the outage lasts (contract §4.3). The book is back in the queue at once and
     * the attempt is not counted, so a worker that claims again immediately spins in a hot
     * loop of claim, fail, claim for as long as MongoDB is down.
     */
    public FailOutcome fail(Claim claim, ErrorCode code, String message) {
        return fail(claim.stage(), claim.task(), code, message);
    }

    /** A longer lease for the same claim; empty if the book was lost meanwhile. */
    public Optional<Claim> renew(Claim claim) {
        BookTask renewed = claim.task().renewed(now() + leaseMillis());
        return books.replace(claim.bookId(), claim.task(), renewed)
                ? Optional.of(new Claim(claim.stage(), renewed))
                : Optional.empty();
    }

    /**
     * {@code <service>-<hostname>/w<n>}, e.g. ingestion-3f2a1c9e/w2. Inside Docker the
     * hostname is the container id, so the owner says which replica and which worker.
     */
    public static String owner(String service, int worker) {
        return service + "-" + hostname() + "/w" + worker;
    }

    // --- Index publication (Indexing, Search) --------------------------------------------

    /**
     * T10: the books that were INDEXED become PUBLISHED. Call it after CURRENT points to the
     * new version. Ids that are not INDEXED in book-state (unknown, or indexed through
     * POST /index/update) are left alone.
     *
     * @return how many books were marked
     */
    public int markPublished(Collection<Integer> ids) {
        List<Integer> all = new ArrayList<>(new HashSet<>(ids));
        int marked = 0;
        for (int from = 0; from < all.size(); from += PUBLISH_BATCH) {
            List<Integer> batch = all.subList(from, Math.min(from + PUBLISH_BATCH, all.size()));
            for (BookTask task : books.getAll(new HashSet<>(batch)).values()) {
                if (task.bookState() == BookState.INDEXED && books.replace(task.bookId(), task, task.published())) {
                    marked++;
                }
            }
        }
        return marked;
    }

    /** Announces a new version on index-events (contract §6). */
    public void publish(IndexPublished event) {
        hz.<IndexPublished>getReliableTopic(CoordinationNames.INDEX_EVENTS).publish(event);
    }

    /**
     * Calls {@code listener} for every IndexPublished from now on. A listener that throws keeps
     * receiving, and one that falls behind skips the lost messages instead of stopping: the
     * event is only a wake-up (Search also checks CURRENT periodically).
     *
     * @return the id for {@link #unsubscribe(UUID)}
     */
    public UUID subscribe(Consumer<IndexPublished> listener) {
        return hz.<IndexPublished>getReliableTopic(CoordinationNames.INDEX_EVENTS)
                .addMessageListener(new WakeUpListener(Objects.requireNonNull(listener, "listener")));
    }

    public boolean unsubscribe(UUID registration) {
        return hz.getReliableTopic(CoordinationNames.INDEX_EVENTS).removeMessageListener(registration);
    }

    /** The lock of the single index writer (contract §5). */
    public IndexWriterLock indexWriterLock() {
        return new IndexWriterLock(hz.getMap(CoordinationNames.LOCKS));
    }

    /**
     * Calls {@code listener} with false when the client loses the cluster and with true when it
     * is connected again. The index writer stops writing on false (contract §5).
     */
    public UUID onConnectionChange(Consumer<Boolean> listener) {
        Objects.requireNonNull(listener, "listener");
        return hz.getLifecycleService().addLifecycleListener(event -> {
            if (event.getState() == LifecycleState.CLIENT_CONNECTED) {
                listener.accept(true);
            } else if (event.getState() == LifecycleState.CLIENT_DISCONNECTED) {
                listener.accept(false);
            }
        });
    }

    /** Shuts the client down if this instance opened it ({@link #connect}). */
    @Override
    public void close() {
        if (ownsClient) {
            hz.shutdown();
        }
    }

    // --- Internals -----------------------------------------------------------------------

    private FailOutcome fail(Stage stage, BookTask expected, ErrorCode code, String message) {
        String error = BookTask.error(code, message);
        int attempts = code.kind() == ErrorCode.Kind.OUTAGE ? expected.attempts() : expected.attempts() + 1;
        if (code.kind() == ErrorCode.Kind.PERMANENT || attempts >= settings.maxAttempts()) {
            return books.replace(expected.bookId(), expected, expected.failed(stage, attempts, error))
                    ? FailOutcome.FAILED
                    : FailOutcome.LEASE_LOST;
        }
        return replaceAndOffer(expected, expected.requeued(stage, attempts, error, now()), stage.queue())
                ? FailOutcome.REQUEUED
                : FailOutcome.LEASE_LOST;
    }

    /** replace(expected, next) and, only if it succeeded, offer the id: one transaction. */
    private boolean replaceAndOffer(BookTask expected, BookTask next, String queue) {
        return inTransaction(tx -> {
            TransactionalMap<Integer, BookTask> map = tx.getMap(CoordinationNames.BOOK_STATE);
            if (!map.replace(next.bookId(), expected, next)) {
                return false;
            }
            tx.<Integer>getQueue(queue).offer(next.bookId());
            return true;
        });
    }

    /** Runs {@code work} in a transaction: committed if it returns, rolled back if it throws. */
    private <T> T inTransaction(Function<TransactionContext, T> work) {
        TransactionContext tx = hz.newTransactionContext(TRANSACTIONS);
        tx.beginTransaction();
        T result;
        try {
            result = work.apply(tx);
        } catch (RuntimeException | Error e) {
            tx.rollbackTransaction();
            throw e;
        }
        try {
            tx.commitTransaction();
        } catch (RuntimeException e) {
            try {
                tx.rollbackTransaction();             // what Hazelcast recommends after a failed commit
            } catch (RuntimeException rollbackFailure) {
                e.addSuppressed(rollbackFailure);
            }
            throw e;
        }
        return result;
    }

    private long now() {
        return clock.millis();
    }

    private long leaseMillis() {
        return TimeUnit.SECONDS.toMillis(settings.leaseSeconds());
    }

    private static String hostname() {
        String fromEnv = System.getenv("HOSTNAME");         // set by Docker: the container id
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv.strip();
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (IOException e) {
            return "localhost";
        }
    }

    /** Adapts a plain listener to the reliable topic with the behaviour of {@link #subscribe}. */
    private record WakeUpListener(Consumer<IndexPublished> target) implements ReliableMessageListener<IndexPublished> {

        @Override
        public void onMessage(Message<IndexPublished> message) {
            target.accept(message.getMessageObject());
        }

        /** -1: start with the next message published, not with old ones. */
        @Override
        public long retrieveInitialSequence() {
            return -1;
        }

        @Override
        public void storeSequence(long sequence) {
        }

        /** A reader that fell behind the ringbuffer skips to the oldest message left. */
        @Override
        public boolean isLossTolerant() {
            return true;
        }

        /** An exception thrown by the listener does not stop the subscription. */
        @Override
        public boolean isTerminal(Throwable failure) {
            return false;
        }
    }
}
