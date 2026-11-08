package es.ulpgc.bigdata.indexing;

import es.ulpgc.bigdata.coordination.BookState;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.IndexPublished;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The publications told through Hazelcast (DR-08b, coordination contract §4.1 T10 and §6):
 * <ul>
 *   <li>IndexPublished on index-events, one per new version, in the order of the versions;</li>
 *   <li>T10: the books INDEXED in book-state that are visible in CURRENT become PUBLISHED.</li>
 * </ul>
 *
 * In a thread of its own (review of #32). Publisher calls afterPublish inside its lock, and a
 * Hazelcast that answers slowly may take up to the invocation timeout (120 s) per call: that
 * must never hold the lock nor the HTTP request. So afterPublish only queues, and this thread
 * does the work in the order it was queued. Each piece in its own try: the event of a version
 * never depends on T10, which needs MongoDB too.
 *
 * T10 is not "the books of this version", but a sync: of the INDEXED books, those that
 * index_books has with a version &lt;= the CURRENT of now. So one sync covers every
 * publication before it, and the pending ones are merged: while one sync is waiting in the
 * queue, more publications do not add another. The flag is cleared just before reading
 * CURRENT, so a publication after that read always gets a sync of its own. The same step
 * repairs a T10 that was interrupted (the process died after the CAS, or Hazelcast was down):
 * contract §7. A book is never marked before CURRENT contains it (contract principle 2): the
 * version comes from CURRENT itself.
 *
 * Without a cluster (still connecting, or the client lost it) nothing calls Hazelcast: a
 * version created meanwhile is counted in skipped(), and the last one is kept. When the client
 * connects or reconnects, that version is announced and the states are synced. A lost event
 * does no harm anyway: Search also checks CURRENT periodically (contract §6).
 *
 * close() lets the thread finish what is queued for CLOSE_GRACE, then interrupts it: what is
 * left is repaired by the next start (the sync on connecting) and by Search's check of CURRENT.
 */
public final class HazelcastPublicationNotifier implements PublicationNotifier, AutoCloseable {

    /** How long close() lets the queued announcements finish. */
    static final Duration CLOSE_GRACE = Duration.ofSeconds(2);

    private static final Logger LOG = LoggerFactory.getLogger(HazelcastPublicationNotifier.class);

    private final PublicationRepository publications;
    private final AtomicReference<Coordination> coordination = new AtomicReference<>();
    private volatile boolean up = false;
    private final AtomicLong skipped = new AtomicLong();
    /** The newest version that could not be announced; announced on the next connection. */
    private final AtomicReference<PublishResult> unannounced = new AtomicReference<>();
    private final AtomicBoolean syncQueued = new AtomicBoolean(false);
    private final ExecutorService worker;

    public HazelcastPublicationNotifier(PublicationRepository publications) {
        this.publications = Objects.requireNonNull(publications, "publications");
        this.worker = Executors.newSingleThreadExecutor(task -> {
            Thread thread = new Thread(task, App.SERVICE_NAME + "-notifier");
            thread.setDaemon(true);                     // never keeps the JVM alive
            return thread;
        });
    }

    /**
     * The client is connected: from now on the publications are announced. Queues the version
     * that was skipped meanwhile, if any, and a sync of the states (contract §7). Called once,
     * by the thread that connected; the reconnections arrive through the connection listener.
     *
     * The listener is registered before marking the connection up. A disconnection in the
     * microseconds between both is only seen at the next event: the worst case is one call in
     * this notifier's thread that waits for the cluster, never the publication.
     */
    public void connected(Coordination connected) {
        Objects.requireNonNull(connected, "connected");
        if (!coordination.compareAndSet(null, connected)) {
            throw new IllegalStateException("ya conectado a Hazelcast");
        }
        connected.onConnectionChange(this::connectionChanged);
        connectionChanged(true);
    }

    /**
     * Called by Hazelcast, in its event thread, when the client loses the cluster (false) or
     * gets it back (true). Never calls Hazelcast here: it only queues.
     */
    void connectionChanged(boolean connectedNow) {
        up = connectedNow;
        if (connectedNow) {
            PublishResult pending = unannounced.getAndSet(null);
            if (pending != null) {
                submit(() -> announce(pending));
            }
            queueSync();
        }
    }

    /**
     * What this notifier believes of the connection: connecting, connected or disconnected. For
     * the tests; GET /status shows the state of BackgroundConnection.
     */
    String state() {
        if (coordination.get() == null) {
            return "connecting";
        }
        return up ? "connected" : "disconnected";
    }

    /** New versions whose announcement was skipped because Hazelcast was not there. */
    public long skipped() {
        return skipped.get();
    }

    /** Only queues: returns at once even if Hazelcast is slow. */
    @Override
    public void afterPublish(PublishResult result) {
        Objects.requireNonNull(result, "result");
        if (result.booksPublished() > 0) {              // nothing new: no event to announce
            if (connectedNow().isEmpty()) {
                skip(result);
            } else {
                submit(() -> announce(result));
            }
        }
        if (result.version() != null) {
            queueSync();                                // without a cluster the sync does nothing
        }
    }

    /**
     * T10 for every INDEXED book visible in the CURRENT of now, in the calling thread.
     *
     * @return how many books became PUBLISHED; 0 if not connected or nothing is published
     */
    public int syncStates() {
        Optional<Coordination> current = connectedNow();
        if (current.isEmpty()) {
            return 0;
        }
        Optional<Publication> published = publications.current();
        return published.map(p -> markVisible(current.get(), p.version())).orElse(0);
    }

    /** Lets the queued work finish for CLOSE_GRACE, then interrupts it. Closing twice does nothing. */
    @Override
    public void close() {
        worker.shutdown();
        try {
            if (!worker.awaitTermination(CLOSE_GRACE.toMillis(), TimeUnit.MILLISECONDS)) {
                List<Runnable> left = worker.shutdownNow();
                LOG.warn("stopping with {} announcements still queued: Search's check of CURRENT "
                        + "and the next start cover them", left.size());
            }
        } catch (InterruptedException e) {
            worker.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /** For the tests: waits until everything queued before this call is done. */
    void awaitQueued(Duration timeout) throws InterruptedException, TimeoutException {
        try {
            worker.submit(() -> { }).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    private Optional<Coordination> connectedNow() {
        Coordination current = coordination.get();
        return current != null && up ? Optional.of(current) : Optional.empty();
    }

    /** At most one sync waiting in the queue: it covers every publication before it. */
    private void queueSync() {
        if (syncQueued.compareAndSet(false, true)) {
            submit(this::sync);
        }
    }

    private void submit(Runnable task) {
        try {
            worker.execute(task);
        } catch (RejectedExecutionException e) {
            LOG.debug("notifier closed, nothing more is queued: the service is stopping");
        }
    }

    // ---- the worker thread ----

    private void announce(PublishResult result) {
        Optional<Coordination> current = connectedNow();
        if (current.isEmpty()) {
            skip(result);                               // lost the cluster while it was queued
            return;
        }
        try {
            current.get().publish(new IndexPublished(result.version(), result.booksPublished(),
                    Math.toIntExact(result.totalBooks()), result.publishedAt()));
        } catch (RuntimeException e) {
            LOG.warn("could not announce version {}: announced again on the next connection",
                    result.version(), e);
            keepNewest(result);
        }
    }

    private void sync() {
        syncQueued.set(false);                          // before reading CURRENT: see the class
        try {
            syncStates();
        } catch (RuntimeException e) {
            LOG.warn("could not mark the published books (T10): the next publication or connection does", e);
        }
    }

    private void skip(PublishResult result) {
        skipped.incrementAndGet();
        keepNewest(result);
        LOG.warn("Hazelcast is not connected: version {} is not announced now; the last version "
                + "skipped is announced, and the states marked, when it connects", result.version());
        // The connection may have come back between the check and keepNewest, after
        // connectionChanged(true) took unannounced: then nobody else would announce it.
        // up and unannounced are volatile, so either that thread sees the version kept here or
        // this check sees up = true (review of #43).
        if (connectedNow().isPresent()) {
            PublishResult pending = unannounced.getAndSet(null);
            if (pending != null) {
                submit(() -> announce(pending));
            }
        }
    }

    private void keepNewest(PublishResult result) {
        unannounced.accumulateAndGet(result,
                (kept, offered) -> kept == null || offered.version() >= kept.version() ? offered : kept);
    }

    private int markVisible(Coordination current, int version) {
        Set<Integer> indexed = current.idsIn(BookState.INDEXED);
        if (indexed.isEmpty()) {
            return 0;
        }
        List<Integer> visible = publications.visibleAmong(indexed, version);
        int marked = visible.isEmpty() ? 0 : current.markPublished(visible);
        if (marked > 0) {
            LOG.info("{} books PUBLISHED in book-state (version {})", marked, version);
        }
        return marked;
    }
}
