package es.ulpgc.bigdata.indexing;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.mongodb.MongoException;
import es.ulpgc.bigdata.coordination.Claim;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.ErrorCode;
import es.ulpgc.bigdata.coordination.FailOutcome;
import es.ulpgc.bigdata.coordination.IndexWriterLock;
import es.ulpgc.bigdata.coordination.Stage;
import es.ulpgc.bigdata.datamart.metadata.MetadataRepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The consumer of index-queue (DR-06b, coordination contract §4.1 T6-T9 and §5): the single
 * index writer of the system. One thread per replica runs it:
 * <ol>
 *   <li>it takes the lock index-writer. Another replica waits for it as a standby, and takes
 *       over when the holder stops or dies (Hazelcast releases the lock of a dead client);</li>
 *   <li>while it holds it: claim (T6), {@link IndexingService#update}, the same work as
 *       POST /index/update, and complete (T7). update writes index_log last, so T7 only
 *       happens when the book is complete in the index (contract principle 2).</li>
 * </ol>
 *
 * Each failure goes back to the contract with its code (§4.3):
 * <pre>
 *   BookNotFoundException (not complete)    NOT_READY            requeued, one attempt more
 *   EmptyBookException                      EMPTY_BOOK           FAILED at once
 *   MongoException, MetadataRepository...   STORAGE_UNAVAILABLE  requeued WITHOUT counting, and the
 *                                                                writer waits 1, 2, 4... 30 s before
 *                                                                its next claim (back to 1 s after a
 *                                                                book goes through)
 *   anything else                           INTERNAL_ERROR       requeued, one attempt more
 * </pre>
 * If complete or fail find the book changed, the writer lost it (the Control requeued it when
 * its lease expired): it is dropped, and what was written is harmless because indexing is
 * idempotent. No renewal of the lease: one book takes milliseconds to seconds, the lease 120 s.
 *
 * The lock belongs to the THREAD that took it (IMap.lock), so the same thread takes and
 * releases it, and it is taken with tryLock in steps so that stop() is noticed while waiting.
 * When the client loses the cluster the writer stops claiming at once, and calls nothing in
 * Hazelcast until it is back: an unlock without a cluster would wait for it (up to the
 * invocation timeout, 120 s) and a stop would have to interrupt it. So the release waits for the
 * connection, and then comes before the next tryLock: IMap.lock is reentrant, and a lock taken
 * twice and released once would stay held. If the service stops meanwhile, the member releases
 * the locks of the client that left. Back with the cluster, the writer takes the lock again
 * before writing anything (contract §5).
 *
 * Also when the cluster is lost AND back while the writer is busy with a book (review of #37):
 * meanwhile the member may have released the lock of this client and another replica may hold
 * it, so connected being true again proves nothing. Each disconnection is counted, and a hold
 * of the lock ends as soon as the count is not the one read before its tryLock. The book in
 * hand is finished (its lease is still ours, and indexing is idempotent), then release() finds
 * out whether the lock was still ours, and the writer competes for it again as a standby.
 *
 * If a Hazelcast call fails, the writer waits 1, 2, 4... 30 s and tries again (the client
 * reconnects by itself); the wait starts again at 1 s as soon as the cluster answers a tryLock.
 *
 * DR-09 puts the parsing threads and the batches in front of this same loop.
 */
public final class IndexingWriter implements Runnable {

    /**
     * How long one claim waits for an id, and one attempt to take the lock: how soon a stop is
     * noticed, idle or as a standby.
     */
    static final Duration CLAIM_WAIT = Duration.ofSeconds(2);
    static final Duration MIN_BACKOFF = Duration.ofSeconds(1);
    static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

    /** How the writer waits: Thread.sleep in production, recorded in the tests. */
    @FunctionalInterface
    public interface Sleeper {
        void sleep(Duration duration) throws InterruptedException;
    }

    /** What the writer is doing, for GET /status. */
    public enum Role { STANDBY, ACTIVE, STOPPED }

    private static final Logger LOG = LoggerFactory.getLogger(IndexingWriter.class);

    private final Coordination coordination;
    private final IndexingService service;
    private final IndexWriterLock lock;
    private final String owner;
    private final Sleeper sleeper;
    private final Duration step;

    private volatile boolean stopping = false;
    private volatile boolean connected = true;
    /** Disconnections seen so far: a hold of the lock is valid only while this does not change. */
    private final AtomicLong disconnections = new AtomicLong();
    private volatile Role role = Role.STANDBY;
    private Duration outageDelay = Duration.ZERO;           // only the writer thread touches it
    private boolean holdsLock = false;                       // taken and not yet released; writer thread only

    private final AtomicLong indexed = new AtomicLong();
    private final AtomicLong requeued = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong leaseLost = new AtomicLong();
    private final AtomicLong outagePauses = new AtomicLong();

    public IndexingWriter(Coordination coordination, IndexingService service, String owner, Sleeper sleeper) {
        this(coordination, service, owner, sleeper, CLAIM_WAIT);
    }

    /** @param step how long one claim or one attempt to lock waits (CLAIM_WAIT; shorter in the tests) */
    IndexingWriter(Coordination coordination, IndexingService service, String owner, Sleeper sleeper, Duration step) {
        this.coordination = Objects.requireNonNull(coordination, "coordination");
        this.service = Objects.requireNonNull(service, "service");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
        this.step = Objects.requireNonNull(step, "step");
        this.lock = coordination.indexWriterLock();
    }

    /** Until stop(): wait for the lock, then index books while holding it. */
    @Override
    public void run() {
        Duration backoff = MIN_BACKOFF;
        try {
            while (!stopping) {
                try {
                    if (!connected) {
                        sleeper.sleep(step);               // the client reconnects by itself
                        continue;
                    }
                    if (holdsLock) {
                        release();                          // pending since the cluster was lost
                    }
                    long generation = disconnections.get();  // read BEFORE taking the lock
                    boolean holding = lock.tryLock(step);
                    backoff = MIN_BACKOFF;                  // the cluster answered: the next failure waits 1 s
                    if (holding) {
                        holdsLock = true;
                        try {
                            writeWhileHolding(generation);
                        } finally {
                            role = Role.STANDBY;
                        }
                        // Not after an exception: a claim fails before the client reports the
                        // disconnection, and an unlock then would wait for the cluster.
                        if (connected) {
                            release();
                        }
                    }
                } catch (InterruptedException e) {
                    return;                                 // stop() after the grace, or the JVM stops
                } catch (HazelcastClientNotActiveException | HazelcastInstanceNotActiveException e) {
                    return;                                 // the client was shut down
                } catch (RuntimeException e) {
                    LOG.warn("{}: coordination failed ({}), retrying in {} s", owner, e.toString(), backoff.toSeconds());
                    try {
                        sleeper.sleep(backoff);
                    } catch (InterruptedException interrupted) {
                        return;
                    }
                    backoff = doubled(backoff);
                }
            }
        } finally {
            role = Role.STOPPED;
        }
    }

    /** Asks the writer to stop after its current book; run() returns within CLAIM_WAIT when idle. */
    public void stop() {
        stopping = true;
    }

    /**
     * Called by Hazelcast when the client loses the cluster (false) or gets it back (true): on
     * false the writer stops claiming and releases the lock (contract §5), and the current hold
     * of the lock ends even if the cluster is back before the writer looks.
     */
    public void connectionChanged(boolean up) {
        if (!up) {
            disconnections.incrementAndGet();           // before connected: never seen up and unchanged
        }
        connected = up;
    }

    public Role role() {
        return role;
    }

    /** For GET /status: what the writer did and the ids the coordination dropped. */
    public Map<String, Object> status() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("role", role.name().toLowerCase(Locale.ROOT));
        status.put("indexed", indexed.get());
        status.put("requeued", requeued.get());
        status.put("failed", failed.get());
        status.put("lease_lost", leaseLost.get());
        status.put("outage_pauses", outagePauses.get());
        status.put("invalid_ids_dropped", coordination.invalidIdsDropped());
        status.put("ids_skipped", coordination.idsSkipped());
        return status;
    }

    /**
     * Claims and indexes while this hold of the lock is valid: not stopping, connected, and no
     * disconnection since the tryLock that took it (generation).
     */
    private void writeWhileHolding(long generation) throws InterruptedException {
        role = Role.ACTIVE;
        LOG.info("{}: holds index-writer, consuming {}", owner, Stage.INDEXING.queue());
        while (!stopping && connected && disconnections.get() == generation) {
            Optional<Claim> claim = coordination.claim(Stage.INDEXING, owner, step);
            if (claim.isPresent()) {
                process(claim.get());
            }
        }
        if (!stopping && connected) {
            LOG.warn("{}: the cluster was lost while holding index-writer; taking it again before "
                    + "the next book", owner);
        }
    }

    /**
     * One hold of the lock, released by the thread that took it. If the lock is gone (the
     * cluster restarted, or it forgot this client) Hazelcast says this thread is not the owner:
     * nothing to release. If the call fails, holdsLock stays true and it is tried again.
     */
    private void release() {
        try {
            lock.unlock();
        } catch (IllegalMonitorStateException e) {
            LOG.info("{}: index-writer was no longer ours", owner);
        }
        holdsLock = false;
    }

    private void process(Claim claim) throws InterruptedException {
        int bookId = claim.bookId();
        try {
            service.update(bookId);
        } catch (BookNotFoundException e) {
            failed(claim, ErrorCode.NOT_READY, "book " + bookId + " is not complete in the datalake");
            return;
        } catch (EmptyBookException e) {
            failed(claim, ErrorCode.EMPTY_BOOK, "book " + bookId + " has no indexable terms");
            return;
        } catch (MongoException | MetadataRepositoryException e) {
            failed(claim, ErrorCode.STORAGE_UNAVAILABLE, e.getMessage());
            pauseForOutage();
            return;
        } catch (RuntimeException e) {
            LOG.error("{}: unexpected error indexing book {}", owner, bookId, e);
            failed(claim, ErrorCode.INTERNAL_ERROR, e.toString());
            return;
        }
        outageDelay = Duration.ZERO;
        if (coordination.complete(claim)) {
            indexed.incrementAndGet();
        } else {
            lost(bookId, "complete");
        }
    }

    /** OUTAGE requeues the book at once: without this wait the writer would spin on a down MongoDB. */
    private void pauseForOutage() throws InterruptedException {
        outageDelay = outageDelay.isZero() ? MIN_BACKOFF : doubled(outageDelay);
        outagePauses.incrementAndGet();
        LOG.warn("{}: MongoDB does not answer, next claim in {} s", owner, outageDelay.toSeconds());
        sleeper.sleep(outageDelay);
    }

    private void failed(Claim claim, ErrorCode code, String message) {
        FailOutcome outcome = coordination.fail(claim, code, message);
        switch (outcome) {
            case REQUEUED -> requeued.incrementAndGet();
            case FAILED -> {
                failed.incrementAndGet();
                LOG.warn("{}: book {} FAILED: {}: {}", owner, claim.bookId(), code, message);
            }
            case LEASE_LOST -> lost(claim.bookId(), "fail");
        }
    }

    /** The Control requeued the book while we worked on it: drop it, whatever we wrote is harmless. */
    private void lost(int bookId, String step) {
        leaseLost.incrementAndGet();
        LOG.warn("{}: lost the lease of book {} before {}; dropped", owner, bookId, step);
    }

    private static Duration doubled(Duration delay) {
        Duration twice = delay.multipliedBy(2);
        return twice.compareTo(MAX_BACKOFF) > 0 ? MAX_BACKOFF : twice;
    }
}
