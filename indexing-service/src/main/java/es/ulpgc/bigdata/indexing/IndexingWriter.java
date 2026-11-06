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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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
 *   <li>while it holds it, in batches (DR-09): it claims a book (T6), waiting for it, and then
 *       the ones already waiting in the queue, without waiting, up to batchSize
 *       (INDEX_WRITE_BATCH). {@link IndexingService#indexBatch} prepares them in parallel and
 *       writes them together, the same work as POST /index/update. Then complete (T7) for each
 *       indexed book. indexBatch writes index_log last, so T7 only happens when the book is
 *       complete in the index (contract principle 2).</li>
 * </ol>
 * With few books in the queue a batch has one book and nothing waits for more; with a backlog
 * the batches fill up by themselves, and one flush writes each distinct term once per batch.
 *
 * Each failure goes back to the contract with its code (§4.3). A book that cannot be prepared
 * fails alone; a write that fails (MongoDB) fails the whole batch, and every book of it is
 * requeued:
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
 * idempotent. No renewal of the lease: a batch of 100 books takes seconds, the lease 120 s. If
 * a claim fails in the middle of a batch (the cluster is lost), the books already claimed wait
 * for their lease to expire, as if the writer had died.
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
 * If a Hazelcast call fails, the writer waits 1, 2, 4... 30 s and tries again (the client
 * reconnects by itself); the wait starts again at 1 s as soon as the cluster answers a tryLock.
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
    private final int batchSize;

    private volatile boolean stopping = false;
    private volatile boolean connected = true;
    private volatile Role role = Role.STANDBY;
    private Duration outageDelay = Duration.ZERO;           // only the writer thread touches it
    private boolean holdsLock = false;                       // taken and not yet released; writer thread only

    private final AtomicLong indexed = new AtomicLong();
    private final AtomicLong requeued = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong leaseLost = new AtomicLong();
    private final AtomicLong outagePauses = new AtomicLong();
    private volatile int lastBatch = 0;

    /** @param batchSize the most books written together (INDEX_WRITE_BATCH), at least 1 */
    public IndexingWriter(Coordination coordination, IndexingService service, String owner, Sleeper sleeper,
                          int batchSize) {
        this(coordination, service, owner, sleeper, batchSize, CLAIM_WAIT);
    }

    /** @param step how long one claim or one attempt to lock waits (CLAIM_WAIT; shorter in the tests) */
    IndexingWriter(Coordination coordination, IndexingService service, String owner, Sleeper sleeper,
                   int batchSize, Duration step) {
        this.coordination = Objects.requireNonNull(coordination, "coordination");
        this.service = Objects.requireNonNull(service, "service");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
        this.step = Objects.requireNonNull(step, "step");
        if (batchSize < 1) {
            throw new IllegalArgumentException("el lote debe tener al menos un libro: " + batchSize);
        }
        this.batchSize = batchSize;
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
                    boolean holding = lock.tryLock(step);
                    backoff = MIN_BACKOFF;                  // the cluster answered: the next failure waits 1 s
                    if (holding) {
                        holdsLock = true;
                        try {
                            writeWhileHolding();
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

    /** Asks the writer to stop after its current batch; run() returns within CLAIM_WAIT when idle. */
    public void stop() {
        stopping = true;
    }

    /**
     * Called by Hazelcast when the client loses the cluster (false) or gets it back (true): on
     * false the writer stops claiming and releases the lock (contract §5).
     */
    public void connectionChanged(boolean up) {
        connected = up;
    }

    public Role role() {
        return role;
    }

    /** For GET /status: what the writer did and the ids the coordination dropped. */
    public Map<String, Object> status() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("role", role.name().toLowerCase());
        status.put("indexed", indexed.get());
        status.put("requeued", requeued.get());
        status.put("failed", failed.get());
        status.put("lease_lost", leaseLost.get());
        status.put("outage_pauses", outagePauses.get());
        status.put("last_batch", lastBatch);
        status.put("invalid_ids_dropped", coordination.invalidIdsDropped());
        status.put("ids_skipped", coordination.idsSkipped());
        return status;
    }

    private void writeWhileHolding() throws InterruptedException {
        role = Role.ACTIVE;
        LOG.info("{}: holds index-writer, consuming {}", owner, Stage.INDEXING.queue());
        while (!stopping && connected) {
            Optional<Claim> first = coordination.claim(Stage.INDEXING, owner, step);
            if (first.isPresent()) {
                process(batchFrom(first.get()));
            }
        }
    }

    /** The first book and the ones already in the queue, without waiting, up to batchSize. */
    private List<Claim> batchFrom(Claim first) throws InterruptedException {
        List<Claim> batch = new ArrayList<>(batchSize);
        batch.add(first);
        while (batch.size() < batchSize && !stopping && connected) {
            Optional<Claim> next = coordination.claim(Stage.INDEXING, owner, Duration.ZERO);
            if (next.isEmpty()) {
                break;                                      // the queue is empty: write what there is
            }
            batch.add(next.get());
        }
        return batch;
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

    private void process(List<Claim> batch) throws InterruptedException {
        lastBatch = batch.size();
        Map<Integer, Claim> claims = new LinkedHashMap<>();
        batch.forEach(claim -> claims.put(claim.bookId(), claim));
        BatchResult result;
        try {
            result = service.indexBatch(List.copyOf(claims.keySet()));
        } catch (MongoException | MetadataRepositoryException e) {
            claims.values().forEach(claim -> failed(claim, ErrorCode.STORAGE_UNAVAILABLE, e.getMessage()));
            pauseForOutage();
            return;
        } catch (RuntimeException e) {
            LOG.error("{}: unexpected error writing a batch of {} books", owner, claims.size(), e);
            claims.values().forEach(claim -> failed(claim, ErrorCode.INTERNAL_ERROR, e.toString()));
            return;
        }
        if (!result.indexed().isEmpty()) {
            outageDelay = Duration.ZERO;                    // MongoDB took the batch
        }
        for (UpdateResult book : result.indexed()) {
            if (coordination.complete(claims.get(book.bookId()))) {
                indexed.incrementAndGet();
            } else {
                lost(book.bookId(), "complete");
            }
        }
        result.rejected().forEach((bookId, why) -> rejected(claims.get(bookId), why));
    }

    /** A book of the batch that could not be prepared: its code, as update's exception says. */
    private void rejected(Claim claim, RuntimeException why) {
        int bookId = claim.bookId();
        if (why instanceof BookNotFoundException) {
            failed(claim, ErrorCode.NOT_READY, "book " + bookId + " is not complete in the datalake");
        } else if (why instanceof EmptyBookException) {
            failed(claim, ErrorCode.EMPTY_BOOK, "book " + bookId + " has no indexable terms");
        } else {
            LOG.error("{}: unexpected error indexing book {}", owner, bookId, why);
            failed(claim, ErrorCode.INTERNAL_ERROR, why.toString());
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
