package es.ulpgc.bigdata.ingestion.worker;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import es.ulpgc.bigdata.coordination.Claim;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.ErrorCode;
import es.ulpgc.bigdata.coordination.FailOutcome;
import es.ulpgc.bigdata.coordination.Stage;
import es.ulpgc.bigdata.ingestion.IngestionService;
import es.ulpgc.bigdata.ingestion.InvalidBookException;
import es.ulpgc.bigdata.ingestion.source.BookNotFoundException;
import es.ulpgc.bigdata.ingestion.source.SourceUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * One worker of ingest-queue (CF-08): claims a book, downloads and stores it with the same
 * IngestionService as POST /ingest, and completes T3 (DOWNLOADED, offered to index-queue). Every
 * failure goes back to the coordination contract with its code (§4.3):
 *
 * <pre>
 *   BookNotFoundException        NOT_FOUND         permanent: FAILED at once
 *   InvalidBookException         INVALID_BOOK      permanent: FAILED at once
 *   SourceUnavailableException   DOWNLOAD_FAILED   after its own retries: requeued, one attempt more
 *   anything else                INTERNAL_ERROR    requeued, one attempt more
 * </pre>
 *
 * A transient source failure is retried INGESTION_FETCH_RETRIES times inside the attempt, after
 * 1 s, 2 s, 4 s...; a throttling (429/403) is not retried: it pauses the whole replica
 * (Throttle). With the defaults (2 retries, 30 s request timeout) one attempt lasts at most
 * 3 x 30 s + 3 s, within the 120 s lease: the timeout bounds the whole download, body
 * included (GutenbergClient), and reading a local file is far faster.
 *
 * If the Coordination calls fail (the cluster is down), run() waits and tries again: the client
 * reconnects by itself (contract §7).
 */
public final class IngestionWorker implements Runnable {

    /** How long one claim waits for an id; also how soon a stop is noticed while idle. */
    static final Duration CLAIM_WAIT = Duration.ofSeconds(2);
    static final Duration RETRY_DELAY = Duration.ofSeconds(1);
    static final Duration MIN_BACKOFF = Duration.ofSeconds(1);
    static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

    /** How a worker waits: Thread.sleep in production, recorded in the tests. */
    @FunctionalInterface
    public interface Sleeper {
        void sleep(Duration duration) throws InterruptedException;
    }

    private static final Logger LOG = LoggerFactory.getLogger(IngestionWorker.class);

    private final Coordination coordination;
    private final IngestionService ingestion;
    private final Throttle throttle;
    private final WorkerStats stats;
    private final String owner;
    private final int fetchRetries;
    private final Sleeper sleeper;
    private volatile boolean stopping;

    public IngestionWorker(Coordination coordination, IngestionService ingestion, Throttle throttle,
                           WorkerStats stats, String owner, int fetchRetries, Sleeper sleeper) {
        this.coordination = Objects.requireNonNull(coordination, "coordination");
        this.ingestion = Objects.requireNonNull(ingestion, "ingestion");
        this.throttle = Objects.requireNonNull(throttle, "throttle");
        this.stats = Objects.requireNonNull(stats, "stats");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.fetchRetries = fetchRetries;
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    }

    /** Until stop(): claim and process books; wait and retry while the cluster does not answer. */
    @Override
    public void run() {
        Duration backoff = MIN_BACKOFF;
        while (!stopping) {
            try {
                runOnce();
                backoff = MIN_BACKOFF;
            } catch (InterruptedException e) {
                return;                                    // stop() or the shutdown of the JVM
            } catch (HazelcastClientNotActiveException | HazelcastInstanceNotActiveException e) {
                // The client was shut down (the service is stopping): retrying would never end.
                // A client of 5.x throws the first; the second, a member instance.
                return;
            } catch (RuntimeException e) {
                LOG.warn("{}: coordination failed ({}), retrying in {} s", owner, e.toString(), backoff.toSeconds());
                try {
                    sleeper.sleep(backoff);
                } catch (InterruptedException interrupted) {
                    return;
                }
                backoff = backoff.multipliedBy(2).compareTo(MAX_BACKOFF) > 0 ? MAX_BACKOFF : backoff.multipliedBy(2);
            }
        }
    }

    /** Asks the worker to stop after its current book; run() returns within CLAIM_WAIT when idle. */
    public void stop() {
        stopping = true;
    }

    /**
     * One step: wait if the replica is paused, else claim one book and process it.
     *
     * @return true if a book was processed
     */
    boolean runOnce() throws InterruptedException {
        Duration pause = throttle.remaining();
        if (!pause.isZero()) {
            sleeper.sleep(pause.compareTo(CLAIM_WAIT) < 0 ? pause : CLAIM_WAIT);   // in steps: stop() is noticed
            return false;
        }
        Optional<Claim> claim = coordination.claim(Stage.INGESTION, owner, CLAIM_WAIT);
        if (claim.isEmpty()) {
            return false;
        }
        process(claim.get());
        return true;
    }

    private void process(Claim claim) throws InterruptedException {
        stats.claimed.incrementAndGet();
        int bookId = claim.bookId();
        // Only the ingestion is inside the try: a failure of complete() or fail() is a failure of
        // the coordination, not of the book, and goes up to run(), which waits and retries
        // (review of #28). The book then keeps its attempts; its lease expires if needed.
        IngestionService.IngestResult result;
        try {
            result = ingestWithRetries(bookId);
        } catch (BookNotFoundException e) {
            failed(claim, ErrorCode.NOT_FOUND, e.getMessage());
            return;
        } catch (InvalidBookException e) {
            failed(claim, ErrorCode.INVALID_BOOK, e.getMessage());
            return;
        } catch (SourceUnavailableException e) {
            if (e.isThrottling()) {
                throttle.throttled();
                LOG.warn("{}: the source throttles us ({}), pausing the replica {} s",
                        owner, e.getMessage(), throttle.remaining().toSeconds());
            }
            failed(claim, ErrorCode.DOWNLOAD_FAILED, e.getMessage());
            return;
        } catch (RuntimeException e) {
            LOG.error("{}: unexpected error ingesting book {}", owner, bookId, e);
            failed(claim, ErrorCode.INTERNAL_ERROR, e.toString());
            return;
        }
        boolean downloaded = result.outcome() == IngestionService.Outcome.DOWNLOADED;
        if (downloaded) {
            throttle.succeeded();       // only a real download says the source answers again
        }
        if (!coordination.complete(claim)) {
            lost(bookId, "complete");
        } else if (downloaded) {
            stats.downloaded.incrementAndGet();
        } else {
            stats.alreadyAvailable.incrementAndGet();
        }
    }

    private IngestionService.IngestResult ingestWithRetries(int bookId)
            throws BookNotFoundException, InvalidBookException, SourceUnavailableException, InterruptedException {
        for (int retry = 0; ; retry++) {
            try {
                return ingestion.ingest(bookId);
            } catch (SourceUnavailableException e) {
                if (e.isThrottling() || retry >= fetchRetries) {
                    throw e;
                }
                stats.fetchRetries.incrementAndGet();
                sleeper.sleep(RETRY_DELAY.multipliedBy(1L << retry));      // 1 s, 2 s, 4 s...
            }
        }
    }

    private void failed(Claim claim, ErrorCode code, String message) {
        FailOutcome outcome = coordination.fail(claim, code, message);
        switch (outcome) {
            case REQUEUED -> stats.requeued.incrementAndGet();
            case FAILED -> {
                stats.failed.incrementAndGet();
                LOG.warn("{}: book {} FAILED: {}: {}", owner, claim.bookId(), code, message);
            }
            case LEASE_LOST -> lost(claim.bookId(), "fail");
        }
    }

    /** The Control requeued the book while we worked on it: drop it, whatever we wrote is harmless. */
    private void lost(int bookId, String step) {
        stats.leaseLost.incrementAndGet();
        LOG.warn("{}: lost the lease of book {} before {}; dropped", owner, bookId, step);
    }
}
