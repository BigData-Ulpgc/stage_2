package es.ulpgc.bigdata.control;

import es.ulpgc.bigdata.coordination.BookState;
import es.ulpgc.bigdata.coordination.Coordination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The two periodic jobs of the Control Module (CF-09). Nothing else in the system does them:
 *
 * <ul>
 *   <li><b>Lease check</b> (every LEASE_CHECK_SECONDS, coordination contract §4.2): books
 *       INGESTING or INDEXING whose worker died or hung go back to their queue, or to FAILED
 *       when their attempts are used up.</li>
 *   <li><b>Publication</b> (checked every second): POST /index/publish when PUBLISH_BATCH books
 *       are INDEXED, or when PUBLISH_INTERVAL_S have passed since the last publication and at
 *       least one book is INDEXED. Indexing does T10 and announces the version (DR-08).</li>
 * </ul>
 *
 * A failed publication (Indexing down, a timeout, 409 during a rebuild, 503 without MongoDB) is
 * retried after 1 s, 2 s, 4 s... up to 30 s, instead of every second; the publication is
 * idempotent, so a retry never duplicates anything.
 *
 * A publication that made nothing new visible (books_published 0) means the INDEXED books are
 * already published, or never will be: T10 has not marked them yet, it failed, or MongoDB was
 * emptied without Hazelcast. A full batch then does not trigger again until the interval is
 * over, so Indexing gets at most one useless call per interval instead of one per second
 * (review of #35), unless the INDEXED books first drop below the batch: Indexing runs T10
 * right after answering (review of #43), and a slow T10 must not delay the next full batch. The methods are called by ControlScheduler,
 * one thread each, and by the tests with their own clock.
 */
public final class ControlTasks {

    static final Duration MIN_BACKOFF = Duration.ofSeconds(1);
    static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

    private static final Logger LOG = LoggerFactory.getLogger(ControlTasks.class);

    private final Coordination coordination;
    private final IndexingClient indexing;
    private final Clock clock;
    private final int publishBatch;
    private final Duration publishInterval;

    // Publication state: only touched by the publication thread, read by GET /control/status
    private volatile long lastPublishedAt;
    private volatile long notBefore;
    private volatile int consecutiveFailures;
    private volatile IndexingClient.PublishOutcome lastOutcome;
    private volatile String lastPublishError;
    private volatile long publications;
    private volatile boolean lastWasEmpty;

    // Lease check state
    private volatile long leasesRequeued;
    private volatile long lastLeaseCheck;

    public ControlTasks(Coordination coordination, IndexingClient indexing, Clock clock,
                        int publishBatch, Duration publishInterval) {
        this.coordination = Objects.requireNonNull(coordination, "coordination");
        this.indexing = Objects.requireNonNull(indexing, "indexing");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.publishBatch = publishBatch;
        this.publishInterval = Objects.requireNonNull(publishInterval, "publishInterval");
        this.lastPublishedAt = clock.millis();         // the interval starts with the Control
    }

    /** Requeues (or fails) the books with an expired lease. @return how many */
    public int checkLeases() {
        int changed = coordination.requeueExpiredLeases();
        lastLeaseCheck = clock.millis();
        if (changed > 0) {
            leasesRequeued += changed;
            LOG.info("lease check: {} books with an expired lease requeued or failed", changed);
        }
        return changed;
    }

    /**
     * Publishes if it is time to.
     *
     * @return what Indexing answered, or empty if it was not time, or the call failed
     */
    public Optional<IndexingClient.PublishOutcome> maybePublish() throws InterruptedException {
        long now = clock.millis();
        if (now < notBefore) {
            return Optional.empty();                    // backing off after a failure
        }
        long indexed = coordination.count(BookState.INDEXED);
        if (indexed < publishBatch) {
            lastWasEmpty = false;                       // T10 caught up: a full batch is new again
        }
        boolean batchFull = indexed >= publishBatch && !lastWasEmpty;
        boolean intervalOver = indexed > 0 && now - lastPublishedAt >= publishInterval.toMillis();
        if (!batchFull && !intervalOver) {
            return Optional.empty();
        }
        try {
            IndexingClient.PublishOutcome outcome = indexing.publish();
            lastPublishedAt = now;
            consecutiveFailures = 0;
            notBefore = 0;
            lastOutcome = outcome;
            lastPublishError = null;
            publications++;
            lastWasEmpty = outcome.booksPublished() == 0;
            if (lastWasEmpty) {
                LOG.info("nothing new to publish: {} books INDEXED but none new in the index, version {} stays;"
                        + " a full batch waits {} s before the next try", indexed, outcome.version(), publishInterval.toSeconds());
            } else {
                LOG.info("published version {}: {} books new, {} visible ({} were INDEXED, {})", outcome.version(),
                        outcome.booksPublished(), outcome.totalBooks(), indexed, batchFull ? "batch full" : "interval over");
            }
            return Optional.of(outcome);
        } catch (IndexingClient.PublishFailedException e) {
            consecutiveFailures++;
            Duration wait = MIN_BACKOFF.multipliedBy(1L << Math.min(consecutiveFailures - 1, 5));
            wait = wait.compareTo(MAX_BACKOFF) > 0 ? MAX_BACKOFF : wait;
            notBefore = now + wait.toMillis();
            lastPublishError = e.getMessage();
            LOG.warn("publication failed ({}), retrying in {} s", e.getMessage(), wait.toSeconds());
            return Optional.empty();
        }
    }

    /** For GET /control/status. */
    public Map<String, Object> status() {
        Map<String, Object> publication = new LinkedHashMap<>();
        IndexingClient.PublishOutcome last = lastOutcome;
        publication.put("batch", publishBatch);
        publication.put("interval_seconds", publishInterval.toSeconds());
        publication.put("publications", publications);
        publication.put("version", last == null ? null : last.version());
        publication.put("last_books_published", last == null ? null : last.booksPublished());
        publication.put("total_books", last == null ? null : last.totalBooks());
        publication.put("published_at", last == null ? null : last.publishedAt());
        publication.put("consecutive_failures", consecutiveFailures);
        publication.put("last_error", lastPublishError);

        Map<String, Object> leases = new LinkedHashMap<>();
        leases.put("requeued_or_failed", leasesRequeued);
        leases.put("last_check", lastLeaseCheck == 0 ? null : Instant.ofEpochMilli(lastLeaseCheck).toString());

        Map<String, Object> status = new LinkedHashMap<>();
        status.put("publication", publication);
        status.put("lease_check", leases);
        return status;
    }
}
