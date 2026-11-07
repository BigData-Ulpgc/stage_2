package es.ulpgc.bigdata.control;

import es.ulpgc.bigdata.coordination.BookState;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.model.BookIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Rebuilds book-state when the Hazelcast cluster lost it (CF-10, coordination contract §7): both
 * members died, or the cluster was restarted. The sign is that coordination-meta has no
 * "initialized" marker; it is checked at startup and before every lease check.
 *
 * Each book gets the most advanced state its persistent evidence proves:
 * <ol>
 *   <li>published in MongoDB (index_books up to CURRENT) → PUBLISHED;</li>
 *   <li>else in index_log → INDEXED (the next publication makes it visible);</li>
 *   <li>else {@code <id>._READY} in the datalake → DOWNLOADED, offered to index-queue;</li>
 *   <li>then the seed list (CONTROL_SEED_FILE) is seeded again, so the rest is PENDING;</li>
 *   <li>finally the marker is written.</li>
 * </ol>
 *
 * The evidence is read entirely BEFORE anything is written: if MongoDB or the datalake fails,
 * nothing changes, the marker stays missing and the next check tries again. A run that fails
 * halfway is also safe to repeat: restore and seed never duplicate anything.
 *
 * What is lost: a book submitted with POST /control/jobs that had no evidence yet (it was
 * PENDING or INGESTING) and is not in the seed file. Submit it again.
 */
public final class Recovery {

    /** Where the evidence comes from. Each read may throw (MongoDB down, an unreadable folder). */
    public interface Evidence {

        /** Books visible to Search. */
        Set<Integer> published();

        /** Books complete in the index (index_log). */
        Set<Integer> indexed();

        /** Books complete in the datalake (with their _READY marker). */
        Collection<Integer> downloaded();
    }

    /** What one recovery did. */
    public record Outcome(int published, int indexed, int downloaded, int seeded, long millis) {
    }

    private static final Logger LOG = LoggerFactory.getLogger(Recovery.class);

    private final Coordination coordination;
    private final Evidence evidence;
    private final List<Integer> seed;
    private final Clock clock;

    // Only the lease-check thread runs recoveries; GET /control/status reads these
    private volatile long runs;
    private volatile Outcome lastOutcome;
    private volatile long lastRunAt;
    private volatile String lastError;

    public Recovery(Coordination coordination, Evidence evidence, List<Integer> seed, Clock clock) {
        this.coordination = Objects.requireNonNull(coordination, "coordination");
        this.evidence = Objects.requireNonNull(evidence, "evidence");
        this.seed = List.copyOf(seed);
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Rebuilds book-state if the marker is missing.
     *
     * @return what the recovery did, or empty if the cluster kept its state
     * @throws RuntimeException if the cluster, MongoDB or the datalake failed: nothing was marked,
     *                          the next call tries again
     */
    public Optional<Outcome> ensureInitialized() {
        if (coordination.isInitialized()) {
            return Optional.empty();
        }
        long start = clock.millis();
        LOG.warn("coordination-meta has no 'initialized' marker: the cluster is new or lost its state, rebuilding book-state");
        try {
            // 1. Read every source first: a failure here writes nothing
            Set<Integer> published = valid(evidence.published(), "index_books");
            Set<Integer> indexed = valid(evidence.indexed(), "index_log");
            Set<Integer> downloaded = valid(evidence.downloaded(), "datalake");
            indexed.removeAll(published);
            downloaded.removeAll(published);
            downloaded.removeAll(indexed);

            // 2. Write, from the most advanced state
            int p = coordination.restore(published, BookState.PUBLISHED);
            int i = coordination.restore(indexed, BookState.INDEXED);
            int d = coordination.restore(downloaded, BookState.DOWNLOADED);
            int s = coordination.seed(seed);
            coordination.markInitialized();

            Outcome outcome = new Outcome(p, i, d, s, clock.millis() - start);
            runs++;
            lastRunAt = start;
            lastOutcome = outcome;
            lastError = null;
            LOG.info("book-state rebuilt in {} ms: {} PUBLISHED, {} INDEXED, {} DOWNLOADED, {} seeded PENDING",
                    outcome.millis(), p, i, d, s);
            return Optional.of(outcome);
        } catch (RuntimeException e) {
            lastError = e.toString();
            throw e;
        }
    }

    /** A copy without the ids that are not book ids (a bug elsewhere): restore would reject them all. */
    private static Set<Integer> valid(Collection<Integer> ids, String source) {
        Set<Integer> valid = new TreeSet<>();
        int dropped = 0;
        for (Integer id : ids) {
            if (id != null && BookIds.isValid(id)) {
                valid.add(id);
            } else {
                dropped++;
            }
        }
        if (dropped > 0) {
            LOG.warn("{} ids of {} are not book ids, ignored", dropped, source);
        }
        return valid;
    }

    /** For GET /control/status. */
    public Map<String, Object> status() {
        Outcome last = lastOutcome;
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("runs", runs);
        status.put("last_run", lastRunAt == 0 ? null : Instant.ofEpochMilli(lastRunAt).toString());
        status.put("published", last == null ? null : last.published());
        status.put("indexed", last == null ? null : last.indexed());
        status.put("downloaded", last == null ? null : last.downloaded());
        status.put("seeded", last == null ? null : last.seeded());
        status.put("millis", last == null ? null : last.millis());
        status.put("last_error", lastError);
        return status;
    }
}
