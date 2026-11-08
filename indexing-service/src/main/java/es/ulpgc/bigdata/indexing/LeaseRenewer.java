package es.ulpgc.bigdata.indexing;

import es.ulpgc.bigdata.coordination.Claim;
import es.ulpgc.bigdata.coordination.Coordination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Keeps the leases of the batch the index writer is writing. A batch is not always quick: as the
 * index grows, updating the posting lists of the frequent terms takes longer (DR-13 measured a
 * batch of 100 books at about 30 s with 10,000 books in the index), and a batch longer than
 * LEASE_SECONDS would be requeued by the Control's lease check while it is still being written.
 * With several attempts lost that way the books would end FAILED although nothing failed.
 *
 * While it is open, every {@code every} (a third of the lease) it renews each claim of the batch
 * (Coordination.renew, a CAS on the task). A claim the coordination no longer has (the book was
 * requeued anyway, for instance after a stall longer than the lease) is dropped: the writer must
 * not complete it. A renewal that fails (the cluster is lost) is tried again at the next turn.
 *
 * {@link #stop()} stops the renewals and returns the latest claim of each book, which is the one
 * complete and fail must use: a renewal replaces the task, so the claim the writer took first no
 * longer matches. stop() waits for a renewal in progress, and none runs after it, so they never
 * race with complete or fail.
 */
final class LeaseRenewer {

    private static final Logger LOG = LoggerFactory.getLogger(LeaseRenewer.class);

    private final Coordination coordination;
    private final Map<Integer, Claim> claims = new LinkedHashMap<>();   // guarded by this
    private final ScheduledFuture<?> renewals;
    private boolean closed = false;                                   // guarded by this

    LeaseRenewer(Coordination coordination, Collection<Claim> batch, ScheduledExecutorService scheduler,
                 Duration every) {
        this.coordination = Objects.requireNonNull(coordination, "coordination");
        batch.forEach(claim -> claims.put(claim.bookId(), claim));
        long millis = Math.max(1, every.toMillis());
        this.renewals = scheduler.scheduleWithFixedDelay(this::renewAll, millis, millis, TimeUnit.MILLISECONDS);
    }

    /** One turn: renew every claim still held; drop the ones the coordination lost. */
    synchronized void renewAll() {
        if (closed) {
            return;
        }
        for (Iterator<Map.Entry<Integer, Claim>> it = claims.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Integer, Claim> entry = it.next();
            try {
                var renewed = coordination.renew(entry.getValue());
                if (renewed.isPresent()) {
                    entry.setValue(renewed.get());
                } else {
                    it.remove();
                    LOG.warn("lost the lease of book {} while writing its batch", entry.getKey());
                }
            } catch (RuntimeException e) {
                LOG.warn("could not renew the lease of book {} ({}); trying again in the next turn",
                        entry.getKey(), e.toString());
            }
        }
    }

    /**
     * Stops the renewals; after it returns none runs.
     *
     * @return the latest claim of each book still held, by book id
     */
    synchronized Map<Integer, Claim> stop() {
        closed = true;
        renewals.cancel(false);
        return Map.copyOf(claims);
    }
}
