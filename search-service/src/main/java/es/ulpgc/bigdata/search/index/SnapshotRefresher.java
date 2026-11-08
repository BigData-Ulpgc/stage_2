package es.ulpgc.bigdata.search.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Keeps this replica on the published version without restarting it: every few seconds it
 * asks SnapshotHolder to reload, which costs one small query unless CURRENT has changed.
 *
 * This is the periodic check of plan DP-06. It also covers a replica that starts while
 * MongoDB is down, and the IndexPublished events that were lost. An event only makes the
 * replica react sooner: it calls wakeUp() (see PublicationEvents).
 *
 * Every check runs on the single thread of this class, so a slow reload never blocks the
 * thread that delivers the Hazelcast events and two checks never overlap.
 *
 * If a check fails, the replica keeps serving the snapshot it has and tries again next time.
 */
public final class SnapshotRefresher implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotRefresher.class);

    private final SnapshotHolder snapshots;
    private final Duration interval;
    private final ScheduledExecutorService scheduler;
    private boolean failing;

    public SnapshotRefresher(SnapshotHolder snapshots, Duration interval) {
        this.snapshots = Objects.requireNonNull(snapshots, "snapshots");
        this.interval = Objects.requireNonNull(interval, "interval");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "snapshot-refresher");
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Fixed delay: the next check is counted from the end of this one, so they never pile up. */
    public void start() {
        scheduler.scheduleWithFixedDelay(this::refreshOnce, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Checks as soon as possible instead of waiting for the next period. Returns at once. */
    public void wakeUp() {
        try {
            scheduler.execute(this::refreshOnce);
        } catch (RejectedExecutionException e) {
            LOG.debug("Ignoring a wake-up: the service is stopping");
        }
    }

    /**
     * One check. It must not throw: an exception escaping a scheduled task would silently
     * cancel all later runs, and the replica would stay on an old version forever. A long
     * outage logs one warning, not one per check.
     */
    void refreshOnce() {
        try {
            Optional<IndexSnapshot> before = snapshots.current();
            Optional<IndexSnapshot> after = snapshots.reload();
            if (failing) {
                LOG.info("The published index can be read again");
                failing = false;
            }
            if (after.isPresent() && !after.equals(before)) {
                LOG.info("Now serving index version {} ({} books, collection {})",
                        after.get().version(), after.get().bookCount(), after.get().collection());
            }
        } catch (RuntimeException e) {
            if (!failing) {
                LOG.warn("Could not check the published index, keeping the current snapshot: {}", e.getMessage());
                failing = true;
            }
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
