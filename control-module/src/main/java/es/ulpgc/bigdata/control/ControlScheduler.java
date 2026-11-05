package es.ulpgc.bigdata.control;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runs the ControlTasks periodically, one thread each: a publication that waits the full 30 s
 * for Indexing never delays the lease check. The lease check runs at once and then every
 * LEASE_CHECK_SECONDS, each time after the recovery check (CF-10): a cluster that lost its state
 * is rebuilt before its leases are looked at.
 *
 * A periodic task that throws is cancelled for good by ScheduledExecutorService, so every run
 * catches what it throws (a Hazelcast call while the cluster is down, for instance), logs it and
 * waits for the next period.
 */
public final class ControlScheduler implements AutoCloseable {

    static final Duration PUBLICATION_CHECK = Duration.ofSeconds(1);

    private static final Logger LOG = LoggerFactory.getLogger(ControlScheduler.class);

    private final ScheduledExecutorService executor;

    private ControlScheduler(ScheduledExecutorService executor) {
        this.executor = executor;
    }

    public static ControlScheduler start(ControlTasks tasks, Recovery recovery, Duration leaseCheck) {
        Objects.requireNonNull(tasks, "tasks");
        Objects.requireNonNull(recovery, "recovery");
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2, runnable -> {
            Thread thread = new Thread(runnable, "control-tasks");
            thread.setDaemon(true);
            return thread;
        });
        executor.scheduleWithFixedDelay(() -> {
                    safely("recovery", recovery::ensureInitialized);
                    safely("lease check", tasks::checkLeases);
                },
                0, leaseCheck.toMillis(), TimeUnit.MILLISECONDS);
        executor.scheduleWithFixedDelay(() -> safely("publication", () -> {
                    try {
                        tasks.maybePublish();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();            // close(): the executor is stopping
                    }
                }),
                PUBLICATION_CHECK.toMillis(), PUBLICATION_CHECK.toMillis(), TimeUnit.MILLISECONDS);
        return new ControlScheduler(executor);
    }

    private static void safely(String what, Runnable task) {
        try {
            task.run();
        } catch (RuntimeException e) {
            LOG.warn("{} failed ({}), trying again at the next period", what, e.toString());
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
