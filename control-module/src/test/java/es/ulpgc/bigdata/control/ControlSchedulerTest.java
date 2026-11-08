package es.ulpgc.bigdata.control;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A periodic task that throws is cancelled for good by ScheduledExecutorService: one Hazelcast
 * failure would stop the lease check or the publication without a word. The scheduler must
 * catch it and run the task again (review of #35).
 */
class ControlSchedulerTest {

    private static final Duration PERIOD = Duration.ofMillis(20);

    @Test
    void aTaskThatThrowsRunsAgainAtTheNextPeriod() throws Exception {
        AtomicInteger leaseChecks = new AtomicInteger();
        AtomicInteger publications = new AtomicInteger();
        try (ControlScheduler scheduler = ControlScheduler.start(
                () -> {
                    leaseChecks.incrementAndGet();
                    throw new IllegalStateException("Hazelcast is not answering");
                },
                () -> {
                    publications.incrementAndGet();
                    throw new IllegalStateException("Indexing is down");
                }, PERIOD, PERIOD)) {
            waitFor(() -> leaseChecks.get() >= 3 && publications.get() >= 3);
        }
    }

    @Test
    void closeStopsBothTasks() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        ControlScheduler scheduler = ControlScheduler.start(runs::incrementAndGet, runs::incrementAndGet, PERIOD, PERIOD);
        waitFor(() -> runs.get() >= 2);
        scheduler.close();
        int afterClose = runs.get();
        Thread.sleep(PERIOD.toMillis() * 5);
        assertEquals(afterClose, runs.get());
    }

    private static void waitFor(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, "timed out");
            Thread.sleep(10);
        }
    }
}
