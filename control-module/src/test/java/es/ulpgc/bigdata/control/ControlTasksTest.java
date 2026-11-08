package es.ulpgc.bigdata.control;

import com.hazelcast.core.HazelcastInstance;
import es.ulpgc.bigdata.coordination.BookState;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.Stage;
import es.ulpgc.bigdata.testsupport.HazelcastTestCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The lease check and the publication rule of the Control, on the embedded cluster, with a clock
 * the test moves and an Indexing that only counts the calls (and fails when told to).
 */
class ControlTasksTest {

    private static final Duration SHORT = Duration.ofMillis(200);
    private static final Duration INTERVAL = Duration.ofSeconds(30);

    private static HazelcastInstance client;

    private final MutableClock clock = new MutableClock();
    private final FakeIndexing indexing = new FakeIndexing();
    private Coordination coordination;

    @BeforeAll
    static void startCluster() {
        client = HazelcastTestCluster.settings().openClient();
    }

    @AfterAll
    static void stopClient() {
        client.shutdown();
    }

    @BeforeEach
    void emptyStructures() {
        HazelcastTestCluster.reset();
        coordination = Coordination.on(client, HazelcastTestCluster.settings(), clock);
    }

    private ControlTasks tasks(int batch) {
        return new ControlTasks(coordination, indexing, clock, batch, INTERVAL);
    }

    // --- Publication ---------------------------------------------------------------------

    @Test
    void nothingIndexedIsNeverPublished() throws Exception {
        ControlTasks tasks = tasks(1);
        clock.advance(INTERVAL.multipliedBy(3));
        assertTrue(tasks.maybePublish().isEmpty());
        assertEquals(0, indexing.calls);
    }

    @Test
    void aFullBatchIsPublishedAtOnce() throws Exception {
        ControlTasks tasks = tasks(2);
        indexed(1);
        assertTrue(tasks.maybePublish().isEmpty(), "1 of 2: wait for the batch or the interval");
        indexed(2);
        assertEquals(7, tasks.maybePublish().orElseThrow().version());
        assertEquals(1, indexing.calls);
    }

    @Test
    void lessThanABatchIsPublishedWhenTheIntervalIsOver() throws Exception {
        ControlTasks tasks = tasks(100);
        indexed(1);
        clock.advance(INTERVAL.minusSeconds(1));
        assertTrue(tasks.maybePublish().isEmpty());
        clock.advance(Duration.ofSeconds(1));
        assertTrue(tasks.maybePublish().isPresent());
        assertEquals(1, indexing.calls);
        // The interval starts again with the publication
        clock.advance(INTERVAL.minusSeconds(1));
        assertTrue(tasks.maybePublish().isEmpty());
        assertEquals(1, indexing.calls);
    }

    @Test
    void aFailedPublicationIsRetriedWithAGrowingDelay() throws Exception {
        ControlTasks tasks = tasks(1);
        indexed(1);
        indexing.failNext(3);
        // 1st failure: wait 1 s; 2nd: 2 s; 3rd: 4 s
        for (int wait : new int[]{1, 2, 4}) {
            int before = indexing.calls;
            assertTrue(tasks.maybePublish().isEmpty());
            assertEquals(before + 1, indexing.calls);
            clock.advance(Duration.ofSeconds(wait).minusMillis(1));
            assertTrue(tasks.maybePublish().isEmpty());
            assertEquals(before + 1, indexing.calls, "retried before " + wait + " s");
            clock.advance(Duration.ofMillis(1));
        }
        Map<String, Object> failing = publication(tasks);
        assertEquals(3, failing.get("consecutive_failures"));
        assertEquals("Indexing is down", failing.get("last_error"));

        assertTrue(tasks.maybePublish().isPresent());
        Map<String, Object> published = publication(tasks);
        assertEquals(0, published.get("consecutive_failures"));
        assertNull(published.get("last_error"));
        assertEquals(1L, published.get("publications"));
        assertEquals(7, published.get("version"));
    }

    @Test
    void theDelayStopsGrowingAt30Seconds() throws Exception {
        ControlTasks tasks = tasks(1);
        indexed(1);
        indexing.failNext(Integer.MAX_VALUE);
        for (int i = 0; i < 10; i++) {
            tasks.maybePublish();
            clock.advance(ControlTasks.MAX_BACKOFF);
        }
        assertEquals(10, indexing.calls, "never more than 30 s between two attempts");
    }

    /**
     * Indexing answered 0 new books (T10 is late or failed, MongoDB was emptied): the full batch
     * waits for the interval instead of calling every second (review of #35).
     */
    @Test
    void aPublicationWithNothingNewWaitsForTheInterval() throws Exception {
        ControlTasks tasks = tasks(1);
        indexed(1);
        indexing.booksPublished = 0;
        assertTrue(tasks.maybePublish().isPresent());
        clock.advance(Duration.ofSeconds(1));
        assertTrue(tasks.maybePublish().isEmpty(), "the batch is still full, but nothing new came of it");
        assertEquals(1, indexing.calls);

        clock.advance(INTERVAL.minusSeconds(1));
        indexing.booksPublished = 2;
        assertTrue(tasks.maybePublish().isPresent(), "the interval is over");
        assertEquals(2, indexing.calls);
        clock.advance(Duration.ofSeconds(1));
        assertTrue(tasks.maybePublish().isPresent(), "something was published: the full batch counts again");
        assertEquals(3, indexing.calls);
    }

    @Test
    void aLateT10DoesNotDelayTheNextFullBatch() throws Exception {
        ControlTasks tasks = tasks(1);
        indexed(1);
        indexing.booksPublished = 0;                    // published, but T10 has not marked it yet
        assertTrue(tasks.maybePublish().isPresent());
        clock.advance(Duration.ofSeconds(1));
        assertTrue(tasks.maybePublish().isEmpty(), "the same INDEXED book: nothing new");

        coordination.markPublished(List.of(1));         // T10 arrives
        clock.advance(Duration.ofSeconds(1));
        assertTrue(tasks.maybePublish().isEmpty(), "nothing INDEXED");
        indexed(2);
        indexing.booksPublished = 1;
        clock.advance(Duration.ofSeconds(1));
        assertTrue(tasks.maybePublish().isPresent(), "a new full batch, long before the interval");
        assertEquals(2, indexing.calls);
    }

    // --- Lease check ---------------------------------------------------------------------

    @Test
    void anExpiredLeaseIsRequeuedByTheCheck() throws Exception {
        ControlTasks tasks = tasks(100);
        coordination.seed(List.of(1342));
        coordination.claim(Stage.INGESTION, "ingestion-dead/w0", SHORT).orElseThrow();
        assertEquals(0, tasks.checkLeases(), "the lease has not expired yet");

        clock.advance(Duration.ofSeconds(HazelcastTestCluster.settings().leaseSeconds() + 1));
        assertEquals(1, tasks.checkLeases());
        assertEquals(BookState.PENDING, coordination.task(1342).orElseThrow().bookState());
        assertEquals(1, coordination.queueSize(Stage.INGESTION));

        @SuppressWarnings("unchecked")
        Map<String, Object> leases = (Map<String, Object>) tasks.status().get("lease_check");
        assertEquals(1L, leases.get("requeued_or_failed"));
        assertEquals(clock.instant().toString(), leases.get("last_check"));
    }

    // --- Helpers -------------------------------------------------------------------------

    /** Seeds the books and takes them through both stages: INDEXED. */
    private void indexed(int... ids) throws Exception {
        for (int id : ids) {
            coordination.seed(List.of(id));
            coordination.complete(coordination.claim(Stage.INGESTION, "ingestion-test/w0", SHORT).orElseThrow());
            coordination.complete(coordination.claim(Stage.INDEXING, "indexing-test/w0", SHORT).orElseThrow());
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> publication(ControlTasks tasks) {
        return (Map<String, Object>) tasks.status().get("publication");
    }

    /** POST /index/publish that answers version 7, or fails while told to. */
    private static final class FakeIndexing extends IndexingClient {

        private final Deque<Boolean> failures = new ArrayDeque<>();
        int calls;
        int booksPublished = 2;

        FakeIndexing() {
            super(URI.create("http://unused.invalid"));
        }

        void failNext(int n) {
            for (int i = 0; i < Math.min(n, 1_000); i++) {
                failures.add(true);
            }
        }

        @Override
        public PublishOutcome publish() throws PublishFailedException {
            calls++;
            if (Optional.ofNullable(failures.poll()).orElse(false)) {
                throw new PublishFailedException("Indexing is down");
            }
            return new PublishOutcome(7, booksPublished, 2, "2026-11-04T10:00:30Z");
        }
    }
}
