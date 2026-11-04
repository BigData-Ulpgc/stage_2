package es.ulpgc.bigdata.ingestion.worker;

import com.hazelcast.core.HazelcastInstance;
import es.ulpgc.bigdata.coordination.BookState;
import es.ulpgc.bigdata.coordination.BookTask;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.Stage;
import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.ingestion.BookSplitter;
import es.ulpgc.bigdata.ingestion.FakeSource;
import es.ulpgc.bigdata.ingestion.IngestionService;
import es.ulpgc.bigdata.testsupport.HazelcastTestCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The worker against two embedded Hazelcast members (common's test-jar) and a fake source: every
 * row of the error table of IngestionWorker, the throttling pause and a lost lease. The clock
 * and the sleeps are the test's, so nothing waits for real.
 */
class IngestionWorkerTest {

    @TempDir
    Path dataDir;

    private static HazelcastInstance client;
    private final MutableClock clock = new MutableClock();
    private final List<Duration> sleeps = new CopyOnWriteArrayList<>();
    private final FakeSource source = new FakeSource();
    private Coordination coordination;
    private IngestionService ingestion;
    private Throttle throttle;
    private WorkerStats stats;
    private IngestionWorker worker;

    @BeforeAll
    static void startCluster() {
        client = HazelcastTestCluster.settings().openClient();
    }

    @AfterAll
    static void stopClient() {
        client.shutdown();
    }

    @BeforeEach
    void setUp() {
        HazelcastTestCluster.reset();
        coordination = Coordination.on(client, HazelcastTestCluster.settings(), clock);
        ingestion = new IngestionService(source, new BookSplitter(), new RangeBasedDatalake(dataDir.resolve("datalake/range")));
        throttle = new Throttle(clock);
        stats = new WorkerStats();
        worker = new IngestionWorker(coordination, ingestion, throttle, stats, "ingestion-test/w0", 2, sleeps::add);
    }

    // --- The normal flow ----------------------------------------------------------------

    @Test
    void aSeededBookIsDownloadedAndHandedToIndexing() throws Exception {
        source.book(1342);
        coordination.seed(List.of(1342));
        assertTrue(worker.runOnce());
        assertEquals(BookState.DOWNLOADED, state(1342));
        assertEquals(1, coordination.queueSize(Stage.INDEXING));     // T3 offers it in the same transaction
        assertTrue(ingestion.locate(1342).isPresent());
        assertEquals(1L, stats.downloaded.get());
    }

    @Test
    void aBookAlreadyInTheDatalakeIsCompletedWithoutDownloading() throws Exception {
        source.book(84);
        ingestion.ingest(84);                                          // e.g. a POST /ingest earlier
        coordination.seed(List.of(84));
        assertTrue(worker.runOnce());
        assertEquals(BookState.DOWNLOADED, state(84));
        assertEquals(1, source.fetchesOf(84));                         // only the first ingestion fetched
        assertEquals(1L, stats.alreadyAvailable.get());
    }

    @Test
    void anEmptyQueueClaimsNothing() throws Exception {
        assertFalse(worker.runOnce());
        assertEquals(0L, stats.claimed.get());
    }

    // --- Failures -----------------------------------------------------------------------

    @Test
    void aBookTheSourceDoesNotHaveFailsAtOnce() throws Exception {
        coordination.seed(List.of(7));
        worker.runOnce();
        BookTask task = coordination.task(7).orElseThrow();
        assertEquals(BookState.FAILED, task.bookState());
        assertEquals("INGESTION", task.failedStage());
        assertTrue(task.lastError().startsWith("NOT_FOUND"), task.lastError());
        assertEquals(1, source.fetchesOf(7));                          // permanent: not retried
        assertEquals(1L, stats.failed.get());
    }

    @Test
    void aBookWithoutMarkersFailsAtOnce() throws Exception {
        source.text(8, "no markers at all");
        coordination.seed(List.of(8));
        worker.runOnce();
        assertEquals(BookState.FAILED, state(8));
        assertTrue(coordination.task(8).orElseThrow().lastError().startsWith("INVALID_BOOK"));
        assertTrue(ingestion.locate(8).isEmpty());
    }

    @Test
    void aTransientFailureIsRetriedInsideTheAttempt() throws Exception {
        source.failsFirst(9, 2);                                       // 503, 503, then the book
        coordination.seed(List.of(9));
        worker.runOnce();
        assertEquals(BookState.DOWNLOADED, state(9));
        assertEquals(3, source.fetchesOf(9));
        assertEquals(List.of(Duration.ofSeconds(1), Duration.ofSeconds(2)), sleeps);
        assertEquals(0, coordination.task(9).orElseThrow().attempts());
        assertEquals(2L, stats.fetchRetries.get());
    }

    @Test
    void aSourceThatKeepsFailingRequeuesTheBookWithOneMoreAttempt() throws Exception {
        source.unavailable(9);
        coordination.seed(List.of(9));
        worker.runOnce();
        BookTask task = coordination.task(9).orElseThrow();
        assertEquals(BookState.PENDING, task.bookState());
        assertEquals(1, task.attempts());
        assertTrue(task.lastError().startsWith("DOWNLOAD_FAILED"), task.lastError());
        assertEquals(3, source.fetchesOf(9));                          // 1 + INGESTION_FETCH_RETRIES
        assertEquals(1, coordination.queueSize(Stage.INGESTION));
        assertEquals(1L, stats.requeued.get());
    }

    @Test
    void throttlingIsNotRetriedAndPausesTheReplica() throws Exception {
        source.throttled(5, 429);
        coordination.seed(List.of(5, 6));
        worker.runOnce();
        assertEquals(1, source.fetchesOf(5));                          // no retry against a 429
        assertEquals(Duration.ofSeconds(30), throttle.remaining());
        assertEquals(BookState.PENDING, state(5));

        sleeps.clear();
        assertFalse(worker.runOnce());                                 // paused: it does not claim
        assertEquals(List.of(IngestionWorker.CLAIM_WAIT), sleeps);
        assertEquals(2, coordination.queueSize(Stage.INGESTION));

        clock.advance(Duration.ofSeconds(30));
        source.book(6);
        assertTrue(worker.runOnce());                                  // after the pause it claims again
    }

    @Test
    void aForbiddenAnswerIsThrottlingToo() throws Exception {
        source.throttled(5, 403);
        coordination.seed(List.of(5));
        worker.runOnce();
        assertEquals(Duration.ofSeconds(30), throttle.remaining());
    }

    @Test
    void aWorkerThatLostItsLeaseDropsTheBook() throws Exception {
        source.book(11).whileFetching(11, () -> {
            clock.advance(Duration.ofSeconds(121));                    // the download takes longer than the lease
            coordination.requeueExpiredLeases();                      // and the Control requeues the book
        });
        coordination.seed(List.of(11));
        worker.runOnce();
        assertEquals(1L, stats.leaseLost.get());
        BookTask task = coordination.task(11).orElseThrow();
        assertEquals(BookState.PENDING, task.bookState());             // the Control's requeue stands
        assertTrue(task.lastError().startsWith("LEASE_EXPIRED"));
        assertTrue(ingestion.locate(11).isPresent());                  // what it wrote is harmless
    }

    // --- Several workers ----------------------------------------------------------------

    @Test
    void fourWorkersShareTheQueueAndEachBookIsDownloadedOnce() throws Exception {
        int books = 100;
        IntStream.rangeClosed(1, books).forEach(source::book);
        coordination.seed(IntStream.rangeClosed(1, books).boxed().toList());
        try (IngestionWorkers pool = IngestionWorkers.start(coordination, ingestion, 4, 2, clock)) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (coordination.countByState().get(BookState.DOWNLOADED) < books && System.nanoTime() < deadline) {
                Thread.sleep(100);
            }
            assertEquals((long) books, coordination.countByState().get(BookState.DOWNLOADED));
            assertEquals(books, source.fetches.get());                 // nobody downloaded a book twice
            assertEquals(4, pool.status().get("workers"));
            assertEquals((long) books, pool.status().get("downloaded"));
        }
    }

    @Test
    void closingThePoolStopsIdleWorkersQuickly() {
        IngestionWorkers pool = IngestionWorkers.start(coordination, ingestion, 4, 2, clock);
        long start = System.nanoTime();
        pool.close();
        long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(millis < IngestionWorker.CLAIM_WAIT.toMillis() + 2_000, "closed in " + millis + " ms");
    }

    @Test
    void aWorkerWhoseClientWasShutDownStops() throws Exception {
        HazelcastInstance own = HazelcastTestCluster.settings().openClient();
        IngestionWorker orphan = new IngestionWorker(Coordination.on(own, HazelcastTestCluster.settings(), clock),
                ingestion, throttle, stats, "ingestion-test/w9", 2, sleeps::add);
        own.shutdown();
        Thread thread = new Thread(orphan);
        thread.start();
        thread.join(10_000);
        assertFalse(thread.isAlive());
    }

    private BookState state(int bookId) {
        return coordination.task(bookId).orElseThrow().bookState();
    }
}
