package es.ulpgc.bigdata.coordination;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.transaction.TransactionContext;
import es.ulpgc.bigdata.testsupport.HazelcastTestCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every transition and rule of the coordination contract (api/contracts/coordination.md),
 * against two embedded Hazelcast members. The clock is controlled by the test, so leases
 * expire without waiting.
 */
class CoordinationTest {

    private static final Duration SHORT = Duration.ofMillis(300);

    private static HazelcastInstance client;
    private final TestClock clock = new TestClock();
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

    // --- T1 seed -------------------------------------------------------------------------

    @Test
    void seedingTwiceEnqueuesEachBookOnce() {
        assertEquals(3, coordination.seed(List.of(1342, 84, 11)));
        assertEquals(0, coordination.seed(List.of(1342, 84, 11)));
        assertEquals(1, coordination.seed(List.of(1342, 2701, 2701)));   // duplicates in one call too
        assertEquals(4, coordination.queueSize(Stage.INGESTION));
        assertEquals(BookState.PENDING, state(1342));
    }

    @Test
    void seedingLeavesAKnownBookAsItIs() throws Exception {
        coordination.seed(List.of(1));
        coordination.complete(claim(Stage.INGESTION));
        assertEquals(0, coordination.seed(List.of(1)));
        assertEquals(BookState.DOWNLOADED, state(1));
        assertEquals(0, coordination.queueSize(Stage.INGESTION));
    }

    @Test
    void seedRejectsAnInvalidIdAndSeedsNothing() {
        assertThrows(IllegalArgumentException.class, () -> coordination.seed(List.of(1, 0)));
        assertThrows(IllegalArgumentException.class, () -> coordination.seed(List.of(1_000_000_000)));
        assertTrue(coordination.task(1).isEmpty());
    }

    @Test
    void aLargeSeedIsSplitInTransactions() {
        List<Integer> ids = IntStream.rangeClosed(1, Coordination.SEED_BATCH * 2 + 7).boxed().toList();
        assertEquals(ids.size(), coordination.seed(ids));
        assertEquals(ids.size(), coordination.queueSize(Stage.INGESTION));
    }

    // --- T2-T3, T6-T7, T10: the normal flow -------------------------------------------

    @Test
    void aBookGoesThroughEveryStageToPublished() throws Exception {
        coordination.seed(List.of(1342));

        Claim ingestion = claim(Stage.INGESTION);
        assertEquals(BookState.INGESTING, ingestion.task().bookState());
        assertTrue(coordination.complete(ingestion));
        BookTask downloaded = coordination.task(1342).orElseThrow();
        assertEquals(BookState.DOWNLOADED, downloaded.bookState());
        assertNull(downloaded.owner());
        assertEquals(0, downloaded.leaseUntil());
        assertEquals(1, coordination.queueSize(Stage.INDEXING));      // offered in the same transaction

        assertTrue(coordination.complete(claim(Stage.INDEXING)));
        assertEquals(BookState.INDEXED, state(1342));
        assertEquals(0, coordination.queueSize(Stage.INDEXING));

        assertEquals(1, coordination.markPublished(List.of(1342)));
        assertEquals(BookState.PUBLISHED, state(1342));
    }

    @Test
    void aClaimRecordsOwnerLeaseAndTimes() throws Exception {
        coordination.seed(List.of(7));
        long seededAt = clock.millis();
        clock.advance(Duration.ofSeconds(5));
        Claim claim = coordination.claim(Stage.INGESTION, "ingestion-test/w0", SHORT).orElseThrow();
        BookTask task = claim.task();
        assertEquals("ingestion-test/w0", task.owner());
        assertEquals(clock.millis() + 60_000, task.leaseUntil());
        assertEquals(seededAt, task.enqueuedAt());
        assertEquals(clock.millis(), task.startedAt());
        assertEquals(task, coordination.task(7).orElseThrow());       // the claim holds the stored value
    }

    @Test
    void aClaimDropsAnIdThatIsNotWaiting() throws Exception {
        coordination.seed(List.of(1, 2));
        client.getQueue(CoordinationNames.INGEST_QUEUE).offer(1);   // 1 is now twice in the queue
        assertTrue(coordination.complete(claim(Stage.INGESTION)));    // book 1
        assertTrue(coordination.complete(claim(Stage.INGESTION)));    // book 2
        assertTrue(coordination.claim(Stage.INGESTION, "w", SHORT).isEmpty());   // the duplicate of 1 is dropped
        assertEquals(0, coordination.queueSize(Stage.INGESTION));
        assertEquals(BookState.DOWNLOADED, state(1));
        assertEquals(1, coordination.idsSkipped());
        assertEquals(0, coordination.invalidIdsDropped());
    }

    @Test
    void aClaimDropsAnInvalidIdOrAnUnknownBook() throws Exception {
        client.getQueue(CoordinationNames.INGEST_QUEUE).offer(0);
        client.getQueue(CoordinationNames.INGEST_QUEUE).offer(555);  // not in book-state
        assertTrue(coordination.claim(Stage.INGESTION, "w", SHORT).isEmpty());
        assertEquals(0, coordination.queueSize(Stage.INGESTION));
        assertEquals(1, coordination.invalidIdsDropped());           // 0: a bad id, someone's bug
        assertEquals(1, coordination.idsSkipped());                  // 555: unknown book, harmless
    }

    @Test
    void aClaimedBookIsNotCountedAsDropped() throws Exception {
        coordination.seed(List.of(1));
        claim(Stage.INGESTION);
        assertEquals(0, coordination.invalidIdsDropped());
        assertEquals(0, coordination.idsSkipped());
    }

    @Test
    void aClaimOnAnEmptyQueueWaitsAndReturnsEmpty() throws Exception {
        long start = System.nanoTime();
        assertTrue(coordination.claim(Stage.INDEXING, "w", SHORT).isEmpty());
        long waited = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(waited >= 250 && waited < 5_000, "waited " + waited + " ms");
    }

    @Test
    void anInterruptedClaimThrowsAndLeavesTheQueueAlone() throws Exception {
        coordination.seed(List.of(9));
        Thread.currentThread().interrupt();
        assertThrows(InterruptedException.class, () -> coordination.claim(Stage.INGESTION, "w", Duration.ofSeconds(5)));
        assertEquals(1, coordination.queueSize(Stage.INGESTION));
        assertEquals(BookState.PENDING, state(9));
    }

    // --- T4-T5, T8-T9: failures --------------------------------------------------------

    @Test
    void aTransientFailureRequeuesWithOneMoreAttempt() throws Exception {
        coordination.seed(List.of(5));
        Claim claim = claim(Stage.INGESTION);
        assertEquals(FailOutcome.REQUEUED, coordination.fail(claim, ErrorCode.DOWNLOAD_FAILED, "HTTP 503"));
        BookTask task = coordination.task(5).orElseThrow();
        assertEquals(BookState.PENDING, task.bookState());
        assertEquals(1, task.attempts());
        assertEquals("DOWNLOAD_FAILED: HTTP 503", task.lastError());
        assertNull(task.owner());
        assertEquals(1, coordination.queueSize(Stage.INGESTION));
    }

    @Test
    void theLastAttemptSendsTheBookToFailed() throws Exception {
        coordination.seed(List.of(5));
        assertEquals(FailOutcome.REQUEUED, coordination.fail(claim(Stage.INGESTION), ErrorCode.DOWNLOAD_FAILED, "1"));
        assertEquals(FailOutcome.REQUEUED, coordination.fail(claim(Stage.INGESTION), ErrorCode.DOWNLOAD_FAILED, "2"));
        assertEquals(FailOutcome.FAILED, coordination.fail(claim(Stage.INGESTION), ErrorCode.DOWNLOAD_FAILED, "3"));
        BookTask task = coordination.task(5).orElseThrow();
        assertEquals(BookState.FAILED, task.bookState());
        assertEquals("INGESTION", task.failedStage());
        assertEquals(3, task.attempts());
        assertEquals(0, coordination.queueSize(Stage.INGESTION));
    }

    @Test
    void aPermanentErrorFailsAtOnce() throws Exception {
        coordination.seed(List.of(5));
        coordination.complete(claim(Stage.INGESTION));
        assertEquals(FailOutcome.FAILED, coordination.fail(claim(Stage.INDEXING), ErrorCode.EMPTY_BOOK, null));
        BookTask task = coordination.task(5).orElseThrow();
        assertEquals("INDEXING", task.failedStage());
        assertEquals("EMPTY_BOOK", task.lastError());
    }

    @Test
    void anOutageDoesNotCountAsAnAttempt() throws Exception {
        coordination.seed(List.of(5));
        coordination.complete(claim(Stage.INGESTION));
        for (int i = 0; i < 5; i++) {
            assertEquals(FailOutcome.REQUEUED, coordination.fail(claim(Stage.INDEXING), ErrorCode.STORAGE_UNAVAILABLE, "mongo"));
        }
        assertEquals(0, coordination.task(5).orElseThrow().attempts());
        assertEquals(BookState.DOWNLOADED, state(5));
    }

    @Test
    void aLongErrorMessageIsCut() throws Exception {
        coordination.seed(List.of(5));
        coordination.fail(claim(Stage.INGESTION), ErrorCode.INTERNAL_ERROR, "x".repeat(10_000));
        assertEquals(BookTask.MAX_ERROR_LENGTH, coordination.task(5).orElseThrow().lastError().length());
    }

    // --- Leases --------------------------------------------------------------------------

    @Test
    void anExpiredLeaseIsRequeuedByTheControl() throws Exception {
        coordination.seed(List.of(5));
        claim(Stage.INGESTION);
        clock.advance(Duration.ofSeconds(61));
        assertEquals(1, coordination.requeueExpiredLeases());
        BookTask task = coordination.task(5).orElseThrow();
        assertEquals(BookState.PENDING, task.bookState());
        assertEquals(1, task.attempts());
        assertTrue(task.lastError().startsWith("LEASE_EXPIRED: lease of ingestion-test/w0 expired"));
        assertEquals(1, coordination.queueSize(Stage.INGESTION));
    }

    @Test
    void aLeaseThatHasNotExpiredIsLeftAlone() throws Exception {
        coordination.seed(List.of(5));
        claim(Stage.INGESTION);
        clock.advance(Duration.ofSeconds(59));
        assertEquals(0, coordination.requeueExpiredLeases());
        assertEquals(BookState.INGESTING, state(5));
    }

    @Test
    void anExpiredIndexingLeaseGoesBackToIndexQueue() throws Exception {
        coordination.seed(List.of(5));
        coordination.complete(claim(Stage.INGESTION));
        claim(Stage.INDEXING);
        clock.advance(Duration.ofMinutes(2));
        assertEquals(1, coordination.requeueExpiredLeases());
        assertEquals(BookState.DOWNLOADED, state(5));
        assertEquals(1, coordination.queueSize(Stage.INDEXING));
    }

    @Test
    void aWorkerThatLostItsLeaseCanNeitherCompleteNorFail() throws Exception {
        coordination.seed(List.of(5));
        Claim late = claim(Stage.INGESTION);
        clock.advance(Duration.ofSeconds(61));
        coordination.requeueExpiredLeases();
        assertFalse(coordination.complete(late));
        assertEquals(FailOutcome.LEASE_LOST, coordination.fail(late, ErrorCode.DOWNLOAD_FAILED, "late"));
        assertEquals(BookState.PENDING, state(5));
        assertEquals(1, coordination.queueSize(Stage.INGESTION));       // not offered twice
    }

    @Test
    void aWorkerCanCompleteAfterItsLeaseIfNobodyRequeuedTheBook() throws Exception {
        coordination.seed(List.of(5));
        Claim claim = claim(Stage.INGESTION);
        clock.advance(Duration.ofSeconds(90));
        assertTrue(coordination.complete(claim));
    }

    @Test
    void renewingMovesTheLeaseForward() throws Exception {
        coordination.seed(List.of(5));
        Claim claim = claim(Stage.INGESTION);
        clock.advance(Duration.ofSeconds(50));
        Claim renewed = coordination.renew(claim).orElseThrow();
        clock.advance(Duration.ofSeconds(50));                       // 100 s after the claim
        assertEquals(0, coordination.requeueExpiredLeases());
        assertTrue(coordination.renew(claim).isEmpty());             // the old claim is stale
        assertTrue(coordination.complete(renewed));
    }

    @Test
    void theLastExpiredLeaseSendsTheBookToFailed() throws Exception {
        coordination.seed(List.of(5));
        for (int i = 0; i < 3; i++) {
            claim(Stage.INGESTION);
            clock.advance(Duration.ofSeconds(61));
            assertEquals(1, coordination.requeueExpiredLeases());
        }
        assertEquals(BookState.FAILED, state(5));
        assertEquals(0, coordination.queueSize(Stage.INGESTION));
    }

    // --- T11, status, T10 -----------------------------------------------------------------

    @Test
    void retryFailedSendsEachBookBackToTheStageThatFailed() throws Exception {
        coordination.seed(List.of(1, 2));
        Claim first = claim(Stage.INGESTION);
        Claim second = claim(Stage.INGESTION);
        coordination.fail(first, ErrorCode.NOT_FOUND, "404");            // 1 or 2 fails ingestion
        coordination.complete(second);
        coordination.fail(claim(Stage.INDEXING), ErrorCode.EMPTY_BOOK, null);   // the other fails indexing

        assertEquals(2, coordination.retryFailed());
        BookTask ingestionFailure = coordination.task(first.bookId()).orElseThrow();
        assertEquals(BookState.PENDING, ingestionFailure.bookState());
        assertEquals(0, ingestionFailure.attempts());
        assertNull(ingestionFailure.failedStage());
        assertEquals("NOT_FOUND: 404", ingestionFailure.lastError());     // kept for diagnosis
        assertEquals(BookState.DOWNLOADED, state(second.bookId()));
        assertEquals(1, coordination.queueSize(Stage.INGESTION));
        assertEquals(1, coordination.queueSize(Stage.INDEXING));
    }

    @Test
    void countByStateListsEveryState() throws Exception {
        coordination.seed(List.of(1, 2, 3));
        coordination.complete(claim(Stage.INGESTION));
        claim(Stage.INGESTION);
        Map<BookState, Long> counts = coordination.countByState();
        assertEquals(BookState.values().length, counts.size());
        assertEquals(1L, counts.get(BookState.PENDING));
        assertEquals(1L, counts.get(BookState.INGESTING));
        assertEquals(1L, counts.get(BookState.DOWNLOADED));
        assertEquals(0L, counts.get(BookState.FAILED));
    }

    @Test
    void countOfOneStateAgreesWithCountByState() throws Exception {
        coordination.seed(List.of(1, 2, 3, 4));
        coordination.complete(claim(Stage.INGESTION));
        coordination.complete(claim(Stage.INGESTION));
        assertEquals(2L, coordination.count(BookState.PENDING));
        assertEquals(2L, coordination.count(BookState.DOWNLOADED));
        assertEquals(0L, coordination.count(BookState.INDEXED));
        assertEquals(coordination.countByState().get(BookState.PENDING), coordination.count(BookState.PENDING));
    }

    @Test
    void markPublishedOnlyTouchesIndexedBooks() throws Exception {
        coordination.seed(List.of(1, 2));
        coordination.complete(claim(Stage.INGESTION));
        coordination.complete(claim(Stage.INDEXING));                  // one book INDEXED, the other PENDING
        assertEquals(1, coordination.markPublished(List.of(1, 2, 999)));
        assertEquals(1L, coordination.countByState().get(BookState.PUBLISHED));
        assertEquals(1L, coordination.countByState().get(BookState.PENDING));
        assertEquals(0, coordination.markPublished(List.of(1, 2)));    // repeating changes nothing
    }

    @Test
    void idsInListsTheBooksOfOneState() throws Exception {
        coordination.seed(List.of(1, 2, 3));
        coordination.complete(claim(Stage.INGESTION));
        coordination.complete(claim(Stage.INDEXING));
        coordination.complete(claim(Stage.INGESTION));
        coordination.complete(claim(Stage.INDEXING));
        assertEquals(Set.of(1, 2), coordination.idsIn(BookState.INDEXED));
        assertEquals(Set.of(3), coordination.idsIn(BookState.PENDING));
        assertEquals(Set.of(), coordination.idsIn(BookState.FAILED));
    }

    @Test
    void theInitializedMarkerSaysWhetherTheClusterKeptItsState() {
        assertFalse(coordination.isInitialized());
        coordination.markInitialized();
        assertTrue(coordination.isInitialized());
    }

    // --- Concurrency and crashes ----------------------------------------------------------

    @Test
    void twoReplicasNeverProcessTheSameBook() throws Exception {
        int books = 200;
        coordination.seed(IntStream.rangeClosed(1, books).boxed().toList());
        HazelcastInstance a = HazelcastTestCluster.settings().openClient();
        HazelcastInstance b = HazelcastTestCluster.settings().openClient();
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            Set<Integer> seen = ConcurrentHashMap.newKeySet();
            List<Future<List<Integer>>> workers = new ArrayList<>();
            for (int w = 0; w < 4; w++) {
                Coordination replica = Coordination.on(w % 2 == 0 ? a : b, HazelcastTestCluster.settings(), clock);
                String owner = "ingestion-" + (w % 2) + "/w" + w;
                workers.add(pool.submit(() -> {
                    List<Integer> done = new ArrayList<>();
                    Optional<Claim> claim;
                    while ((claim = replica.claim(Stage.INGESTION, owner, SHORT)).isPresent()) {
                        assertTrue(seen.add(claim.get().bookId()), "book claimed twice: " + claim.get().bookId());
                        assertTrue(replica.complete(claim.get()));
                        done.add(claim.get().bookId());
                    }
                    return done;
                }));
            }
            int total = 0;
            for (Future<List<Integer>> worker : workers) {
                List<Integer> done = worker.get(60, TimeUnit.SECONDS);
                assertFalse(done.isEmpty(), "every worker got some books");
                total += done.size();
            }
            assertEquals(books, total);
            assertEquals((long) books, coordination.countByState().get(BookState.DOWNLOADED));
            assertEquals(books, coordination.queueSize(Stage.INDEXING));
        } finally {
            pool.shutdownNow();
            a.shutdown();
            b.shutdown();
        }
    }

    /**
     * The guarantee claim() is built on: a process that dies after its poll and before its
     * commit loses nothing, the id goes back to the queue (contract §7, first row).
     */
    @Test
    void anIdPolledByAProcessThatDiesBeforeCommittingGoesBackToTheQueue() throws Exception {
        coordination.seed(List.of(5));
        HazelcastInstance dying = HazelcastTestCluster.settings().openClient();
        Thread worker = new Thread(() -> {
            TransactionContext tx = dying.newTransactionContext();
            tx.beginTransaction();
            tx.getQueue(CoordinationNames.INGEST_QUEUE).poll();
            dying.getLifecycleService().terminate();                 // dies without commit or rollback
        });
        worker.start();
        worker.join();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (coordination.queueSize(Stage.INGESTION) == 0 && System.nanoTime() < deadline) {
            Thread.sleep(200);
        }
        assertEquals(1, coordination.queueSize(Stage.INGESTION));
        assertEquals(BookState.PENDING, state(5));
        assertEquals(5, claim(Stage.INGESTION).bookId());
    }

    // --- Events and lock ------------------------------------------------------------------

    @Test
    void everySubscriberReceivesIndexPublished() throws Exception {
        HazelcastInstance other = HazelcastTestCluster.settings().openClient();
        try {
            Coordination search = Coordination.on(other, HazelcastTestCluster.settings(), clock);
            CountDownLatch got = new CountDownLatch(2);
            List<IndexPublished> received = new CopyOnWriteArrayList<>();   // filled by the listener thread
            search.subscribe(e -> {
                received.add(e);
                got.countDown();
            });
            coordination.subscribe(e -> got.countDown());
            IndexPublished event = new IndexPublished(43, 13, 200, "2026-11-03T10:15:00Z");
            coordination.publish(event);
            assertTrue(got.await(10, TimeUnit.SECONDS));
            assertEquals(List.of(event), received);
        } finally {
            other.shutdown();
        }
    }

    @Test
    void aListenerThatThrowsKeepsReceiving() throws Exception {
        CountDownLatch got = new CountDownLatch(2);
        coordination.subscribe(e -> {
            got.countDown();
            throw new IllegalStateException("listener bug");
        });
        coordination.publish(new IndexPublished(1, 1, 1, "2026-11-03T10:15:00Z"));
        coordination.publish(new IndexPublished(2, 1, 2, "2026-11-03T10:16:00Z"));
        assertTrue(got.await(10, TimeUnit.SECONDS));
    }

    @Test
    void anUnsubscribedListenerReceivesNothingMore() throws Exception {
        CountDownLatch got = new CountDownLatch(1);
        UUID registration = coordination.subscribe(e -> got.countDown());
        assertTrue(coordination.unsubscribe(registration));
        coordination.publish(new IndexPublished(1, 1, 1, "2026-11-03T10:15:00Z"));
        assertFalse(got.await(2, TimeUnit.SECONDS));
        assertFalse(coordination.unsubscribe(registration));          // already removed
    }

    /**
     * What the index writer relies on (contract §5): false when the client loses the cluster,
     * true when it is back. A cluster of its own, stopped and started again.
     */
    @Test
    void theConnectionListenerSeesTheClusterGoAndComeBack() throws Exception {
        String name = "connection-test-" + UUID.randomUUID();
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        HazelcastInstance member = HazelcastTestCluster.startStandaloneMember(name, port);
        CoordinationSettings settings = new CoordinationSettings(List.of("127.0.0.1:" + port), name, 60, 3, 10);
        HazelcastInstance own = settings.openClient();
        BlockingQueue<Boolean> changes = new LinkedBlockingQueue<>();
        try {
            Coordination.on(own, settings, clock).onConnectionChange(changes::add);
            member.shutdown();
            assertEquals(Boolean.FALSE, changes.poll(30, TimeUnit.SECONDS), "no CLIENT_DISCONNECTED");
            member = HazelcastTestCluster.startStandaloneMember(name, port);
            assertEquals(Boolean.TRUE, changes.poll(60, TimeUnit.SECONDS), "no CLIENT_CONNECTED");
        } finally {
            own.shutdown();
            member.shutdown();
        }
    }

    @Test
    void theIndexWriterLockHasOneHolderAtATime() throws Exception {
        HazelcastInstance other = HazelcastTestCluster.settings().openClient();
        try {
            IndexWriterLock mine = coordination.indexWriterLock();
            IndexWriterLock theirs = Coordination.on(other, HazelcastTestCluster.settings(), clock).indexWriterLock();
            mine.lock();
            assertTrue(theirs.isLocked());
            assertFalse(callInOtherThread(() -> theirs.tryLock(Duration.ofMillis(200))));
            mine.unlock();
            assertTrue(callInOtherThread(() -> {
                boolean taken = theirs.tryLock(Duration.ofSeconds(5));
                theirs.unlock();
                return taken;
            }));
        } finally {
            other.shutdown();
        }
    }

    @Test
    void anOwnerNamesTheServiceTheHostAndTheWorker() {
        assertTrue(Coordination.owner("ingestion", 2).matches("ingestion-.+/w2"));
    }

    // --- Helpers --------------------------------------------------------------------------

    private Claim claim(Stage stage) throws InterruptedException {
        return coordination.claim(stage, "ingestion-test/w0", SHORT).orElseThrow();
    }

    private BookState state(int bookId) {
        return coordination.task(bookId).orElseThrow().bookState();
    }

    private static <T> T callInOtherThread(Callable<T> task) throws Exception {
        ExecutorService thread = Executors.newSingleThreadExecutor();
        try {
            return thread.submit(task).get(30, TimeUnit.SECONDS);
        } finally {
            thread.shutdownNow();
        }
    }

    /** A clock the test moves by hand. */
    private static final class TestClock extends Clock {
        private final AtomicLong millis = new AtomicLong(Instant.parse("2026-11-04T10:00:00Z").toEpochMilli());

        void advance(Duration d) {
            millis.addAndGet(d.toMillis());
        }

        @Override
        public long millis() {
            return millis.get();
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis());
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }
}
