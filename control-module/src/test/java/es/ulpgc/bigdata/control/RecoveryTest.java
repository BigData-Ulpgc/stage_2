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

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The recovery of a cluster that lost its state (contract §7), on the embedded cluster. */
class RecoveryTest {

    private static final Duration SHORT = Duration.ofMillis(200);

    private static HazelcastInstance client;

    private final MutableClock clock = new MutableClock();
    private final FakeEvidence evidence = new FakeEvidence();
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
        HazelcastTestCluster.reset();                   // what a restarted cluster looks like
        coordination = Coordination.on(client, HazelcastTestCluster.settings(), clock);
    }

    @Test
    void eachBookGetsTheMostAdvancedStateOfItsEvidence() throws Exception {
        // 1 is everywhere, 2 indexed and downloaded, 3 only downloaded, 4 only in the seed list
        evidence.published = Set.of(1);
        evidence.indexed = Set.of(1, 2);
        evidence.downloaded = List.of(1, 2, 3);
        Recovery recovery = new Recovery(coordination, evidence, List.of(1, 2, 3, 4), clock);

        Recovery.Outcome outcome = recovery.ensureInitialized().orElseThrow();

        assertEquals(new Recovery.Outcome(1, 1, 1, 1, 0), outcome);
        assertEquals(BookState.PUBLISHED, state(1));
        assertEquals(BookState.INDEXED, state(2));
        assertEquals(BookState.DOWNLOADED, state(3));
        assertEquals(BookState.PENDING, state(4));
        // Each queue holds exactly the books that must be worked on again
        assertEquals(3, coordination.claim(Stage.INDEXING, "indexing-test/w0", SHORT).orElseThrow().bookId());
        assertEquals(4, coordination.claim(Stage.INGESTION, "ingestion-test/w0", SHORT).orElseThrow().bookId());
        assertEquals(0, coordination.queueSize(Stage.INDEXING) + coordination.queueSize(Stage.INGESTION));
        assertTrue(coordination.isInitialized());
    }

    @Test
    void aClusterThatKeptItsStateIsNotTouched() {
        coordination.markInitialized();
        evidence.published = Set.of(1);
        Recovery recovery = new Recovery(coordination, evidence, List.of(5), clock);

        assertTrue(recovery.ensureInitialized().isEmpty());
        assertEquals(0, evidence.reads);
        assertTrue(coordination.task(1).isEmpty());
        assertTrue(coordination.task(5).isEmpty());
    }

    @Test
    void itRunsOnceUntilTheStateIsLostAgain() {
        evidence.indexed = Set.of(2);
        Recovery recovery = new Recovery(coordination, evidence, List.of(), clock);
        assertTrue(recovery.ensureInitialized().isPresent());
        assertTrue(recovery.ensureInitialized().isEmpty());

        HazelcastTestCluster.reset();                   // the cluster restarts again
        assertTrue(recovery.ensureInitialized().isPresent());
        assertEquals(BookState.INDEXED, state(2));
        assertEquals(2L, recovery.status().get("runs"));
    }

    /** MongoDB down: nothing is written and the marker stays missing, so the next check retries. */
    @Test
    void aFailedReadWritesNothingAndIsRetried() {
        evidence.published = Set.of(1);
        evidence.indexed = Set.of(2);
        evidence.indexedFails = true;
        Recovery recovery = new Recovery(coordination, evidence, List.of(4), clock);

        assertThrows(IllegalStateException.class, recovery::ensureInitialized);
        assertTrue(coordination.task(1).isEmpty(), "published() was read but nothing was written");
        assertTrue(coordination.task(4).isEmpty());
        assertFalse(coordination.isInitialized());
        assertEquals("java.lang.IllegalStateException: MongoDB is down", recovery.status().get("last_error"));

        evidence.indexedFails = false;
        assertTrue(recovery.ensureInitialized().isPresent());
        assertEquals(BookState.INDEXED, state(2));
        assertNull(recovery.status().get("last_error"));
    }

    @Test
    void aBookSeededDuringTheRecoveryMovesForward() {
        coordination.seed(List.of(2));                  // POST /control/jobs right after the restart
        evidence.indexed = Set.of(2);
        new Recovery(coordination, evidence, List.of(), clock).ensureInitialized();
        assertEquals(BookState.INDEXED, state(2));
    }

    @Test
    void idsThatAreNotBookIdsAreIgnored() {
        evidence.indexed = Set.of(2, 0, -5);
        Recovery.Outcome outcome = new Recovery(coordination, evidence, List.of(), clock).ensureInitialized().orElseThrow();
        assertEquals(1, outcome.indexed());
        assertEquals(BookState.INDEXED, state(2));
    }

    @Test
    void theStatusReportsTheLastRun() {
        Map<String, Object> before = new Recovery(coordination, evidence, List.of(), clock).status();
        assertEquals(0L, before.get("runs"));
        assertNull(before.get("last_run"));

        evidence.downloaded = List.of(3);
        Recovery recovery = new Recovery(coordination, evidence, List.of(3, 4), clock);
        recovery.ensureInitialized();
        Map<String, Object> after = recovery.status();
        assertEquals(1L, after.get("runs"));
        assertEquals(clock.instant().toString(), after.get("last_run"));
        assertEquals(1, after.get("downloaded"));
        assertEquals(1, after.get("seeded"), "3 was already DOWNLOADED");
    }

    private BookState state(int bookId) {
        return coordination.task(bookId).orElseThrow().bookState();
    }

    private static final class FakeEvidence implements Recovery.Evidence {

        Set<Integer> published = Set.of();
        Set<Integer> indexed = Set.of();
        Collection<Integer> downloaded = List.of();
        boolean indexedFails;
        int reads;

        @Override
        public Set<Integer> published() {
            reads++;
            return published;
        }

        @Override
        public Set<Integer> indexed() {
            reads++;
            if (indexedFails) {
                throw new IllegalStateException("MongoDB is down");
            }
            return indexed;
        }

        @Override
        public Collection<Integer> downloaded() {
            reads++;
            return downloaded;
        }
    }
}
