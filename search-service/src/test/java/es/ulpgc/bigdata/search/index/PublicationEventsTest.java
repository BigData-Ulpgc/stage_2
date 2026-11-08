package es.ulpgc.bigdata.search.index;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.CoordinationSettings;
import es.ulpgc.bigdata.coordination.IndexPublished;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The events of a real (embedded, single-member) Hazelcast cluster reaching Search. */
class PublicationEventsTest {

    private static final IndexPublished EVENT = new IndexPublished(3, 1, 9, "2026-11-04T10:15:00Z");
    private static final long RECONNECTION_MILLIS = 8_000;

    private HazelcastInstance member;
    private Coordination coordination;

    @BeforeEach
    void setUp() {
        String cluster = "search-test-" + UUID.randomUUID();
        Config config = new Config().setClusterName(cluster);
        config.setProperty("hazelcast.logging.type", "none");
        config.getNetworkConfig().setPort(5951).setPortAutoIncrement(true);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        member = Hazelcast.newHazelcastInstance(config);
        coordination = Coordination.on(member,
                new CoordinationSettings(List.of("unused"), cluster, 120, 3, 10), Clock.systemUTC());
    }

    @AfterEach
    void tearDown() {
        member.shutdown();
    }

    /**
     * The subscription happens in the background and the topic only delivers what is published
     * after subscribing, so it publishes until the expected deliveries arrive.
     */
    private void publishUntil(CountDownLatch deliveries) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (deliveries.getCount() > 0 && System.nanoTime() < deadline) {
            coordination.publish(EVENT);
            deliveries.await(50, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void anEventWakesTheReplica() throws Exception {
        CountDownLatch woken = new CountDownLatch(1);
        try (PublicationEvents events = new PublicationEvents(() -> coordination, woken::countDown)) {
            events.start();
            publishUntil(woken);
        }
        assertEquals(0, woken.getCount(), "the event did not arrive");
    }

    @Test
    void aListenerThatFailsKeepsReceivingEvents() throws Exception {
        CountDownLatch deliveries = new CountDownLatch(2);
        try (PublicationEvents events = new PublicationEvents(() -> coordination, () -> {
            deliveries.countDown();
            throw new IllegalStateException("simulated failure of the listener");
        })) {
            events.start();
            publishUntil(deliveries);
        }
        assertEquals(0, deliveries.getCount(), "the subscription stopped after a failure");
    }

    @Test
    void startReturnsWhileTheClusterIsDownAndCloseDoesNotWaitForIt() {
        CountDownLatch neverAnswers = new CountDownLatch(1);
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            PublicationEvents events = new PublicationEvents(() -> {
                try {
                    neverAnswers.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return coordination;
            }, () -> { });
            events.start();
            events.close();
        });
        neverAnswers.countDown();
    }

    @Test
    void aFailedConnectionDoesNotStopTheService() {
        try (PublicationEvents events = new PublicationEvents(() -> {
            throw new IllegalStateException("no cluster");
        }, () -> { })) {
            assertDoesNotThrow(events::start);
        }
    }

    /**
     * Both members die and a new one takes their place: the same client reconnects to a cluster
     * that remembers nothing, its ringbuffer included. One event per version, as Indexing does:
     * publishing several would hide a reader that is waiting at a sequence ahead of the new
     * ringbuffer.
     */
    @Test
    void oneEventArrivesAfterTheClusterRestarts() throws Exception {
        String cluster = "search-restart-" + UUID.randomUUID();
        CoordinationSettings settings = new CoordinationSettings(List.of("127.0.0.1:5991"), cluster, 120, 3, 10);
        HazelcastInstance restarted = memberOn(cluster);
        AtomicInteger deliveries = new AtomicInteger();
        try (PublicationEvents events = new PublicationEvents(() -> Coordination.connect(settings), deliveries::incrementAndGet)) {
            events.start();
            publishUntilDelivered(restarted, settings, deliveries);
            publishOnce(restarted, settings, deliveries, "before the restarts");
            for (int restart = 1; restart <= 3; restart++) {
                int beforeRestart = deliveries.get();
                restarted.getLifecycleService().terminate();
                restarted = memberOn(cluster);
                Thread.sleep(RECONNECTION_MILLIS);
                // Nothing was published: the one call is the check of CURRENT after reconnecting
                assertTrue(deliveries.get() > beforeRestart, "no check of CURRENT after restart " + restart);
                publishOnce(restarted, settings, deliveries, "after restart " + restart);
                publishOnce(restarted, settings, deliveries, "again after restart " + restart);
            }
        } finally {
            restarted.getLifecycleService().terminate();
        }
    }

    private static HazelcastInstance memberOn(String cluster) {
        Config config = new Config().setClusterName(cluster);
        config.setProperty("hazelcast.logging.type", "none");
        config.getNetworkConfig().setPort(5991).setPortAutoIncrement(false);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        return Hazelcast.newHazelcastInstance(config);
    }

    /** The first subscription happens in the background: publishes until it is in place. */
    private static void publishUntilDelivered(HazelcastInstance member, CoordinationSettings settings,
                                              AtomicInteger deliveries) throws InterruptedException {
        Coordination publisher = Coordination.on(member, settings, Clock.systemUTC());
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (deliveries.get() == 0 && System.nanoTime() < deadline) {
            publisher.publish(EVENT);
            Thread.sleep(200);
        }
        assertTrue(deliveries.get() > 0, "the first event did not arrive");
        Thread.sleep(500);
    }

    private static void publishOnce(HazelcastInstance member, CoordinationSettings settings,
                                    AtomicInteger deliveries, String when) throws InterruptedException {
        int before = deliveries.get();
        Coordination.on(member, settings, Clock.systemUTC()).publish(EVENT);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (deliveries.get() == before && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        assertTrue(deliveries.get() > before, "the event did not arrive " + when);
    }
}
