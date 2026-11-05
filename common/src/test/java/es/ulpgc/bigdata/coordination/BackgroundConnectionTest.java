package es.ulpgc.bigdata.coordination;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.core.HazelcastInstance;
import es.ulpgc.bigdata.testsupport.HazelcastTestCluster;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BackgroundConnectionTest {

    /** A connection that returns when the test says so, over a client of the test cluster. */
    private static final class GatedConnector {
        final CountDownLatch release = new CountDownLatch(1);

        Coordination connect() {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // connect() owns its client: close() of the Coordination shuts it down
            return Coordination.connect(HazelcastTestCluster.settings());
        }
    }

    @Test
    void itIsConnectingUntilTheClusterAnswersThenConnected() throws Exception {
        GatedConnector gate = new GatedConnector();
        AtomicReference<Coordination> given = new AtomicReference<>();
        try (BackgroundConnection connection = BackgroundConnection.start(gate::connect, given::set)) {
            assertEquals(BackgroundConnection.State.CONNECTING, connection.state());
            assertTrue(connection.coordination().isEmpty());
            gate.release.countDown();
            await(() -> given.get() != null);
            assertEquals(BackgroundConnection.State.CONNECTED, connection.state());
            assertSame(given.get(), connection.coordination().orElseThrow());
            assertEquals("connected", connection.state().label());
        }
    }

    @Test
    void aConnectionThatCannotBeAttemptedIsFailedNotConnectingForever() throws Exception {
        try (BackgroundConnection connection = BackgroundConnection.start(() -> {
            throw new IllegalArgumentException("HZ_MEMBERS is malformed");
        }, c -> { })) {
            await(() -> connection.state() == BackgroundConnection.State.FAILED);
            assertTrue(connection.failure().orElseThrow().contains("HZ_MEMBERS is malformed"));
        }
    }

    @Test
    void closingWhileConnectingClosesTheClientWhenItConnects() throws Exception {
        GatedConnector gate = new GatedConnector();
        AtomicReference<Coordination> created = new AtomicReference<>();
        CountDownLatch connectorReturned = new CountDownLatch(1);
        BackgroundConnection connection = BackgroundConnection.start(() -> {
            Coordination c = gate.connect();
            created.set(c);
            connectorReturned.countDown();
            return c;
        }, c -> created.set(null));                                     // onConnected must not run
        connection.close();
        assertEquals(BackgroundConnection.State.CLOSED, connection.state());
        gate.release.countDown();
        assertTrue(connectorReturned.await(30, TimeUnit.SECONDS));
        Coordination c = created.get();
        await(() -> c != null && isShutDown(c));
        assertTrue(connection.coordination().isEmpty());
        assertEquals(BackgroundConnection.State.CLOSED, connection.state());
    }

    @Test
    void closeShutsDownTheClientAndASecondCloseDoesNothing() throws Exception {
        AtomicReference<Coordination> given = new AtomicReference<>();
        BackgroundConnection connection = BackgroundConnection.start(HazelcastTestCluster.settings(), given::set);
        await(() -> given.get() != null);
        assertFalse(isShutDown(given.get()));
        connection.close();
        connection.close();
        assertTrue(isShutDown(given.get()));
        assertEquals(BackgroundConnection.State.CLOSED, connection.state());
    }

    @Test
    void aFailingOnConnectedIsReportedAndTheConnectionStays() throws Exception {
        try (BackgroundConnection connection = BackgroundConnection.start(HazelcastTestCluster.settings(), c -> {
            throw new IllegalStateException("workers could not start");
        })) {
            await(() -> connection.failure().isPresent());
            assertTrue(connection.failure().orElseThrow().contains("workers could not start"));
            assertEquals(BackgroundConnection.State.CONNECTED, connection.state());
        }
    }

    /** What GET /status shows while the cluster goes away and comes back. */
    @Test
    void itFollowsTheClusterGoingAwayAndComingBack() throws Exception {
        String name = "background-test-" + UUID.randomUUID();
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        HazelcastInstance member = HazelcastTestCluster.startStandaloneMember(name, port);
        CoordinationSettings settings = new CoordinationSettings(List.of("127.0.0.1:" + port), name, 60, 3, 10);
        try (BackgroundConnection connection = BackgroundConnection.start(settings, c -> { })) {
            await(() -> connection.state() == BackgroundConnection.State.CONNECTED);
            member.shutdown();
            await(() -> connection.state() == BackgroundConnection.State.DISCONNECTED);
            member = HazelcastTestCluster.startStandaloneMember(name, port);
            await(() -> connection.state() == BackgroundConnection.State.CONNECTED);
        } finally {
            member.shutdown();
        }
    }

    /** Whether the client of a Coordination was shut down: using it then throws. */
    private static boolean isShutDown(Coordination c) {
        try {
            c.queueSize(Stage.INGESTION);
            return false;
        } catch (HazelcastClientNotActiveException e) {
            return true;
        }
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within 60 s");
            }
            Thread.sleep(50);
        }
    }
}
