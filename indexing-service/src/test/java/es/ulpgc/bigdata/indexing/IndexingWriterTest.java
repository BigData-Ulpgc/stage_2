package es.ulpgc.bigdata.indexing;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.mongodb.MongoException;
import es.ulpgc.bigdata.coordination.BookState;
import es.ulpgc.bigdata.coordination.BookTask;
import es.ulpgc.bigdata.coordination.Claim;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.CoordinationSettings;
import es.ulpgc.bigdata.coordination.Stage;
import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.datamart.index.Tokenizer;
import es.ulpgc.bigdata.datamart.metadata.MetadataParser;
import es.ulpgc.bigdata.model.RawBook;
import es.ulpgc.bigdata.testsupport.HazelcastTestCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The consumer of index-queue (DR-06b) against a real Hazelcast cluster embedded in the JVM
 * (common's HazelcastTestCluster, two members): the books reach DOWNLOADED through the real
 * transitions, and IndexingService runs on the in-memory stores. The waits of the writer are recorded and shortened to 10 ms.
 */
class IndexingWriterTest {

    /** Claims and lock attempts of 200 ms: a writer stops quickly at the end of each test. */
    private static final Duration STEP = Duration.ofMillis(200);

    @TempDir Path dataDir;

    private static HazelcastInstance client;

    private Coordination coordination;
    private RangeBasedDatalake datalake;
    private InMemoryMetadataRepository metadata;
    private IndexingServiceTest.SpyIndex index;
    private InMemoryIndexLog indexLog;
    private IndexingService service;
    private final List<Duration> sleeps = new CopyOnWriteArrayList<>();
    private final List<Running> running = new ArrayList<>();

    private record Running(IndexingWriter writer, Thread thread) {
    }

    @BeforeAll
    static void connect() {
        client = HazelcastTestCluster.settings().openClient();
    }

    @AfterAll
    static void disconnect() {
        client.shutdown();
    }

    @BeforeEach
    void setUp() {
        HazelcastTestCluster.reset();
        coordination = Coordination.on(client, HazelcastTestCluster.settings(), Clock.systemUTC());
        datalake = new RangeBasedDatalake(dataDir.resolve("datalake/range"));
        metadata = new InMemoryMetadataRepository();
        index = new IndexingServiceTest.SpyIndex();
        indexLog = new InMemoryIndexLog();
        service = new IndexingService(datalake, metadata, index, indexLog, new InMemoryPublicationRepository(indexLog),
                new MetadataParser(), Tokenizer.standard());
    }

    @AfterEach
    void stopWriters() throws InterruptedException {
        for (Running r : running) {
            r.writer().stop();
        }
        for (Running r : running) {
            r.thread().join(10_000);
            assertFalse(r.thread().isAlive(), "el escritor no se paró");
        }
    }

    private IndexingWriter start(Coordination on, String owner) {
        IndexingWriter writer = new IndexingWriter(on, service, owner, d -> {
            sleeps.add(d);
            Thread.sleep(10);
        }, STEP);
        Thread thread = new Thread(writer, "test-writer-" + owner);
        thread.start();
        running.add(new Running(writer, thread));
        return writer;
    }

    private void inDatalake(int... ids) {
        for (int id : ids) {
            datalake.save(new RawBook(id, IndexingServiceTest.HEADER_1342, IndexingServiceTest.BODY_1342));
        }
    }

    /** PENDING -> INGESTING -> DOWNLOADED, offered to index-queue, as an Ingestion worker does. */
    private void downloaded(Integer... ids) throws InterruptedException {
        coordination.seed(List.of(ids));
        for (int i = 0; i < ids.length; i++) {
            Claim claim = coordination.claim(Stage.INGESTION, "ingestion-test/w0", Duration.ofSeconds(5)).orElseThrow();
            assertTrue(coordination.complete(claim));
        }
    }

    private BookTask task(int id) {
        return coordination.task(id).orElseThrow();
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        for (int i = 0; i < 500 && !condition.getAsBoolean(); i++) {
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean(), "no ocurrió: " + what);
    }

    // ---- the happy path ----

    @Test
    void indexaLosLibrosDeLaColaYLosDejaIndexed() throws Exception {
        inDatalake(1, 2);
        downloaded(1, 2);

        IndexingWriter writer = start(coordination, "w-a");

        await("1 y 2 INDEXED", () -> task(1).bookState() == BookState.INDEXED && task(2).bookState() == BookState.INDEXED);
        assertTrue(indexLog.contains(1) && indexLog.contains(2), "T7 solo con el libro en index_log");
        assertEquals(IndexingWriter.Role.ACTIVE, writer.role());
        assertEquals(2L, writer.status().get("indexed"));
        assertNull(task(1).owner(), "T7 libera el lease");
    }

    // ---- errors (contract §4.3) ----

    @Test
    void unLibroSinReadyVuelveALaColaHastaAgotarLosIntentos() throws Exception {
        downloaded(5);                                     // DOWNLOADED, but not in the datalake

        IndexingWriter writer = start(coordination, "w-a");

        await("5 FAILED", () -> task(5).bookState() == BookState.FAILED);
        assertEquals("INDEXING", task(5).failedStage());
        assertEquals(3, task(5).attempts());
        assertTrue(task(5).lastError().startsWith("NOT_READY"), task(5).lastError());
        assertEquals(2L, writer.status().get("requeued"));
        assertEquals(1L, writer.status().get("failed"));
    }

    @Test
    void unLibroSinTerminosFallaDeUnaVez() throws Exception {
        datalake.save(new RawBook(6, IndexingServiceTest.HEADER_1342, "The and of ... !!!"));
        downloaded(6);

        start(coordination, "w-a");

        await("6 FAILED", () -> task(6).bookState() == BookState.FAILED);
        assertEquals(1, task(6).attempts());
        assertTrue(task(6).lastError().startsWith("EMPTY_BOOK"), task(6).lastError());
    }

    @Test
    void conMongoCaidoNoGastaIntentosEsperaCadaVezMasYDespuesIndexa() throws Exception {
        inDatalake(7);
        indexLog.failing(true);                            // the last write of update fails
        downloaded(7);

        IndexingWriter writer = start(coordination, "w-a");
        await("tres pausas", () -> ((Long) writer.status().get("outage_pauses")) >= 3);
        assertEquals(0, task(7).attempts(), "una caída de MongoDB no es culpa del libro");
        assertTrue(task(7).lastError().startsWith("STORAGE_UNAVAILABLE"), task(7).lastError());
        indexLog.failing(false);

        await("7 INDEXED", () -> task(7).bookState() == BookState.INDEXED);
        List<Duration> outage = sleeps.stream().filter(d -> d.compareTo(IndexingWriter.MIN_BACKOFF) >= 0).toList();
        assertEquals(List.of(Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(4)), outage.subList(0, 3));
    }

    @Test
    void trasUnLibroBuenoLaEsperaVuelveAEmpezarEnUnSegundo() throws Exception {
        inDatalake(8, 9);
        indexLog.failing(true);
        downloaded(8);
        IndexingWriter writer = start(coordination, "w-a");
        await("dos pausas", () -> ((Long) writer.status().get("outage_pauses")) >= 2);
        indexLog.failing(false);
        await("8 INDEXED", () -> task(8).bookState() == BookState.INDEXED);
        sleeps.clear();

        indexLog.failing(true);
        downloaded(9);
        await("otra pausa", () -> sleeps.contains(IndexingWriter.MIN_BACKOFF));

        assertEquals(IndexingWriter.MIN_BACKOFF, sleeps.stream()
                .filter(d -> d.compareTo(IndexingWriter.MIN_BACKOFF) >= 0).findFirst().orElseThrow());
        indexLog.failing(false);
        await("9 INDEXED", () -> task(9).bookState() == BookState.INDEXED);
    }

    @Test
    void unErrorInesperadoEsInternalErrorYGastaUnIntento() throws Exception {
        inDatalake(10);
        index.failNextFlushWith = new IllegalStateException("fallo inesperado (simulado)");
        downloaded(10);

        IndexingWriter writer = start(coordination, "w-a");

        await("10 INDEXED en el segundo intento", () -> task(10).bookState() == BookState.INDEXED);
        assertEquals(1L, writer.status().get("requeued"));
        assertEquals(0L, writer.status().get("outage_pauses"));
        assertTrue(task(10).lastError().startsWith("INTERNAL_ERROR: "), task(10).lastError());
    }

    // ---- Hazelcast failing ----

    @Test
    void siHazelcastFallaEsperaCadaVezMasYCuandoVuelveEmpiezaOtraVezEnUnSegundo() throws Exception {
        // A member of its own (stopping the shared one would break the other tests) and a client
        // that fails at once while offline (ASYNC). The writer is NOT told of the disconnection,
        // so its calls to Hazelcast throw: the case of a failure the listener has not seen.
        String cluster = "indexing-writer-outage-" + UUID.randomUUID();
        int port = 5961;
        HazelcastInstance member = HazelcastTestCluster.startStandaloneMember(cluster, port);
        CoordinationSettings settings = new CoordinationSettings(List.of("127.0.0.1:" + port), cluster, 120, 3, 10);
        ClientConfig config = new ClientConfig().setClusterName(cluster);
        config.getNetworkConfig().addAddress("127.0.0.1:" + port);
        config.getConnectionStrategyConfig()
                .setReconnectMode(ClientConnectionStrategyConfig.ReconnectMode.ASYNC)
                .getConnectionRetryConfig().setInitialBackoffMillis(100).setMaxBackoffMillis(200)
                .setClusterConnectTimeoutMillis(-1);
        HazelcastInstance ownClient = HazelcastClient.newHazelcastClient(config);
        try {
            IndexingWriter writer = start(Coordination.on(ownClient, settings, Clock.systemUTC()), "w-a");
            await("activo", () -> writer.role() == IndexingWriter.Role.ACTIVE);

            member.shutdown();
            await("tres esperas", () -> backoffs().size() >= 3);
            assertEquals(List.of(Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(4)),
                    backoffs().subList(0, 3));

            member = HazelcastTestCluster.startStandaloneMember(cluster, port);   // back, on the same port
            await("vuelve a tener el lock", () -> writer.role() == IndexingWriter.Role.ACTIVE);
            sleeps.clear();
            member.shutdown();

            await("otra espera", () -> !backoffs().isEmpty());
            assertEquals(Duration.ofSeconds(1), backoffs().get(0));
        } finally {
            ownClient.shutdown();
            member.shutdown();
        }
    }

    /** The waits after a failure (1 s or more): the steps of 200 ms are left out. */
    private List<Duration> backoffs() {
        return sleeps.stream().filter(d -> d.compareTo(IndexingWriter.MIN_BACKOFF) >= 0).toList();
    }

    // ---- leases ----

    @Test
    void siPierdeElLeaseDescartaElLibroYOtroIntentoLoIndexa() throws Exception {
        inDatalake(11);
        CountDownLatch gate = new CountDownLatch(1);
        index.flushGate = gate;                            // the writer stops inside update
        downloaded(11);
        IndexingWriter writer = start(coordination, "w-a");
        assertTrue(index.flushEntered.await(5, TimeUnit.SECONDS));

        // The Control, 10 minutes later, sees the lease expired and requeues the book (T8).
        Coordination control = Coordination.on(client, HazelcastTestCluster.settings(),
                Clock.fixed(Instant.now().plus(Duration.ofMinutes(10)), ZoneOffset.UTC));
        assertEquals(1, control.requeueExpiredLeases());
        index.flushGate = null;                            // the next attempt does not stop
        gate.countDown();                                  // the first one finishes, too late

        await("11 INDEXED tras volver a la cola", () -> task(11).bookState() == BookState.INDEXED);
        assertEquals(1L, writer.status().get("lease_lost"));
        assertEquals(1L, writer.status().get("indexed"));
    }

    // ---- one writer at a time (contract §5) ----

    @Test
    void conDosEscritoresSoloUnoIndexaYElOtroTomaElRelevo() throws Exception {
        HazelcastInstance otherClient = HazelcastTestCluster.settings().openClient();
        try {
            Coordination other = Coordination.on(otherClient, HazelcastTestCluster.settings(), Clock.systemUTC());
            inDatalake(20, 21, 22, 23);
            IndexingWriter a = start(coordination, "w-a");
            await("a activo", () -> a.role() == IndexingWriter.Role.ACTIVE);
            IndexingWriter b = start(other, "w-b");

            downloaded(20, 21);
            await("20 y 21 INDEXED", () -> task(20).bookState() == BookState.INDEXED
                    && task(21).bookState() == BookState.INDEXED);
            assertEquals(2L, a.status().get("indexed"));
            assertEquals(0L, b.status().get("indexed"));
            assertEquals(IndexingWriter.Role.STANDBY, b.role());

            a.stop();
            await("b toma el lock", () -> b.role() == IndexingWriter.Role.ACTIVE);
            downloaded(22, 23);

            await("22 y 23 INDEXED", () -> task(22).bookState() == BookState.INDEXED
                    && task(23).bookState() == BookState.INDEXED);
            assertEquals(2L, b.status().get("indexed"));
            assertEquals(2L, a.status().get("indexed"));
            assertEquals(1, index.maxWritersAtOnce.get(), "nunca dos escritores a la vez");
        } finally {
            for (Running r : running) {
                r.writer().stop();
                r.thread().join(10_000);
            }
            otherClient.shutdown();
        }
    }

    @Test
    void sinClusterDejaDeReclamarSueltaElLockYAlVolverSigue() throws Exception {
        inDatalake(30);
        IndexingWriter writer = start(coordination, "w-a");
        await("activo", () -> writer.role() == IndexingWriter.Role.ACTIVE);

        writer.connectionChanged(false);
        await("en espera", () -> writer.role() == IndexingWriter.Role.STANDBY);
        downloaded(30);
        Thread.sleep(300);
        assertEquals(BookState.DOWNLOADED, task(30).bookState(), "sin cluster no reclama");
        // Without a cluster it does not even release the lock: it would wait for the cluster.
        assertTrue(coordination.indexWriterLock().isLocked(), "sin cluster no llama a Hazelcast");

        writer.connectionChanged(true);
        await("30 INDEXED", () -> task(30).bookState() == BookState.INDEXED);

        // The pending release came before taking it again: once stopped, the lock is free
        // (taken twice and released once, it would stay held and no replica could take over).
        writer.stop();
        running.get(0).thread().join(5_000);
        assertFalse(coordination.indexWriterLock().isLocked(), "el lock quedó tomado al parar");
    }

    @Test
    void enEsperaSeParaSinTenerElLock() throws Exception {
        coordination.indexWriterLock().lock();             // another replica holds it (this thread)
        try {
            IndexingWriter writer = start(coordination, "w-b");
            Thread.sleep(300);
            assertEquals(IndexingWriter.Role.STANDBY, writer.role());

            writer.stop();
            Running r = running.get(0);
            r.thread().join(STEP.multipliedBy(5).toMillis());
            assertFalse(r.thread().isAlive());
            assertEquals(IndexingWriter.Role.STOPPED, writer.role());
        } finally {
            coordination.indexWriterLock().unlock();
        }
    }

    @Test
    void elEstadoLlevaLosContadoresDelContrato() {
        IndexingWriter writer = new IndexingWriter(coordination, service, "w-a", d -> { });

        assertEquals(List.of("role", "indexed", "requeued", "failed", "lease_lost", "outage_pauses",
                "invalid_ids_dropped", "ids_skipped"), List.copyOf(writer.status().keySet()));
        assertEquals("standby", writer.status().get("role"));
    }
}
