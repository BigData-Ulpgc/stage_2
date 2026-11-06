package es.ulpgc.bigdata.indexing;

import com.hazelcast.core.HazelcastInstance;
import com.mongodb.MongoException;
import es.ulpgc.bigdata.coordination.BookState;
import es.ulpgc.bigdata.coordination.Claim;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.CoordinationSettings;
import es.ulpgc.bigdata.coordination.IndexPublished;
import es.ulpgc.bigdata.coordination.Stage;
import es.ulpgc.bigdata.testsupport.HazelcastTestCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * T10 and IndexPublished (DR-08b) against a real Hazelcast cluster embedded in the JVM (common's
 * HazelcastTestCluster, two members): the books reach INDEXED through the same transitions as
 * in production (seed, claim, complete). index_books is the in-memory fake.
 *
 * The notifier works in a thread of its own: the tests wait for it with awaitQueued.
 */
class HazelcastPublicationNotifierTest {

    private static final String AT = "2026-11-05T12:00:00Z";

    private static HazelcastInstance client;

    private Coordination coordination;
    private InMemoryPublicationRepository publications;
    private HazelcastPublicationNotifier notifier;
    private final BlockingQueue<IndexPublished> events = new LinkedBlockingQueue<>();
    private UUID subscription;

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
        coordination = Coordination.on(client, HazelcastTestCluster.settings(), Clock.systemUTC());
        publications = new InMemoryPublicationRepository();
        notifier = new HazelcastPublicationNotifier(publications);
        subscription = coordination.subscribe(events::add);
    }

    @AfterEach
    void tearDown() {
        notifier.close();
        coordination.unsubscribe(subscription);
    }

    private void awaitNotifier() throws Exception {
        notifier.awaitQueued(Duration.ofSeconds(5));
    }

    /** The books go PENDING -> ... -> INDEXED, as the workers of Ingestion and Indexing do. */
    private void indexedInBookState(Integer... ids) throws InterruptedException {
        coordination.seed(List.of(ids));
        for (Stage stage : List.of(Stage.INGESTION, Stage.INDEXING)) {
            for (int i = 0; i < ids.length; i++) {
                Claim claim = coordination.claim(stage, "test/w1", Duration.ofSeconds(5)).orElseThrow();
                assertTrue(coordination.complete(claim));
            }
        }
    }

    private BookState stateOf(int id) {
        return coordination.task(id).orElseThrow().bookState();
    }

    private void published(int version, Integer... ids) {
        publications.addBooks(List.of(ids), version);
        publications.setCurrent(new Publication(version, publications.countVisible(version), "inverted_index", AT));
    }

    @Test
    void antesDeConectarNoNotificaNadaYDiceConnecting() throws Exception {
        indexedInBookState(1);
        published(1, 1);

        notifier.afterPublish(new PublishResult(1, 1, 1, AT));

        notifier.afterPublish(new PublishResult(1, 0, 1, AT));     // nothing new: not counted

        assertEquals("connecting", notifier.state());
        assertEquals(1, notifier.skipped());
        assertEquals(BookState.INDEXED, stateOf(1));
        assertNull(events.poll(300, TimeUnit.MILLISECONDS));
    }

    @Test
    void trasUnaPublicacionLosIndexadosVisiblesPasanAPublishedYSeEmiteElEvento() throws Exception {
        notifier.connected(coordination);
        indexedInBookState(1, 2, 3);
        published(1, 1, 2);                              // 3 is indexed but not published yet

        notifier.afterPublish(new PublishResult(1, 2, 2, AT));
        awaitNotifier();

        assertEquals(BookState.PUBLISHED, stateOf(1));
        assertEquals(BookState.PUBLISHED, stateOf(2));
        assertEquals(BookState.INDEXED, stateOf(3));
        assertEquals(new IndexPublished(1, 2, 2, AT), events.poll(5, TimeUnit.SECONDS));
        assertEquals("connected", notifier.state());
    }

    @Test
    void sinVersionNuevaNoHayEventoPeroSiSeReparanLosEstados() throws Exception {
        notifier.connected(coordination);
        indexedInBookState(4);
        published(1, 4);                                 // T10 of version 1 was lost

        notifier.afterPublish(new PublishResult(1, 0, 1, AT));
        notifier.afterPublish(new PublishResult(2, 1, 2, AT));    // a later one, as a marker
        awaitNotifier();

        assertEquals(BookState.PUBLISHED, stateOf(4));
        // The first event is the one of version 2: 1 new book, 2 in total.
        assertEquals(new IndexPublished(2, 1, 2, AT), events.poll(5, TimeUnit.SECONDS));
    }

    @Test
    void sinNadaPublicadoNuncaNoMarcaNada() throws Exception {
        notifier.connected(coordination);
        indexedInBookState(5);

        notifier.afterPublish(new PublishResult(null, 0, 0, null));
        awaitNotifier();

        assertEquals(BookState.INDEXED, stateOf(5));
        assertNull(events.poll(300, TimeUnit.MILLISECONDS));
    }

    @Test
    void alConectarReparaUnT10Interrumpido() throws Exception {
        indexedInBookState(6, 7);
        published(1, 6);

        notifier.connected(coordination);
        awaitNotifier();

        assertEquals(BookState.PUBLISHED, stateOf(6));
        assertEquals(BookState.INDEXED, stateOf(7));
    }

    @Test
    void desconectadoSeSaltaYAlVolverSeAnunciaLaVersionYSeMarcanLosEstados() throws Exception {
        notifier.connected(coordination);
        awaitNotifier();
        indexedInBookState(8);
        published(1, 8);

        notifier.connectionChanged(false);
        notifier.afterPublish(new PublishResult(1, 1, 1, AT));
        awaitNotifier();
        assertEquals("disconnected", notifier.state());
        assertEquals(1, notifier.skipped());
        assertEquals(BookState.INDEXED, stateOf(8));
        assertNull(events.poll(300, TimeUnit.MILLISECONDS));

        notifier.connectionChanged(true);
        awaitNotifier();

        assertEquals(new IndexPublished(1, 1, 1, AT), events.poll(5, TimeUnit.SECONDS));
        assertEquals(BookState.PUBLISHED, stateOf(8));
    }

    @Test
    void antesDeConectarSeGuardaLaUltimaVersionYSeAnunciaAlConectar() throws Exception {
        indexedInBookState(10, 11);
        published(1, 10);
        notifier.afterPublish(new PublishResult(1, 1, 1, AT));
        published(2, 11);
        notifier.afterPublish(new PublishResult(2, 1, 2, AT));

        notifier.connected(coordination);
        awaitNotifier();

        assertEquals(2, notifier.skipped());
        assertEquals(new IndexPublished(2, 1, 2, AT), events.poll(5, TimeUnit.SECONDS));
        assertNull(events.poll(300, TimeUnit.MILLISECONDS), "solo se anuncia la última");
        assertEquals(BookState.PUBLISHED, stateOf(10));
        assertEquals(BookState.PUBLISHED, stateOf(11));
    }

    @Test
    void unaReconexionSinNadaSaltadoNoAnunciaNada() throws Exception {
        notifier.connected(coordination);
        notifier.afterPublish(new PublishResult(1, 1, 1, AT));
        awaitNotifier();
        assertEquals(new IndexPublished(1, 1, 1, AT), events.poll(5, TimeUnit.SECONDS));

        notifier.connectionChanged(false);
        notifier.connectionChanged(true);
        awaitNotifier();

        assertNull(events.poll(300, TimeUnit.MILLISECONDS));
    }

    // ---- its own thread (review of #32) ----

    @Test
    void conT10BloqueadoAfterPublishVuelveEnseguidaYLasSincronizacionesSeAgrupan() throws Exception {
        notifier.connected(coordination);
        awaitNotifier();
        indexedInBookState(12, 13);
        published(1, 12);
        CountDownLatch insideSync = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger syncs = new AtomicInteger();
        publications.beforeCurrent(() -> {
            if (syncs.incrementAndGet() == 1) {
                insideSync.countDown();
                await(release);                         // a MongoDB or a Hazelcast that is slow
            }
        });

        notifier.afterPublish(new PublishResult(1, 1, 1, AT));
        assertTrue(insideSync.await(5, TimeUnit.SECONDS));
        long start = System.nanoTime();
        published(2, 13);
        for (int version = 2; version <= 4; version++) {
            notifier.afterPublish(new PublishResult(version, 1, version, AT));
        }
        long tookMs = (System.nanoTime() - start) / 1_000_000;
        release.countDown();
        awaitNotifier();

        assertTrue(tookMs < 500, "afterPublish solo encola, tardó " + tookMs + " ms");
        assertEquals(2, syncs.get(), "las tres publicaciones durante la espera comparten una sincronización");
        List<Integer> versions = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            versions.add(events.poll(5, TimeUnit.SECONDS).version());
        }
        assertEquals(List.of(1, 2, 3, 4), versions, "un evento por versión, en orden");
        assertEquals(BookState.PUBLISHED, stateOf(12));
        assertEquals(BookState.PUBLISHED, stateOf(13));
    }

    @Test
    void siT10FallaElEventoSaleIgualYLaSiguientePublicacionMarcaLosEstados() throws Exception {
        notifier.connected(coordination);
        awaitNotifier();
        indexedInBookState(14);
        published(1, 14);
        AtomicBoolean mongoDown = new AtomicBoolean(true);
        publications.beforeCurrent(() -> {
            if (mongoDown.get()) {
                throw new MongoException("MongoDB no responde (simulado)");
            }
        });

        notifier.afterPublish(new PublishResult(1, 1, 1, AT));
        awaitNotifier();
        assertEquals(new IndexPublished(1, 1, 1, AT), events.poll(5, TimeUnit.SECONDS));
        assertEquals(BookState.INDEXED, stateOf(14));

        mongoDown.set(false);
        notifier.afterPublish(new PublishResult(1, 0, 1, AT));
        awaitNotifier();

        assertEquals(BookState.PUBLISHED, stateOf(14));
    }

    @Test
    void siSePierdeElClusterConUnEventoEnColaNoSeEnviaYSeAnunciaAlVolver() throws Exception {
        notifier.connected(coordination);
        awaitNotifier();
        published(1, 16);
        CountDownLatch insideSync = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean first = new AtomicBoolean(true);
        publications.beforeCurrent(() -> {
            if (first.getAndSet(false)) {
                insideSync.countDown();
                await(release);
            }
        });
        notifier.afterPublish(new PublishResult(1, 0, 1, AT));   // only a sync, which blocks
        assertTrue(insideSync.await(5, TimeUnit.SECONDS));
        notifier.afterPublish(new PublishResult(2, 1, 2, AT));   // queued behind it

        notifier.connectionChanged(false);
        release.countDown();
        awaitNotifier();

        assertNull(events.poll(300, TimeUnit.MILLISECONDS), "sin clúster no se llama a Hazelcast");
        assertEquals(1, notifier.skipped());
        notifier.connectionChanged(true);
        awaitNotifier();
        assertEquals(new IndexPublished(2, 1, 2, AT), events.poll(5, TimeUnit.SECONDS));
    }

    @Test
    void closeEsperaUnMargenYLuegoCortaLoQueQueda() throws Exception {
        notifier.connected(coordination);
        awaitNotifier();
        published(1, 15);
        CountDownLatch insideSync = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        publications.beforeCurrent(() -> {
            insideSync.countDown();
            try {
                Thread.sleep(60_000);                   // never ends by itself
            } catch (InterruptedException e) {
                interrupted.countDown();
                Thread.currentThread().interrupt();
            }
        });
        notifier.afterPublish(new PublishResult(1, 1, 1, AT));
        assertTrue(insideSync.await(5, TimeUnit.SECONDS));

        long start = System.nanoTime();
        notifier.close();
        long tookMs = (System.nanoTime() - start) / 1_000_000;

        assertTrue(tookMs >= HazelcastPublicationNotifier.CLOSE_GRACE.toMillis() - 100, "esperó " + tookMs + " ms");
        assertTrue(tookMs < HazelcastPublicationNotifier.CLOSE_GRACE.toMillis() + 2_000, "esperó " + tookMs + " ms");
        assertTrue(interrupted.await(1, TimeUnit.SECONDS), "close() debe interrumpir lo que sigue en marcha");
        assertDoesNotThrow(() -> notifier.afterPublish(new PublishResult(2, 1, 2, AT)));
    }

    @Test
    void unLibroIndexadoPorHttpQueNoEstaEnBookStateNoMolesta() throws Exception {
        notifier.connected(coordination);
        published(1, 9);                                 // indexed with POST /index/update: no state

        notifier.afterPublish(new PublishResult(1, 1, 1, AT));
        awaitNotifier();

        assertTrue(coordination.task(9).isEmpty());
        assertEquals(new IndexPublished(1, 1, 1, AT), events.poll(5, TimeUnit.SECONDS));
    }

    @Test
    void siElClientePierdeElClusterElEstadoPasaADisconnected() throws Exception {
        // A member of its own: stopping the shared one would break the other tests.
        String cluster = "indexing-disconnect-" + UUID.randomUUID();
        HazelcastInstance member = HazelcastTestCluster.startStandaloneMember(cluster, 5941);
        CoordinationSettings settings = new CoordinationSettings(List.of("127.0.0.1:5941"), cluster, 120, 3, 10);
        HazelcastInstance ownClient = settings.openClient();
        try {
            notifier.connected(Coordination.on(ownClient, settings, Clock.systemUTC()));
            assertEquals("connected", notifier.state());

            member.shutdown();

            for (int i = 0; i < 100 && !"disconnected".equals(notifier.state()); i++) {
                Thread.sleep(50);
            }
            assertEquals("disconnected", notifier.state());
        } finally {
            ownClient.shutdown();
            member.shutdown();
        }
    }

    @Test
    void conectarDosVecesEsUnError() {
        notifier.connected(coordination);

        assertThrows(IllegalStateException.class, () -> notifier.connected(coordination));
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
