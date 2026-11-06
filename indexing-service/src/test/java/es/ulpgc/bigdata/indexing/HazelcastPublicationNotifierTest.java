package es.ulpgc.bigdata.indexing;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import es.ulpgc.bigdata.coordination.BookState;
import es.ulpgc.bigdata.coordination.Claim;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.CoordinationSettings;
import es.ulpgc.bigdata.coordination.IndexPublished;
import es.ulpgc.bigdata.coordination.Stage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * T10 and IndexPublished (DR-08b) against a real Hazelcast member embedded in the JVM: the
 * books reach INDEXED through the same transitions as in production (seed, claim, complete).
 * index_books is the in-memory fake.
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
        client = HazelcastTestMember.settings().openClient();
    }

    @AfterAll
    static void stopClient() {
        client.shutdown();
    }

    @BeforeEach
    void setUp() {
        HazelcastTestMember.reset();
        coordination = Coordination.on(client, HazelcastTestMember.settings(), Clock.systemUTC());
        publications = new InMemoryPublicationRepository();
        notifier = new HazelcastPublicationNotifier(publications);
        subscription = coordination.subscribe(events::add);
    }

    @AfterEach
    void tearDown() {
        coordination.unsubscribe(subscription);
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

        assertEquals(BookState.PUBLISHED, stateOf(4));
        // The first event is the one of version 2: 1 new book, 2 in total.
        assertEquals(new IndexPublished(2, 1, 2, AT), events.poll(5, TimeUnit.SECONDS));
    }

    @Test
    void sinNadaPublicadoNuncaNoMarcaNada() throws Exception {
        notifier.connected(coordination);
        indexedInBookState(5);

        notifier.afterPublish(new PublishResult(null, 0, 0, null));

        assertEquals(BookState.INDEXED, stateOf(5));
        assertNull(events.poll(300, TimeUnit.MILLISECONDS));
    }

    @Test
    void alConectarReparaUnT10Interrumpido() throws Exception {
        indexedInBookState(6, 7);
        published(1, 6);

        int marked = notifier.connected(coordination);

        assertEquals(1, marked);
        assertEquals(BookState.PUBLISHED, stateOf(6));
        assertEquals(BookState.INDEXED, stateOf(7));
    }

    @Test
    void desconectadoSeSaltaYAlVolverSeNotificaDeNuevo() throws Exception {
        notifier.connected(coordination);
        indexedInBookState(8);
        published(1, 8);

        notifier.connectionChanged(false);
        notifier.afterPublish(new PublishResult(1, 1, 1, AT));
        assertEquals("disconnected", notifier.state());
        assertEquals(BookState.INDEXED, stateOf(8));

        notifier.connectionChanged(true);
        notifier.afterPublish(new PublishResult(1, 0, 1, AT));
        assertEquals(BookState.PUBLISHED, stateOf(8));
        assertEquals(1, notifier.skipped());
    }

    @Test
    void unLibroIndexadoPorHttpQueNoEstaEnBookStateNoMolesta() throws Exception {
        notifier.connected(coordination);
        published(1, 9);                                 // indexed with POST /index/update: no state

        notifier.afterPublish(new PublishResult(1, 1, 1, AT));

        assertTrue(coordination.task(9).isEmpty());
        assertEquals(new IndexPublished(1, 1, 1, AT), events.poll(5, TimeUnit.SECONDS));
    }

    @Test
    void siElClientePierdeElClusterElEstadoPasaADisconnected() throws Exception {
        // A member of its own: stopping the shared one would break the other tests.
        Config config = new Config().setClusterName("indexing-disconnect-" + UUID.randomUUID());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.getJetConfig().setEnabled(false);
        config.getNetworkConfig().setPort(5941).setPortAutoIncrement(true);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        CoordinationSettings settings = new CoordinationSettings(
                List.of("127.0.0.1:" + member.getCluster().getLocalMember().getAddress().getPort()),
                config.getClusterName(), 120, 3, 10);
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
}
