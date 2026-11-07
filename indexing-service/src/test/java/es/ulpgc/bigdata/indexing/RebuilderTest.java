package es.ulpgc.bigdata.indexing;

import com.hazelcast.core.HazelcastInstance;
import com.mongodb.MongoException;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.CoordinationNames;
import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.datamart.index.InvertedIndex;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The rebuild of the index (DR-10) against the embedded Hazelcast cluster (the marker and
 * index-writer are real) and the in-memory stores, with one in-memory index per collection, as
 * App opens one MongoInvertedIndex per collection.
 */
class RebuilderTest {

    private static final String DEFAULT = "inverted_index";
    private static final Duration DROP_DELAY = Duration.ofMillis(500);

    @TempDir Path dataDir;

    private static HazelcastInstance client;

    private Coordination coordination;
    private RangeBasedDatalake datalake;
    private InMemoryMetadataRepository metadata;
    private InMemoryIndexLog indexLog;
    private InMemoryPublicationRepository publications;
    private final Map<String, IndexingServiceTest.SpyIndex> collections = new ConcurrentHashMap<>();
    private InMemoryRebuildRepository repository;
    private IndexingService service;
    private Publisher publisher;
    private Rebuilder rebuilder;
    /** The index of this collection fails its first flush, as MongoDB going down. */
    private volatile String failingCollection = null;

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
        indexLog = new InMemoryIndexLog();
        publications = new InMemoryPublicationRepository(indexLog);
        repository = new InMemoryRebuildRepository(collections);
        service = new IndexingService(datalake, metadata,
                name -> collections.computeIfAbsent(name, this::newIndex), DEFAULT,
                indexLog, publications, new MetadataParser(), Tokenizer.standard(), Runnable::run);
        publisher = new Publisher(publications, DEFAULT, Clock.systemUTC());
        rebuilder = rebuilder(Optional.of(coordination));
    }

    private IndexingServiceTest.SpyIndex newIndex(String collection) {
        IndexingServiceTest.SpyIndex index = new IndexingServiceTest.SpyIndex();
        if (collection.equals(failingCollection)) {
            index.failNextFlushWith = new MongoException("MongoDB no responde (simulado)");
        }
        return index;
    }

    private Rebuilder rebuilder(Optional<Coordination> cluster) {
        return new Rebuilder(() -> cluster, service, datalake, publications, publisher, repository,
                DEFAULT, 2, DROP_DELAY, Clock.systemUTC());
    }

    @AfterEach
    void tearDown() {
        rebuilder.close();
    }

    /** Three books indexed and published into inverted_index, as before any rebuild. */
    private void indexedAndPublished() {
        datalake.save(new RawBook(1342, IndexingServiceTest.HEADER_1342, IndexingServiceTest.BODY_1342));
        datalake.save(book(11, "Alice and the white rabbit, a fortune of tea."));
        datalake.save(book(84, "Victor builds a creature with a fortune."));
        service.update(1342);
        service.update(11);
        service.update(84);
        publisher.publish();
    }

    private static RawBook book(int id, String body) {
        return new RawBook(id, "Title: Book " + id + "\n\nAuthor: Someone\n\nLanguage: English\n", body);
    }

    private Rebuild finished() throws InterruptedException {
        assertTrue(rebuilder.awaitIdle(Duration.ofSeconds(20)), "la reconstrucción no terminó");
        return repository.last().orElseThrow();
    }

    private List<Integer> postings(String collection, String term) {
        InvertedIndex index = collections.get(collection);
        return index == null ? List.of() : index.postings(term);
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, what);
            Thread.sleep(20);
        }
    }

    /** index-writer held by another thread, as the writer of index-queue holds it. */
    private static final class WriterHoldingTheLock implements AutoCloseable {
        private final CountDownLatch taken = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final Thread thread;

        WriterHoldingTheLock(Coordination coordination) throws InterruptedException {
            thread = new Thread(() -> {
                coordination.indexWriterLock().lock();
                taken.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    coordination.indexWriterLock().unlock();
                }
            });
            thread.start();
            assertTrue(taken.await(5, TimeUnit.SECONDS));
        }

        @Override
        public void close() throws InterruptedException {
            release.countDown();
            thread.join(5000);
        }
    }

    // --- A rebuild -----------------------------------------------------------------------

    @Test
    void reconstruyeTodoElDatalakeEnUnaColeccionNuevaYLaPublica() throws Exception {
        indexedAndPublished();
        collections.get(DEFAULT).addDocument(11, Set.of("obsolete"));      // as a tokenizer of before

        Rebuild started = rebuilder.start();
        Rebuild done = finished();

        assertEquals(Rebuild.State.RUNNING, started.state());
        assertEquals(3, started.total());
        assertEquals(Rebuild.State.DONE, done.state());
        assertEquals(3, done.processed());
        assertNotNull(done.finishedAt());
        assertNull(done.error());
        Publication current = publications.current().orElseThrow();
        assertEquals(2, current.version(), "el cambio es una publicación más");
        assertEquals("inverted_index_r01", current.collection());
        assertEquals(3, current.books());
        assertEquals(List.of(11, 84, 1342), postings("inverted_index_r01", "fortune"));
        assertEquals(List.of(), postings("inverted_index_r01", "obsolete"), "se reconstruye desde el datalake");
        assertFalse(coordination.isRebuilding(), "la marca se quita al terminar");
        assertFalse(coordination.indexWriterLock().isLocked(), "y index-writer se suelta");
    }

    @Test
    void elProgresoSeGuardaLoteALote() throws Exception {
        indexedAndPublished();

        rebuilder.start();
        finished();

        List<Integer> processed = repository.saved.stream()
                .filter(r -> r.state() == Rebuild.State.RUNNING).map(Rebuild::processed).toList();
        assertEquals(List.of(0, 2, 3), processed, "lotes de 2: 0 al empezar, 2 y 3");
    }

    @Test
    void laColeccionViejaSeBorraTrasElMargenYNoAntes() throws Exception {
        indexedAndPublished();

        rebuilder.start();
        Rebuild done = finished();

        assertTrue(collections.containsKey(DEFAULT), "Search aún puede estar leyéndola");
        assertEquals(DEFAULT, done.pendingDrop());
        await("la colección vieja no se borró", () -> repository.dropped.contains(DEFAULT));
        assertTrue(collections.containsKey("inverted_index_r01"));
    }

    @Test
    void conElDatalakeVacioPublicaUnaColeccionVacia() throws Exception {
        rebuilder.start();
        Rebuild done = finished();

        assertEquals(Rebuild.State.DONE, done.state());
        assertEquals(0, done.total());
        assertEquals(new Publication(1, 0, "inverted_index_r01", publications.current().orElseThrow().publishedAt()),
                publications.current().orElseThrow());
    }

    // --- One at a time, and the writers stop ---------------------------------------------

    @Test
    void siHayOtraEnMarchaRespondeQueYaHayUna() {
        coordination.tryStartRebuild(Duration.ofSeconds(30)).orElseThrow();   // another replica

        assertThrows(RebuildInProgressException.class, rebuilder::start);
        assertTrue(rebuilder.isRebuilding(), "update y publish responden 409");
        assertTrue(repository.last().isEmpty(), "no ha empezado nada");
    }

    @Test
    void esperaAQueElEscritorSuelteElLockAntesDeTocarNada() throws Exception {
        indexedAndPublished();
        try (WriterHoldingTheLock writer = new WriterHoldingTheLock(coordination)) {
            rebuilder.start();
            Thread.sleep(500);

            assertTrue(rebuilder.isRebuilding());
            assertFalse(collections.containsKey("inverted_index_r01"), "no escribe con el escritor activo");
            assertEquals(1, publications.current().orElseThrow().version());
        }
        assertEquals(Rebuild.State.DONE, finished().state(), "en cuanto lo suelta, reconstruye");
    }

    @Test
    void unLibroEscritoEnLaColeccionViejaMientrasEsperaTambienEntra() throws Exception {
        indexedAndPublished();
        try (WriterHoldingTheLock writer = new WriterHoldingTheLock(coordination)) {
            rebuilder.start();                                  // lists 3 books
            // An update that passed the 409 check just before the marker: into CURRENT, the old one.
            datalake.save(book(7, "A late fortune."));
            service.update(7);
        }
        finished();

        assertEquals(List.of(7, 11, 84, 1342), postings("inverted_index_r01", "fortune"),
                "lo que publica el cambio está en la colección nueva");
        assertEquals(4, publications.current().orElseThrow().books());
    }

    // --- Failures --------------------------------------------------------------------------

    @Test
    void siFallaAntesDelCambioBorraSuColeccionYCurrentNoCambia() throws Exception {
        indexedAndPublished();
        failingCollection = "inverted_index_r01";

        rebuilder.start();
        Rebuild failed = finished();

        assertEquals(Rebuild.State.FAILED, failed.state());
        assertTrue(failed.error().startsWith("the index storage (MongoDB) does not answer"), failed.error());
        assertEquals(new Publication(1, 3, DEFAULT, publications.current().orElseThrow().publishedAt()),
                publications.current().orElseThrow(), "Search no se ha enterado de nada");
        assertFalse(collections.containsKey("inverted_index_r01"), "la colección a medias se borra");
        assertFalse(coordination.isRebuilding());
        assertFalse(coordination.indexWriterLock().isLocked());
    }

    @Test
    void trasUnFalloLaSiguienteEmpiezaDeCeroYNoReutilizaElNombre() throws Exception {
        indexedAndPublished();
        failingCollection = "inverted_index_r01";
        rebuilder.start();
        finished();

        Rebuild second = rebuilder.start();
        Rebuild done = finished();

        assertEquals("inverted_index_r02", second.collection(), "r01 pudo quedar abierta en otra réplica");
        assertEquals(Rebuild.State.DONE, done.state());
        assertEquals("inverted_index_r02", publications.current().orElseThrow().collection());
    }

    @Test
    void siLaMarcaCaducaSeDetieneSinCambiarCurrent() throws Exception {
        indexedAndPublished();
        try (WriterHoldingTheLock writer = new WriterHoldingTheLock(coordination)) {
            rebuilder.start();
            // What a replica stalled longer than the TTL finds: the marker is gone.
            client.getMap(CoordinationNames.COORDINATION_META).remove(CoordinationNames.REBUILD);
            Rebuild failed = finished();

            assertEquals(Rebuild.State.FAILED, failed.state());
            assertTrue(failed.error().contains("marker expired"), failed.error());
        }
        assertEquals(1, publications.current().orElseThrow().version());
        assertFalse(collections.containsKey("inverted_index_r01"));
    }

    @Test
    void alCerrarElServicioLaReconstruccionFallaYBorraSuColeccion() throws Exception {
        indexedAndPublished();
        try (WriterHoldingTheLock writer = new WriterHoldingTheLock(coordination)) {
            rebuilder.start();
            Thread.sleep(300);                                  // it waits for index-writer now
            rebuilder.close();

            Rebuild failed = repository.last().orElseThrow();
            assertEquals(Rebuild.State.FAILED, failed.state());
            assertEquals("the service stopped", failed.error());
        }
        assertFalse(coordination.isRebuilding(), "la marca no espera a su TTL");
        assertEquals(1, publications.current().orElseThrow().version());
    }

    // --- Collections nobody reads --------------------------------------------------------

    @Test
    void borraLasColeccionesHuerfanasYNuncaReutilizaSuNombre() throws Exception {
        indexedAndPublished();
        collections.put("inverted_index_r05", new IndexingServiceTest.SpyIndex());   // a rebuild that died
        collections.put("otra_coleccion", new IndexingServiceTest.SpyIndex());       // not of the index

        Rebuild started = rebuilder.start();
        finished();

        assertEquals("inverted_index_r06", started.collection());
        assertTrue(repository.dropped.contains("inverted_index_r05"));
        assertTrue(collections.containsKey("otra_coleccion"), "solo las del índice");
        assertTrue(collections.containsKey(DEFAULT) || repository.dropped.indexOf(DEFAULT) > 0,
                "la de CURRENT espera a su margen");
    }

    @Test
    void noBorraLaColeccionQueOtraReconstruccionDejoEsperandoSuMargen() throws Exception {
        indexedAndPublished();
        publisher.publishInto("inverted_index_r02");
        collections.put("inverted_index_r01", new IndexingServiceTest.SpyIndex());
        collections.put("inverted_index_r02", new IndexingServiceTest.SpyIndex());
        Instant now = Instant.now();
        repository.save(new Rebuild(Rebuild.State.DONE, now.minusSeconds(60), now, 3, 3, null,
                "inverted_index_r02", "inverted_index_r01", now.plusSeconds(3600)));

        rebuilder.start();
        finished();

        assertTrue(collections.containsKey("inverted_index_r01"), "Search puede estar leyéndola todavía");
        assertTrue(repository.dropped.contains(DEFAULT), "inverted_index ya no la lee nadie");
    }

    // --- Status ----------------------------------------------------------------------------

    @Test
    void unaQueDiceRunningSinMarcaEsDeUnaReplicaQueMurio() {
        repository.save(new Rebuild(Rebuild.State.RUNNING, Instant.now(), null, 5, 10, null,
                "inverted_index_r01", null, null));

        Rebuild shown = rebuilder.status().orElseThrow();

        assertEquals(Rebuild.State.FAILED, shown.state());
        assertEquals("the replica that ran it stopped", shown.error());
        assertFalse(rebuilder.isRebuilding());
    }

    @Test
    void sinHazelcastNoArrancaYUpdateYPublishSiguen() {
        Rebuilder disconnected = rebuilder(Optional.empty());
        try {
            assertThrows(Rebuilder.CoordinationUnavailableException.class, disconnected::start);
            assertFalse(disconnected.isRebuilding(), "D4: indexar no depende de Hazelcast");
        } finally {
            disconnected.close();
        }
    }
}
