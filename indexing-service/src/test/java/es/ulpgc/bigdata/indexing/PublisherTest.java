package es.ulpgc.bigdata.indexing;

import com.mongodb.MongoException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** The publication protocol of plan §6.5 (DR-08a, D1-D8) on the fakes, without MongoDB. */
class PublisherTest {

    private static final String AT = "2026-11-05T09:30:15Z";

    private InMemoryIndexLog indexLog;
    private InMemoryPublicationRepository publications;
    private Publisher publisher;

    @BeforeEach
    void setUp() {
        indexLog = new InMemoryIndexLog();
        publications = new InMemoryPublicationRepository(indexLog);
        // With milliseconds, to check they are cut (D6).
        publisher = new Publisher(publications, "inverted_index",
                Clock.fixed(Instant.parse("2026-11-05T09:30:15.987Z"), ZoneOffset.UTC));
    }

    private void indexed(Integer... ids) {
        indexLog.markIndexed(List.of(ids), Instant.now());
    }

    // ---- nothing to publish (D5) ----

    @Test
    void sinNadaPublicadoNiNadaNuevoDevuelveVersionNula() {
        PublishResult result = publisher.publish();

        assertEquals(new PublishResult(null, 0, 0, null), result);
        assertEquals(Optional.empty(), publications.current());
        assertEquals(0, publications.switchAttempts());
    }

    @Test
    void sinNadaNuevoDescribeCurrentSinCambiarlo() {
        indexed(1, 2);
        publisher.publish();

        PublishResult again = publisher.publish();

        assertEquals(new PublishResult(1, 0, 2, AT), again);
        assertEquals(1, publications.switchAttempts());
    }

    // ---- a publication ----

    @Test
    void laPrimeraPublicacionHaceVisiblesLosLibrosIndexados() {
        indexed(3, 1, 2);

        PublishResult result = publisher.publish();

        assertEquals(new PublishResult(1, 3, 3, AT), result);
        assertEquals(new Publication(1, 3, "inverted_index", AT), publications.current().orElseThrow());
        assertEquals(1, publications.versionOf(2));
        assertEquals(new PublishedVersion(1, List.of(1, 2, 3), 3, AT, "inverted_index"), publications.historyOf(1));
    }

    @Test
    void laSiguientePublicacionSoloPublicaLosNuevosYCuentaTodos() {
        indexed(1, 2);
        publisher.publish();
        indexed(2, 5);                                    // 2 indexed again: already published

        PublishResult result = publisher.publish();

        assertEquals(new PublishResult(2, 1, 3, AT), result);
        assertEquals(1, publications.versionOf(2));       // keeps its first version
        assertEquals(2, publications.versionOf(5));
        assertEquals(List.of(5), publications.historyOf(2).books());
    }

    @Test
    void conservaLaColeccionDeCurrent() {
        publications.setCurrent(new Publication(7, 0, "inverted_index_v2", "2026-11-01T00:00:00Z"));
        publications.recordHistory(new PublishedVersion(7, List.of(), 0, "2026-11-01T00:00:00Z", "inverted_index_v2"));
        indexed(1);

        publisher.publish();

        assertEquals("inverted_index_v2", publications.current().orElseThrow().collection());
        assertEquals(8, publications.current().orElseThrow().version());
    }

    @Test
    void siCurrentNoDiceLaColeccionUsaLaPorDefecto() {
        publications.setCurrent(new Publication(7, 0));    // written by another program
        indexed(1);

        publisher.publish();

        assertEquals("inverted_index", publications.current().orElseThrow().collection());
    }

    @Test
    void alRecontarUnCurrentSinColeccionLePoneLaPorDefecto() {
        publications.setCurrent(new Publication(1, 0));    // written by another program, books wrong
        publications.addBooks(List.of(4), 1);

        PublishResult result = publisher.publish();

        assertEquals(new PublishResult(1, 0, 1, null), result);
        assertEquals(new Publication(1, 1, "inverted_index", null), publications.current().orElseThrow());
    }

    // ---- interrupted publications (D2, D3) ----

    @Test
    void completaUnaPublicacionQueMurioAntesDeCambiarCurrent() {
        indexed(1, 2);
        publications.failSwitch(true);
        assertThrows(MongoException.class, publisher::publish);
        assertEquals(1, publications.versionOf(1));       // recorded, but invisible
        assertEquals(Optional.empty(), publications.current());
        publications.failSwitch(false);
        indexed(3);

        PublishResult result = publisher.publish();

        assertEquals(new PublishResult(1, 3, 3, AT), result);
        assertEquals(List.of(1, 2, 3), publications.historyOf(1).books());
    }

    @Test
    void alArrancarCompletaSoloLosInterrumpidos() {
        indexed(1, 2);
        publications.failSwitch(true);
        assertThrows(MongoException.class, publisher::publish);
        publications.failSwitch(false);
        indexed(9);                                       // indexed later: waits for a publish

        Optional<PublishResult> recovered = publisher.recover();

        assertEquals(Optional.of(new PublishResult(1, 2, 2, AT)), recovered);
        assertNull(publications.versionOf(9));
        assertEquals(new PublishResult(2, 1, 3, AT), publisher.publish());
    }

    @Test
    void alArrancarSinNadaInterrumpidoNoPublicaNada() {
        indexed(1);

        assertEquals(Optional.empty(), publisher.recover());
        assertEquals(Optional.empty(), publications.current());
    }

    @Test
    void siFallaElHistorialRespondeLaVersionPublicadaYLaSiguienteLlamadaLoRepara() {
        indexed(1, 2);
        publications.failHistory(true);

        assertEquals(new PublishResult(1, 2, 2, AT), publisher.publish());
        assertEquals(1, publications.current().orElseThrow().version());
        assertNull(publications.historyOf(1));
        publications.failHistory(false);

        PublishResult result = publisher.publish();

        assertEquals(new PublishResult(1, 0, 2, AT), result);
        assertEquals(new PublishedVersion(1, List.of(1, 2), 2, AT, "inverted_index"), publications.historyOf(1));
    }

    @Test
    void siFallaElHistorialLaVersionSeAnunciaIgual() {
        List<PublishResult> notified = new CopyOnWriteArrayList<>();
        Publisher notifying = new Publisher(publications, "inverted_index",
                Clock.fixed(Instant.parse(AT), ZoneOffset.UTC), notified::add);
        indexed(1);
        publications.failHistory(true);

        notifying.publish();

        assertEquals(List.of(new PublishResult(1, 1, 1, AT)), notified);
    }

    @Test
    void alArrancarReparaElHistorialQueFalta() {
        indexed(1, 2);
        publisher.publish();
        publications.forgetHistory(1);

        assertEquals(Optional.empty(), publisher.recover());
        assertEquals(List.of(1, 2), publications.historyOf(1).books());
    }

    // ---- the CAS (D4) ----

    @Test
    void siOtraReplicaPublicaLaMismaVersionCuentaTambienLosLibrosDeEstaReplica() {
        indexed(1, 2, 3);
        AtomicBoolean first = new AtomicBoolean(true);
        publications.beforeSwitch(() -> {
            if (first.getAndSet(false)) {
                // The other replica read the same CURRENT, published 1 and 2 as version 1 and
                // counted before this one recorded 3 with version 1 too.
                otherReplicaPublishes(new Publication(1, 2, "inverted_index", "2026-11-05T09:00:00Z"), List.of(1, 2));
            }
        });

        PublishResult result = publisher.publish();

        assertEquals(2, publications.switchAttempts());   // the failed CAS + the one that counts again
        assertEquals(new PublishResult(1, 0, 3, "2026-11-05T09:00:00Z"), result);
        assertEquals(3, publications.current().orElseThrow().books());
        assertEquals(List.of(1, 2, 3), publications.historyOf(1).books());
        assertEquals(3, publications.historyOf(1).totalBooks());
    }

    @Test
    void siOtraReplicaPublicaAntesRepiteLaPublicacionSobreSuVersion() {
        indexed(1, 2);
        AtomicBoolean first = new AtomicBoolean(true);
        publications.beforeSwitch(() -> {
            if (first.getAndSet(false)) {
                otherReplicaPublishes(new Publication(1, 2, "inverted_index", "2026-11-05T09:00:00Z"), List.of(1, 2));
                indexed(7);                               // indexed meanwhile: for the retry
            }
        });

        PublishResult result = publisher.publish();

        assertEquals(new PublishResult(2, 1, 3, AT), result);
        assertEquals(List.of(7), publications.historyOf(2).books());
    }

    @Test
    void siElCasFallaTresVecesRespondeConError() {
        indexed(1);
        AtomicInteger otherBook = new AtomicInteger(100);
        publications.beforeSwitch(() -> {                 // another replica publishes before every CAS
            int version = publications.current().map(Publication::version).orElse(0) + 1;
            publications.addBooks(List.of(1), version);
            otherReplicaPublishes(new Publication(version, publications.countVisible(version), "inverted_index", AT),
                    publications.booksOfVersion(version));
            indexed(otherBook.getAndIncrement());         // so that there is always something new
        });

        IllegalStateException e = assertThrows(IllegalStateException.class, publisher::publish);

        assertEquals(Publisher.MAX_ATTEMPTS, publications.switchAttempts());
        assertTrue(e.getMessage().contains("3 intentos"), e.getMessage());
    }

    /** What another replica leaves after a whole publication: CURRENT and its history entry. */
    private void otherReplicaPublishes(Publication current, List<Integer> books) {
        publications.setCurrent(current);
        publications.recordHistory(new PublishedVersion(current.version(), books, current.books(),
                current.publishedAt(), current.collection()));
    }

    // ---- notifications (DR-08b) ----

    @Test
    void notificaCadaPublicacionConLoQueRespondio() {
        List<PublishResult> notified = new CopyOnWriteArrayList<>();
        Publisher notifying = new Publisher(publications, "inverted_index",
                Clock.fixed(Instant.parse(AT), ZoneOffset.UTC), notified::add);
        indexed(1, 2);

        PublishResult first = notifying.publish();
        PublishResult nothingNew = notifying.publish();

        assertEquals(List.of(first, nothingNew), notified);
        assertEquals(new PublishResult(1, 0, 2, AT), nothingNew);
    }

    @Test
    void siNotificarFallaLaPublicacionSigueHecha() {
        Publisher failing = new Publisher(publications, "inverted_index",
                Clock.fixed(Instant.parse(AT), ZoneOffset.UTC), result -> {
                    throw new IllegalStateException("Hazelcast no responde (simulado)");
                });
        indexed(1);

        PublishResult result = failing.publish();

        assertEquals(new PublishResult(1, 1, 1, AT), result);
        assertEquals(1, publications.current().orElseThrow().version());
    }

    @Test
    void unaPublicacionQueFallaNoSeNotifica() {
        List<PublishResult> notified = new CopyOnWriteArrayList<>();
        Publisher notifying = new Publisher(publications, "inverted_index",
                Clock.fixed(Instant.parse(AT), ZoneOffset.UTC), notified::add);
        indexed(1);
        publications.failSwitch(true);

        assertThrows(MongoException.class, notifying::publish);

        assertEquals(List.of(), notified);
    }

    @Test
    void laRecuperacionAlArrancarTambienNotifica() {
        List<PublishResult> notified = new CopyOnWriteArrayList<>();
        Publisher notifying = new Publisher(publications, "inverted_index",
                Clock.fixed(Instant.parse(AT), ZoneOffset.UTC), notified::add);
        publications.addBooks(List.of(5), 1);              // a publication of 1 died before the CAS

        notifying.recover();

        assertEquals(List.of(new PublishResult(1, 1, 1, AT)), notified);
    }

    // ---- one publication at a time in the process (D7) ----

    @Test
    void dosPublicacionesALaVezEnElMismoProcesoVanUnaTrasOtra() throws Exception {
        indexed(1, 2);
        CountDownLatch insideSwitch = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean first = new AtomicBoolean(true);
        publications.beforeSwitch(() -> {
            if (first.getAndSet(false)) {
                insideSwitch.countDown();
                await(release);
            }
        });
        CompletableFuture<PublishResult> a = CompletableFuture.supplyAsync(publisher::publish);
        assertTrue(insideSwitch.await(5, TimeUnit.SECONDS));

        CompletableFuture<PublishResult> b = CompletableFuture.supplyAsync(publisher::publish);
        Thread.sleep(200);
        assertFalse(b.isDone(), "la segunda publicación no debe empezar mientras la primera no termine");
        release.countDown();

        assertEquals(new PublishResult(1, 2, 2, AT), a.get(5, TimeUnit.SECONDS));
        assertEquals(new PublishResult(1, 0, 2, AT), b.get(5, TimeUnit.SECONDS));
        assertEquals(1, publications.switchAttempts());
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
