package es.ulpgc.bigdata.indexing;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import es.ulpgc.bigdata.datamart.indexlog.MongoIndexLog;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The publication collections (contract §6.5) on a real MongoDB. index_meta, index_books and
 * index_publications have fixed names, so each test uses a database of its own, dropped
 * afterwards: no interference with Search's tests, which write their own CURRENT in
 * search_engine_test. Skipped if there is no MongoDB.
 */
class MongoPublicationRepositoryTest {

    private static final String URI = System.getenv().getOrDefault("MONGO_URI", "mongodb://localhost:27017");

    private MongoClient client;
    private MongoDatabase db;
    private MongoPublicationRepository publications;
    private MongoIndexLog indexLog;

    @BeforeEach
    void setUp() {
        client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(URI))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(1500, TimeUnit.MILLISECONDS))
                .build());
        assumeTrue(mongoAnswers(), "MongoDB no está disponible en " + URI + ": test saltado");
        db = client.getDatabase("search_engine_test_pub_" + System.nanoTime());
        publications = new MongoPublicationRepository(db);
        indexLog = new MongoIndexLog(db);
    }

    private boolean mongoAnswers() {
        try {
            client.getDatabase("admin").runCommand(new Document("ping", 1));
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    @AfterEach
    void tearDown() {
        if (db != null) {
            db.drop();
        }
        client.close();
    }

    private void insertCurrent(Document current) {
        db.getCollection(MongoPublicationRepository.INDEX_META).insertOne(current);
    }

    private MongoCollection<Document> indexBooks() {
        return db.getCollection(MongoPublicationRepository.INDEX_BOOKS);
    }

    private Document historyDoc(int version) {
        return db.getCollection(MongoPublicationRepository.INDEX_PUBLICATIONS)
                .find(new Document("_id", version)).first();
    }

    // ---- current ----

    @Test
    void sinCurrentNoHayNadaPublicado() {
        assertEquals(Optional.empty(), publications.current());
    }

    @Test
    void leeCurrentConElFormatoDelContrato() {
        // The same document Search's MongoFixtureLoader writes (§6.5).
        insertCurrent(new Document("_id", "CURRENT")
                .append("version", 42)
                .append("collection", "inverted_index")
                .append("books", 187)
                .append("published_at", "2026-11-03T10:15:00Z"));

        assertEquals(Optional.of(new Publication(42, 187, "inverted_index", "2026-11-03T10:15:00Z")),
                publications.current());
    }

    @Test
    void leeNumerosGuardadosComoInt64PorOtroPrograma() {
        insertCurrent(new Document("_id", "CURRENT").append("version", 42L).append("books", 187L));

        assertEquals(Optional.of(new Publication(42, 187)), publications.current());
    }

    @Test
    void leePublishedAtGuardadoComoFechaPorOtroPrograma() {
        insertCurrent(new Document("_id", "CURRENT").append("version", 1).append("books", 2)
                .append("published_at", Date.from(Instant.parse("2026-11-03T10:15:00.789Z"))));

        assertEquals("2026-11-03T10:15:00Z", publications.current().orElseThrow().publishedAt());
    }

    @Test
    void unCurrentSinBooksIncumpleElContrato() {
        insertCurrent(new Document("_id", "CURRENT").append("version", 42));

        assertThrows(IllegalStateException.class, publications::current);
    }

    // ---- unpublished (D1, D2) ----

    @Test
    void sinNadaIndexadoNoHayNadaQuePublicar() {
        Unpublished unpublished = publications.unpublished(null);

        assertTrue(unpublished.isEmpty());
    }

    @Test
    void losLibrosDelLogQueNoEstanEnIndexBooksSonLosNuevos() {
        indexLog.markIndexed(List.of(30, 10, 20), Instant.now());
        publications.addBooks(List.of(10), 1);
        insertCurrent(new Document("_id", "CURRENT").append("version", 1).append("books", 1));

        Unpublished unpublished = publications.unpublished(1);

        assertEquals(List.of(20, 30), unpublished.indexed());
        assertEquals(List.of(), unpublished.interrupted());
    }

    @Test
    void losLibrosDeIndexBooksPorEncimaDeCurrentSonUnaPublicacionInterrumpida() {
        indexLog.markIndexed(List.of(10, 20, 30), Instant.now());
        publications.addBooks(List.of(10), 1);
        publications.addBooks(List.of(30, 20), 2);           // the publication of 2 died here

        Unpublished unpublished = publications.unpublished(1);

        assertEquals(List.of(), unpublished.indexed());
        assertEquals(List.of(20, 30), unpublished.interrupted());
        assertEquals(List.of(20, 30), unpublished.all());
    }

    @Test
    void sinCurrentTodoIndexBooksEstaInterrumpido() {
        publications.addBooks(List.of(5), 1);                 // the FIRST publication died here

        assertEquals(List.of(5), publications.unpublished(null).interrupted());
    }

    @Test
    void losIdsVuelvenOrdenadosAunqueSeIndexaranDesordenados() {
        // One call per book: MongoDB keeps them in the order they arrived, not by id.
        for (int id : List.of(300, 7, 150, 42, 999)) {
            indexLog.markIndexed(List.of(id), Instant.now());
        }

        assertEquals(List.of(7, 42, 150, 300, 999), publications.unpublished(null).indexed());
    }

    // ---- addBooks ----

    @Test
    void unLibroYaPublicadoConservaSuPrimeraVersion() {
        publications.addBooks(List.of(7), 3);
        publications.addBooks(List.of(7, 8), 5);

        assertEquals(3, indexBooks().find(new Document("_id", 7)).first().getInteger("version"));
        assertEquals(5, indexBooks().find(new Document("_id", 8)).first().getInteger("version"));
    }

    @Test
    void addBooksSinLibrosNoEscribeNada() {
        publications.addBooks(List.of(), 1);

        assertEquals(0, indexBooks().countDocuments());
    }

    @Test
    void addBooksCreaElIndiceSobreVersion() {
        publications.addBooks(List.of(1), 1);

        boolean found = false;
        for (Document index : indexBooks().listIndexes()) {
            found |= new Document("version", 1).equals(index.get("key", Document.class));
        }
        assertTrue(found, "falta el índice {version: 1} en index_books");
    }

    // ---- countVisible, booksOfVersion ----

    @Test
    void cuentaLosLibrosConVersionMenorOIgual() {
        publications.addBooks(List.of(1, 2), 1);
        publications.addBooks(List.of(3), 2);
        publications.addBooks(List.of(4), 3);

        assertEquals(0, publications.countVisible(0));
        assertEquals(2, publications.countVisible(1));
        assertEquals(3, publications.countVisible(2));
        assertEquals(4, publications.countVisible(3));
    }

    @Test
    void booksOfVersionDevuelveSoloLosDeEsaVersionOrdenados() {
        publications.addBooks(List.of(1), 1);
        publications.addBooks(List.of(9, 3), 2);
        publications.addBooks(List.of(4), 3);

        assertEquals(List.of(3, 9), publications.booksOfVersion(2));
        assertEquals(List.of(), publications.booksOfVersion(7));
    }

    // ---- switchCurrent (the fencing of contract §5) ----

    @Test
    void laPrimeraVersionCreaCurrent() {
        Publication first = new Publication(1, 3, "inverted_index", "2026-11-05T09:00:00Z");

        assertTrue(publications.switchCurrent(null, first));
        assertEquals(Optional.of(first), publications.current());
    }

    @Test
    void laPrimeraVersionFallaSiOtroYaCreoCurrent() {
        insertCurrent(new Document("_id", "CURRENT").append("version", 1).append("books", 3));

        assertFalse(publications.switchCurrent(null, new Publication(1, 9, "x", "2026-11-05T09:00:00Z")));
        assertEquals(3, publications.current().orElseThrow().books());
    }

    @Test
    void cambiaCurrentSiSigueEnLaVersionEsperada() {
        insertCurrent(new Document("_id", "CURRENT").append("version", 4).append("books", 10)
                .append("collection", "inverted_index").append("published_at", "2026-11-04T00:00:00Z"));
        Publication next = new Publication(5, 12, "inverted_index", "2026-11-05T09:00:00Z");

        assertTrue(publications.switchCurrent(4, next));
        assertEquals(Optional.of(next), publications.current());
    }

    @Test
    void noCambiaCurrentSiOtraReplicaPublicoAntes() {
        insertCurrent(new Document("_id", "CURRENT").append("version", 5).append("books", 12));

        assertFalse(publications.switchCurrent(4, new Publication(5, 99, "x", "2026-11-05T09:00:00Z")));
        assertEquals(new Publication(5, 12), publications.current().orElseThrow());
    }

    @Test
    void noCreaCurrentSiSeEsperabaUnaVersionQueNoExiste() {
        assertFalse(publications.switchCurrent(4, new Publication(5, 1, "x", "2026-11-05T09:00:00Z")));
        assertEquals(Optional.empty(), publications.current());
    }

    // ---- history (D3) ----

    @Test
    void escribeLaEntradaDelHistorialConElFormatoDelContrato() {
        publications.recordHistory(new PublishedVersion(3, List.of(4, 9), 20, "2026-11-05T09:00:00Z", "inverted_index"));

        Document doc = historyDoc(3);
        assertEquals(List.of(4, 9), doc.getList("books", Integer.class));
        assertEquals(2, doc.getInteger("books_published"));
        assertEquals(20L, doc.get("total_books", Number.class).longValue());
        assertEquals("2026-11-05T09:00:00Z", doc.getString("published_at"));
        assertEquals("inverted_index", doc.getString("collection"));
        assertTrue(publications.hasHistory(3));
        assertFalse(publications.hasHistory(4));
    }

    @Test
    void escribirDosVecesLaMismaVersionDejaUnaSolaEntrada() {
        publications.recordHistory(new PublishedVersion(3, List.of(4), 20, "2026-11-05T09:00:00Z", "inverted_index"));
        publications.recordHistory(new PublishedVersion(3, List.of(4, 9), 21, "2026-11-05T09:00:01Z", "inverted_index"));

        assertEquals(1, db.getCollection(MongoPublicationRepository.INDEX_PUBLICATIONS).countDocuments());
        assertEquals(21L, historyDoc(3).get("total_books", Number.class).longValue());
    }
}
