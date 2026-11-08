package es.ulpgc.bigdata.datamart.indexlog;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import es.ulpgc.bigdata.model.BookIds;
import es.ulpgc.bigdata.testsupport.MongoTestSupport;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Needs a running MongoDB (see MongoTestSupport). Without it these tests are SKIPPED.
 * Each test uses its own collection in search_engine_test and drops it afterwards.
 */
class MongoIndexLogTest {

    private static final Instant T1 = Instant.parse("2026-11-04T10:00:00Z");
    private static final Instant T2 = Instant.parse("2026-11-04T11:30:00Z");

    private MongoClient client;
    private MongoDatabase database;
    private String collectionName;
    private IndexLog log;                                // used as an interface, not as a concrete class
    private MongoCollection<Document> raw;               // to look at what is really in Mongo

    @BeforeEach
    void setUp() {
        assumeTrue(MongoTestSupport.available(), "MongoDB no está disponible en " + MongoTestSupport.URI);
        client = MongoClients.create(MongoTestSupport.URI);
        database = client.getDatabase(MongoTestSupport.TEST_DB);
        collectionName = MongoTestSupport.newCollectionName("index_log");
        log = new MongoIndexLog(database, collectionName);
        raw = database.getCollection(collectionName);
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            raw.drop();                                  // do not leave test collections behind
            client.close();
        }
    }

    private boolean collectionExists() {
        return database.listCollectionNames().into(new ArrayList<>()).contains(collectionName);
    }

    // --- Empty log ----------------------------------------------------------

    @Test
    void unLogVacioNoTieneLibrosNiFecha() {
        assertEquals(0, log.count());
        assertEquals(Optional.empty(), log.lastIndexedAt());
    }

    // --- The document -------------------------------------------------------

    @Test
    void cadaLibroEsUnDocumentoConSuIdYUnaFechaDeMongo() {
        log.markIndexed(List.of(1342), T1);

        Document doc = raw.find().first();
        assertNotNull(doc);
        assertEquals(new Document("_id", 1342).append("indexed_at", Date.from(T1)), doc,
                "exactamente {_id, indexed_at}, y indexed_at como fecha, no como texto");
    }

    @Test
    void laFechaSeGuardaConPrecisionDeMilisegundos() {
        Instant withNanos = Instant.parse("2026-11-04T10:00:00.123456789Z");

        log.markIndexed(List.of(1342), withNanos);

        assertEquals(Optional.of(Instant.parse("2026-11-04T10:00:00.123Z")), log.lastIndexedAt());
    }

    // --- Idempotence and $max ---------------------------------------------

    @Test
    void marcarDosVecesElMismoLibroNoLoDuplica() {
        log.markIndexed(List.of(1342), T1);
        log.markIndexed(List.of(1342), T1);

        assertEquals(1, log.count());
        assertEquals(1, raw.countDocuments());
    }

    @Test
    void reindexarUnLibroAdelantaSuFecha() {
        log.markIndexed(List.of(1342), T1);

        log.markIndexed(List.of(1342), T2);

        assertEquals(1, log.count());
        assertEquals(Optional.of(T2), log.lastIndexedAt());
    }

    @Test
    void unaEscrituraQueLlegaTardeNoRetrasaLaFecha() {
        // Two updates of the same book: the one that ended at T1 reaches MongoDB after the one
        // that ended at T2. With $set the book would go back to T1.
        log.markIndexed(List.of(1342), T2);

        log.markIndexed(List.of(1342), T1);

        assertEquals(Date.from(T2), raw.find().first().getDate("indexed_at"));
        assertEquals(Optional.of(T2), log.lastIndexedAt());
    }

    // --- Batches ------------------------------------------------------------

    @Test
    void unLoteMarcaTodosSusLibrosConLaMismaFecha() {
        log.markIndexed(List.of(84, 1342, 11), T1);

        assertEquals(3, log.count());
        assertEquals(List.of(11, 84, 1342),
                raw.find().sort(Sorts.ascending("_id")).map(d -> d.getInteger("_id")).into(new ArrayList<>()));
        raw.find().forEach(d -> assertEquals(Date.from(T1), d.getDate("indexed_at")));
    }

    @Test
    void unLoteConUnIdRepetidoLoCuentaUnaVez() {
        log.markIndexed(List.of(1342, 84, 1342), T1);

        assertEquals(2, log.count());
    }

    @Test
    void unLoteVacioNoHaceNadaNiCreaLaColeccion() {
        log.markIndexed(List.of(), T1);

        assertEquals(0, log.count());
        assertFalse(collectionExists(), "no debe hablar con Mongo para no escribir nada");
    }

    @Test
    void unIdInvalidoEnElLoteNoEscribeNingunLibro() {
        for (Integer invalid : Arrays.asList(0, -5, BookIds.MAX + 1, null)) {
            List<Integer> batch = Arrays.asList(84, invalid, 1342);

            assertThrows(IllegalArgumentException.class, () -> log.markIndexed(batch, T1),
                    "id " + invalid);
        }
        assertEquals(0, log.count(), "un id inválido se publicaría en DR-08: el lote entero se rechaza");
        assertFalse(collectionExists());
    }

    @Test
    void losLimitesDelRangoDeIdsSonValidos() {
        log.markIndexed(List.of(1, BookIds.MAX), T1);

        assertEquals(2, log.count());
    }

    // --- lastIndexedAt ------------------------------------------------------

    @Test
    void laUltimaFechaEsLaMayorDeTodoElLog() {
        log.markIndexed(List.of(84), T2);
        log.markIndexed(List.of(1342), T1);              // written later, but earlier in time

        assertEquals(Optional.of(T2), log.lastIndexedAt());
    }

    @Test
    void laPrimeraEscrituraCreaElIndiceSobreIndexedAt() {
        log.markIndexed(List.of(1342), T1);

        List<Document> keys = raw.listIndexes().map(i -> i.get("key", Document.class)).into(new ArrayList<>());
        assertTrue(keys.contains(new Document("indexed_at", 1)), "índices: " + keys);
    }

    @Test
    void laUltimaFechaSeLeeDelIndiceSinRecorrerLaColeccion() {
        List<Integer> ids = new ArrayList<>();
        for (int id = 1; id <= 500; id++) {
            ids.add(id);
        }
        log.markIndexed(ids, T1);
        log.markIndexed(List.of(7), T2);

        Document explain = raw.find()
                .sort(Sorts.descending("indexed_at"))
                .projection(new Document("indexed_at", 1))
                .limit(1)
                .explain();
        String plan = explain.get("queryPlanner", Document.class).toJson();
        assertTrue(plan.contains("IXSCAN"), "debe usar el índice: " + plan);
        assertFalse(plan.contains("COLLSCAN"), "no debe recorrer la colección: " + plan);
        assertEquals(Optional.of(T2), log.lastIndexedAt());
    }

    // --- Another program wrote the log ---------------------------------------

    @Test
    void cuentaYLeeDocumentosEscritosPorOtroPrograma() {
        // As the Control or a person in mongosh would see them: the same two fields.
        raw.insertOne(new Document("_id", 5).append("indexed_at", Date.from(T2)));

        log.markIndexed(List.of(6), T1);

        assertEquals(2, log.count());
        assertEquals(Optional.of(T2), log.lastIndexedAt());
        assertEquals(Set.of(5, 6), log.indexedIds());
    }

    // --- The ids (recovery of the Control, CF-10) ----------------------------

    @Test
    void losIdsSonLosDeTodosLosLibrosDelLog() {
        assertEquals(Set.of(), log.indexedIds());
        log.markIndexed(List.of(1342, 84), T1);
        log.markIndexed(List.of(84, BookIds.MAX), T2);

        assertEquals(Set.of(84, 1342, BookIds.MAX), log.indexedIds());
    }

    @Test
    void unIdGuardadoComoInt64SeLeeIgual() {
        // mongosh writes 5 as a double and NumberLong(6) as int64: both are a book id
        raw.insertOne(new Document("_id", 5.0).append("indexed_at", Date.from(T1)));
        raw.insertOne(new Document("_id", 6L).append("indexed_at", Date.from(T1)));

        assertEquals(Set.of(5, 6), log.indexedIds());
    }
}
