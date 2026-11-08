package es.ulpgc.bigdata.indexing;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** index_meta REBUILD and the collections of the database, against a real MongoDB. Skipped without one. */
class MongoRebuildRepositoryTest {

    private static final String URI = System.getenv().getOrDefault("MONGO_URI", "mongodb://localhost:27017");

    private MongoClient client;
    private MongoDatabase db;
    private MongoRebuildRepository repository;

    @BeforeEach
    void setUp() {
        client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(URI))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(1500, TimeUnit.MILLISECONDS))
                .build());
        assumeTrue(mongoAnswers(), "MongoDB no está disponible en " + URI + ": test saltado");
        db = client.getDatabase("search_engine_test_dr10_" + System.nanoTime());
        repository = new MongoRebuildRepository(db);
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

    @Test
    void sinReconstruccionesNoHayUltima() {
        assertEquals(Optional.empty(), repository.last());
    }

    @Test
    void guardaYLeeLaUltimaConSusNulos() {
        Instant started = Instant.parse("2026-11-06T10:00:00.123Z");
        Rebuild running = new Rebuild(Rebuild.State.RUNNING, started, null, 0, 200, null, "inverted_index_r01", null, null);
        repository.save(running);
        assertEquals(Optional.of(running), repository.last());

        Rebuild done = running.withProgress(200, 200, "inverted_index_r01")
                .done(started.plusSeconds(90), "inverted_index", started.plusSeconds(150));
        repository.save(done);

        assertEquals(Optional.of(done), repository.last(), "la reemplaza: una sola, la última");
        assertEquals(1, db.getCollection("index_meta").countDocuments(new Document("_id", "REBUILD")));
    }

    @Test
    void noTocaCurrent() {
        assertTrue(new MongoPublicationRepository(db).switchCurrent(null,
                new Publication(1, 0, "inverted_index", "2026-11-06T10:00:00Z")));

        repository.save(new Rebuild(Rebuild.State.RUNNING, Instant.now(), null, 0, 0, null, "inverted_index_r01",
                null, null));

        assertEquals(1, new MongoPublicationRepository(db).current().orElseThrow().version());
    }

    @Test
    void listaYBorraColecciones() {
        db.getCollection("inverted_index_r01").insertOne(new Document("term", "x"));

        assertTrue(repository.collections().contains("inverted_index_r01"));
        repository.drop("inverted_index_r01");
        repository.drop("no_existe");                       // nothing

        assertFalse(repository.collections().contains("inverted_index_r01"));
        assertNotNull(repository.collections());
    }
}
