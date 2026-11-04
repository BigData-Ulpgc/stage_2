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

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Reading CURRENT (contract §6.5) from a real MongoDB. index_meta has a fixed name, so each
 * test uses a database of its own, dropped afterwards: no interference with Search's tests,
 * which write their own CURRENT in search_engine_test. Skipped if there is no MongoDB.
 */
class MongoPublicationRepositoryTest {

    private static final String URI = System.getenv().getOrDefault("MONGO_URI", "mongodb://localhost:27017");

    private MongoClient client;
    private MongoDatabase db;
    private MongoPublicationRepository publications;

    @BeforeEach
    void setUp() {
        client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(URI))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(1500, TimeUnit.MILLISECONDS))
                .build());
        assumeTrue(mongoAnswers(), "MongoDB no está disponible en " + URI + ": test saltado");
        db = client.getDatabase("search_engine_test_pub_" + System.nanoTime());
        publications = new MongoPublicationRepository(db);
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

    @Test
    void sinCurrentNoHayNadaPublicado() {
        assertEquals(Optional.empty(), publications.current());
    }

    @Test
    void leeVersionYBooksDeCurrentConElFormatoDelContrato() {
        // The same document Search's MongoFixtureLoader writes (§6.5).
        insertCurrent(new Document("_id", "CURRENT")
                .append("version", 42)
                .append("collection", "inverted_index")
                .append("books", 187)
                .append("published_at", "2026-11-03T10:15:00Z"));

        assertEquals(Optional.of(new Publication(42, 187)), publications.current());
    }

    @Test
    void leeNumerosGuardadosComoInt64PorOtroPrograma() {
        insertCurrent(new Document("_id", "CURRENT").append("version", 42L).append("books", 187L));

        assertEquals(Optional.of(new Publication(42, 187)), publications.current());
    }

    @Test
    void unCurrentSinBooksIncumpleElContrato() {
        insertCurrent(new Document("_id", "CURRENT").append("version", 42));

        assertThrows(IllegalStateException.class, publications::current);
    }
}
