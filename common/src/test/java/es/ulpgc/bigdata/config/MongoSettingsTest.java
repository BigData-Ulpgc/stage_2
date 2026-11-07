package es.ulpgc.bigdata.config;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import es.ulpgc.bigdata.datamart.index.MongoInvertedIndex;
import es.ulpgc.bigdata.testsupport.MongoTestSupport;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class MongoSettingsTest {

    @Test
    void thePoolHasTheRequestedSizeAndABoundedWait() {
        MongoSettings settings = new MongoSettings("mongodb://localhost:27017", "db", "c");

        var pool = settings.pooledSettings(7).getConnectionPoolSettings();

        assertEquals(7, pool.getMaxSize());
        assertEquals(MongoSettings.POOL_WAIT_SECONDS, pool.getMaxWaitTime(java.util.concurrent.TimeUnit.SECONDS));
    }

    @Test
    void defaultsAreTheStage1Ones() {
        MongoSettings settings = MongoSettings.fromEnv(Env.of(Map.of()));

        assertEquals("mongodb://localhost:27017", settings.uri());
        assertEquals("search_engine", settings.database());
        assertEquals("inverted_index", settings.collection());
    }

    @Test
    void environmentWins() {
        MongoSettings settings = MongoSettings.fromEnv(Env.of(Map.of(
                "MONGO_URI", "mongodb://mongo:27017",
                "MONGO_DATABASE", "search_engine_test",
                "MONGO_COLLECTION", "idx")));

        assertEquals("mongodb://mongo:27017", settings.uri());
        assertEquals("search_engine_test", settings.database());
        assertEquals("idx", settings.collection());
    }

    @Test
    void openClientConnectsToTheConfiguredMongo() {
        assumeTrue(MongoTestSupport.available(), "MongoDB no está disponible en " + MongoTestSupport.URI);
        MongoSettings settings = MongoSettings.fromEnv(Env.of(Map.of("MONGO_URI", MongoTestSupport.URI)));

        try (MongoClient client = settings.openClient()) {
            Document pong = client.getDatabase("admin").runCommand(new Document("ping", 1));
            assertEquals(1.0, pong.get("ok", Number.class).doubleValue());
        }
    }

    @Test
    void openIndexOnTheServiceClientUsesTheConfiguredDatabaseAndCollection() {
        assumeTrue(MongoTestSupport.available(), "MongoDB no está disponible en " + MongoTestSupport.URI);
        String collection = MongoTestSupport.newCollectionName("settings");
        MongoSettings settings = new MongoSettings(MongoTestSupport.URI, MongoTestSupport.TEST_DB, collection);

        try (MongoClient client = settings.openClient()) {
            try (MongoInvertedIndex index = settings.openIndex(client)) {
                index.addDocument(10, Set.of("boat"));
                index.flush();
            }
            // Closing the index left the client open, and the term is where the settings say.
            MongoCollection<Document> stored = client.getDatabase(MongoTestSupport.TEST_DB).getCollection(collection);
            assertEquals(1, stored.countDocuments(new Document("term", "boat")));
            stored.drop();
        }
    }
}
