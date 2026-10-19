package es.ulpgc.bigdata.datamart.index;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;

import java.util.concurrent.TimeUnit;

/**
 * Where the tests find MongoDB: MONGO_URI environment variable, or mongodb://localhost:27017.
 * They use the "search_engine_test" database and a new collection per test, so they never
 * touch the real index. Without MongoDB the Mongo tests are SKIPPED, not failed, so that
 * "mvn verify" also works without Docker (moved here from Stage 1's MongoInvertedIndexTest).
 */
final class MongoTestSupport {

    static final String URI = System.getenv().getOrDefault("MONGO_URI", "mongodb://localhost:27017");
    static final String TEST_DB = "search_engine_test";

    private static Boolean available;

    private MongoTestSupport() {
    }

    /** Does Mongo answer? It is asked once and remembered. */
    static synchronized boolean available() {
        if (available == null) {
            MongoClientSettings quick = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(URI))
                    .applyToClusterSettings(b -> b.serverSelectionTimeout(1500, TimeUnit.MILLISECONDS))
                    .build();
            try (MongoClient probe = MongoClients.create(quick)) {
                probe.getDatabase("admin").runCommand(new Document("ping", 1));
                available = true;
            } catch (RuntimeException e) {
                available = false;
            }
        }
        return available;
    }

    /** A collection name no other test uses. */
    static String newCollectionName(String prefix) {
        return prefix + "_" + System.nanoTime();
    }
}
