package es.ulpgc.bigdata.search.testsupport;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;

import java.util.concurrent.TimeUnit;

/**
 * MongoDB for the Search tests, same rules as common's MongoTestSupport: MONGO_URI or
 * mongodb://localhost:27017, and the tests are SKIPPED (not failed) when MongoDB does not
 * answer, so "mvn verify" also works without Docker. Each test class uses a database of its
 * own and drops it at the end, so it never touches search_engine.
 */
public final class MongoTestDb {

    public static final String URI = System.getenv().getOrDefault("MONGO_URI", "mongodb://localhost:27017");

    private static Boolean available;

    private MongoTestDb() {
    }

    /** Does Mongo answer? Asked once and remembered. */
    public static synchronized boolean available() {
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

    /** A database name no other test class uses. */
    public static String newDatabaseName(String prefix) {
        return "search_test_" + prefix + "_" + System.nanoTime();
    }

    public static void drop(String database) {
        try (MongoClient client = MongoClients.create(URI)) {
            client.getDatabase(database).drop();
        }
    }
}
