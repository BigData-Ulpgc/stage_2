package es.ulpgc.bigdata.datamart.publication;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import es.ulpgc.bigdata.testsupport.MongoTestSupport;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Needs a running MongoDB (see MongoTestSupport); without it the tests are SKIPPED. Each test
 * uses a database of its own, because the collection names are fixed by the contract.
 */
class MongoPublishedBooksTest {

    private MongoClient client;
    private MongoDatabase database;
    private MongoPublishedBooks published;

    @BeforeEach
    void setUp() {
        assumeTrue(MongoTestSupport.available(), "MongoDB is not available at " + MongoTestSupport.URI);
        client = MongoClients.create(MongoTestSupport.URI);
        database = client.getDatabase(MongoTestSupport.newCollectionName("published_books_test"));
        published = new MongoPublishedBooks(database);
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            database.drop();
            client.close();
        }
    }

    @Test
    void nothingPublishedIsEmpty() {
        assertEquals(Set.of(), published.publishedIds());
        // index_books without CURRENT: a first publication that never switched
        database.getCollection("index_books").insertOne(new Document("_id", 1342).append("version", 1));
        assertEquals(Set.of(), published.publishedIds());
    }

    @Test
    void onlyTheBooksUpToCurrentArePublished() {
        current(2);
        database.getCollection("index_books").insertMany(List.of(
                new Document("_id", 1342).append("version", 1),
                new Document("_id", 84).append("version", 2),
                new Document("_id", 11).append("version", 3),          // publication 3 in progress
                new Document("_id", 5L).append("version", 2L)));        // int64, as another program may write
        assertEquals(Set.of(5, 84, 1342), published.publishedIds());
    }

    @Test
    void aCurrentWithoutVersionIsABug() {
        database.getCollection("index_meta").insertOne(new Document("_id", "CURRENT").append("books", 3));
        assertThrows(IllegalStateException.class, published::publishedIds);
    }

    @Test
    void withMongoDownItIsBuiltButTheReadFails() {
        try (MongoClient down = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://127.0.0.1:1"))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(300, TimeUnit.MILLISECONDS))
                .build())) {
            MongoPublishedBooks unreachable = new MongoPublishedBooks(down.getDatabase("search_engine_test"));
            assertThrows(MongoException.class, unreachable::publishedIds);
        }
    }

    private void current(int version) {
        database.getCollection("index_meta").insertOne(new Document("_id", "CURRENT").append("version", version)
                .append("collection", "inverted_index").append("books", 0));
    }
}
