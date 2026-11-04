package es.ulpgc.bigdata.datamart.index;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MongoInvertedIndex on a shared client when MongoDB does NOT answer (DR-05, D4): the
 * service must be able to start anyway, and fail only in the operations that need MongoDB.
 *
 * Unlike MongoInvertedIndexTest, it needs no running MongoDB: the client points to a port
 * where nobody listens, so it also runs without Docker.
 */
class MongoInvertedIndexUnavailableTest {

    /** Port 1: nothing listens there, as if MongoDB were down. */
    private static final String NOBODY_LISTENS = "mongodb://127.0.0.1:1";

    private MongoClient downClient;

    @BeforeEach
    void setUp() {
        downClient = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(NOBODY_LISTENS))
                // Fail in 0.2 s instead of the 3 s of the services: the test stays fast.
                .applyToClusterSettings(b -> b.serverSelectionTimeout(200, TimeUnit.MILLISECONDS))
                .build());
    }

    @AfterEach
    void tearDown() {
        downClient.close();
    }

    @Test
    void conClienteCompartidoYMongoCaidoSeConstruyePeroFlushFalla() {
        MongoInvertedIndex index = assertDoesNotThrow(
                () -> new MongoInvertedIndex(downClient.getDatabase("search_engine_test"), "inverted_index"),
                "el constructor no debe necesitar a Mongo");

        index.addDocument(10, Set.of("boat"));               // only in memory: it works too

        // The first operation that needs MongoDB is the one that fails (the API turns it into a 503).
        assertThrows(MongoException.class, index::flush);

        index.close();
    }
}
