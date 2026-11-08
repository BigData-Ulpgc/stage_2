package es.ulpgc.bigdata.datamart.indexlog;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MongoIndexLog when MongoDB does NOT answer: the service must start anyway (the constructor
 * does not talk to MongoDB) and only the operations that need it fail, with the MongoException
 * the APIs answer with 503. Needs no running MongoDB: the client points to a port where nobody
 * listens, so it also runs without Docker.
 */
class MongoIndexLogUnavailableTest {

    /** Port 1: nothing listens there, as if MongoDB were down. */
    private static final String NOBODY_LISTENS = "mongodb://127.0.0.1:1";

    private MongoClient downClient;

    @BeforeEach
    void setUp() {
        downClient = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(NOBODY_LISTENS))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(200, TimeUnit.MILLISECONDS))
                .build());
    }

    @AfterEach
    void tearDown() {
        downClient.close();
    }

    @Test
    void conMongoCaidoSeConstruyePeroSusOperacionesFallan() {
        IndexLog log = assertDoesNotThrow(
                () -> new MongoIndexLog(downClient.getDatabase("search_engine_test")),
                "el constructor no debe necesitar a Mongo");

        assertThrows(MongoException.class, () -> log.markIndexed(List.of(1342), Instant.now()));
        assertThrows(MongoException.class, log::count);
        assertThrows(MongoException.class, log::lastIndexedAt);
        assertThrows(MongoException.class, log::indexedIds);
    }

    @Test
    void unLoteVacioNoNecesitaAMongo() {
        IndexLog log = new MongoIndexLog(downClient.getDatabase("search_engine_test"));

        assertDoesNotThrow(() -> log.markIndexed(List.of(), Instant.now()));
    }
}
