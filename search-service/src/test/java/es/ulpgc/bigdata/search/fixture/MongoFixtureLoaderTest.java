package es.ulpgc.bigdata.search.fixture;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import es.ulpgc.bigdata.datamart.indexlog.MongoIndexLog;
import es.ulpgc.bigdata.search.testsupport.MongoTestDb;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** What the fixture writes must match what Indexing writes: /index/status reads index_log. */
class MongoFixtureLoaderTest {

    private static final int INDEXED_NOT_PUBLISHED = 2701;

    private String dbName;
    private MongoClient client;
    private MongoDatabase db;

    @BeforeEach
    void setUp() {
        assumeTrue(MongoTestDb.available(), "MongoDB is not running: start it with docker compose up -d mongo");
        dbName = MongoTestDb.newDatabaseName("fixture");
        client = MongoClients.create(MongoTestDb.URI);
        db = client.getDatabase(dbName);
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            db.drop();
            client.close();
        }
    }

    @Test
    void everyIndexedBookIsInTheLogPublishedOrNot() {
        TestBooks.load(MongoTestDb.URI, dbName);

        var log = db.getCollection(MongoIndexLog.DEFAULT_COLLECTION);
        assertEquals(db.getCollection("books").countDocuments(), log.countDocuments());
        assertEquals(1, log.countDocuments(Filters.eq("_id", INDEXED_NOT_PUBLISHED)));
    }

    @Test
    void loadingAgainStartsFromAnEmptyLog() {
        TestBooks.load(MongoTestDb.URI, dbName);
        db.getCollection(MongoIndexLog.DEFAULT_COLLECTION).insertOne(new org.bson.Document("_id", 999));

        TestBooks.load(MongoTestDb.URI, dbName);

        assertEquals(0, db.getCollection(MongoIndexLog.DEFAULT_COLLECTION).countDocuments(Filters.eq("_id", 999)));
    }
}
