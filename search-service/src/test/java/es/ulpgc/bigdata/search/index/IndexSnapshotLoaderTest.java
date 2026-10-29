package es.ulpgc.bigdata.search.index;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import es.ulpgc.bigdata.datamart.metadata.MongoMetadataRepository;
import es.ulpgc.bigdata.model.BookMetadata;
import es.ulpgc.bigdata.model.YearSource;
import es.ulpgc.bigdata.search.fixture.TestBooks;
import es.ulpgc.bigdata.search.testsupport.MongoTestDb;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** The loader against a real MongoDB with the TestBooks fixture (skipped without MongoDB). */
class IndexSnapshotLoaderTest {

    private String dbName;
    private MongoClient client;
    private MongoDatabase db;
    private IndexSnapshotLoader loader;

    @BeforeEach
    void setUp() {
        assumeTrue(MongoTestDb.available(), "MongoDB is not running: start it with docker compose up -d mongo");
        dbName = MongoTestDb.newDatabaseName("loader");
        client = MongoClients.create(MongoTestDb.URI);
        db = client.getDatabase(dbName);
        loader = new IndexSnapshotLoader(db, new MongoMetadataRepository(db));
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            db.drop();
            client.close();
        }
    }

    @Test
    void nothingPublishedYetGivesNoSnapshot() {
        assertTrue(loader.loadCurrent().isEmpty());
    }

    @Test
    void loadsExactlyTheBooksPublishedUpToCurrent() {
        TestBooks.load(MongoTestDb.URI, dbName);

        IndexSnapshot snapshot = loader.loadCurrent().orElseThrow();

        assertEquals(TestBooks.CURRENT_VERSION, snapshot.version());
        assertEquals("inverted_index", snapshot.collection());
        assertEquals(TestBooks.VISIBLE_IDS, snapshot.books().stream().map(BookMetadata::bookId).toList());
        assertFalse(snapshot.isPublished(2701), "indexed but never published");
        assertFalse(snapshot.isPublished(11), "published with version 3 while CURRENT is still 2");
    }

    @Test
    void metadataFollowsTheContractRules() {
        TestBooks.load(MongoTestDb.URI, dbName);

        IndexSnapshot snapshot = loader.loadCurrent().orElseThrow();

        BookMetadata pride = snapshot.book(1342).orElseThrow();
        assertEquals("Pride and Prejudice", pride.title());
        assertEquals("Jane Austen", pride.author());
        assertEquals("en", pride.language());
        assertEquals(1998, pride.year());                    // ebook year: no "Original publication" line
        assertEquals(YearSource.RELEASE_DATE, pride.yearSource());
        assertEquals("June 1, 1998", pride.releaseDate());

        BookMetadata castle = snapshot.book(67979).orElseThrow();
        assertEquals(1926, castle.year());                   // year of the work
        assertEquals(YearSource.ORIGINAL_PUBLICATION, castle.yearSource());

        assertEquals("fr", snapshot.book(17489).orElseThrow().language());
    }

    @Test
    void theFixtureIsDeterministic() {
        TestBooks.load(MongoTestDb.URI, dbName);
        IndexSnapshot first = loader.loadCurrent().orElseThrow();
        List<Document> firstIndex = db.getCollection("inverted_index").find().sort(new Document("term", 1))
                .projection(new Document("_id", 0)).into(new java.util.ArrayList<>());

        TestBooks.load(MongoTestDb.URI, dbName);
        IndexSnapshot second = loader.loadCurrent().orElseThrow();
        List<Document> secondIndex = db.getCollection("inverted_index").find().sort(new Document("term", 1))
                .projection(new Document("_id", 0)).into(new java.util.ArrayList<>());

        assertEquals(List.copyOf(first.books()), List.copyOf(second.books()));
        assertEquals(firstIndex, secondIndex);
    }

    @Test
    void aPublishedBookWithoutMetadataStaysVisible() {
        db.getCollection(IndexSnapshotLoader.INDEX_META).insertOne(new Document("_id", "CURRENT")
                .append("version", 1).append("collection", "inverted_index"));
        db.getCollection(IndexSnapshotLoader.INDEX_BOOKS).insertOne(new Document("_id", 5).append("version", 1));

        IndexSnapshot snapshot = loader.loadCurrent().orElseThrow();

        assertTrue(snapshot.isPublished(5));
        assertNull(snapshot.book(5).orElseThrow().title());
    }

    @Test
    void aCurrentWithoutCollectionIsAnError() {
        db.getCollection(IndexSnapshotLoader.INDEX_META).insertOne(new Document("_id", "CURRENT").append("version", 1));

        assertThrows(IllegalStateException.class, loader::loadCurrent);
    }
}
