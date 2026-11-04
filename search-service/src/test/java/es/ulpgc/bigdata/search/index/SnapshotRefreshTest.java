package es.ulpgc.bigdata.search.index;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import es.ulpgc.bigdata.datamart.metadata.MongoMetadataRepository;
import es.ulpgc.bigdata.search.fixture.TestBooks;
import es.ulpgc.bigdata.search.testsupport.MongoTestDb;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * A running replica moving to a new version without a restart. TestBooks starts at CURRENT = 2
 * with book 11 written in index_books as version 3: a publication stopped before step 2.
 */
class SnapshotRefreshTest {

    private static final int STOPPED_HALFWAY = 11;
    private static final int NOT_PUBLISHED = 2701;
    private static final int ALREADY_PUBLISHED = 1342;

    private String dbName;
    private MongoClient client;
    private MongoDatabase db;
    private SnapshotHolder holder;

    @BeforeEach
    void setUp() {
        assumeTrue(MongoTestDb.available(), "MongoDB is not running: start it with docker compose up -d mongo");
        dbName = MongoTestDb.newDatabaseName("refresh");
        TestBooks.load(MongoTestDb.URI, dbName);
        client = MongoClients.create(MongoTestDb.URI);
        db = client.getDatabase(dbName);
        holder = new SnapshotHolder(new IndexSnapshotLoader(db, new MongoMetadataRepository(db)));
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            db.drop();
            client.close();
        }
    }

    @Test
    void withoutAPublicationTheSameSnapshotKeepsBeingServed() {
        IndexSnapshot first = holder.reload().orElseThrow();

        assertSame(first, holder.reload().orElseThrow());
    }

    @Test
    void aPublicationStoppedBeforeSwitchingCurrentStaysInvisible() {
        IndexSnapshot snapshot = holder.reload().orElseThrow();

        assertEquals(TestBooks.CURRENT_VERSION, snapshot.version());
        assertFalse(snapshot.isPublished(STOPPED_HALFWAY));
    }

    @Test
    void switchingCurrentMakesTheNewBooksVisibleAndLeavesTheOldSnapshotIntact() {
        IndexSnapshot old = holder.reload().orElseThrow();

        switchCurrent(3, "inverted_index");
        IndexSnapshot updated = holder.reload().orElseThrow();

        assertEquals(3, updated.version());
        assertTrue(updated.isPublished(STOPPED_HALFWAY));
        assertEquals(TestBooks.VISIBLE_IDS.size() + 1, updated.bookCount());
        // A request that took the old snapshot before the switch keeps a consistent view.
        assertEquals(TestBooks.CURRENT_VERSION, old.version());
        assertFalse(old.isPublished(STOPPED_HALFWAY));
    }

    @Test
    void bothPublicationStepsInOrderExposeABookThatWasNotPublished() {
        holder.reload();

        db.getCollection(IndexSnapshotLoader.INDEX_BOOKS)
                .insertOne(new Document("_id", NOT_PUBLISHED).append("version", 4));
        assertFalse(holder.reload().orElseThrow().isPublished(NOT_PUBLISHED), "step 1 alone must not show it");

        switchCurrent(4, "inverted_index");
        assertTrue(holder.reload().orElseThrow().isPublished(NOT_PUBLISHED));
    }

    @Test
    void movingForwardReusesTheMetadataInMemoryAndReadsOnlyTheNewBooks() {
        holder.reload();
        changeTitleInMongo(ALREADY_PUBLISHED, "changed in MongoDB");

        switchCurrent(3, "inverted_index");
        IndexSnapshot updated = holder.reload().orElseThrow();

        assertEquals("Pride and Prejudice", updated.book(ALREADY_PUBLISHED).orElseThrow().title());
        assertEquals("Alice's Adventures in Wonderland", updated.book(STOPPED_HALFWAY).orElseThrow().title());
    }

    @Test
    void aRebuildIntoAnotherCollectionReadsAllTheMetadataAgain() {
        holder.reload();
        changeTitleInMongo(ALREADY_PUBLISHED, "changed in MongoDB");

        switchCurrent(3, "inverted_index_r1");
        IndexSnapshot updated = holder.reload().orElseThrow();

        assertEquals("inverted_index_r1", updated.collection());
        assertEquals("changed in MongoDB", updated.book(ALREADY_PUBLISHED).orElseThrow().title());
    }

    @Test
    void aFailedCheckKeepsTheSnapshotAndTheNextOneRecovers() {
        IndexSnapshot served = holder.reload().orElseThrow();
        try (SnapshotRefresher refresher = new SnapshotRefresher(holder, Duration.ofHours(1))) {
            // CURRENT without a collection makes the reload throw, as a MongoDB failure would.
            db.getCollection(IndexSnapshotLoader.INDEX_META).updateOne(
                    Filters.eq("_id", IndexSnapshotLoader.CURRENT), Updates.unset("collection"));

            refresher.refreshOnce();
            assertSame(served, holder.current().orElseThrow());

            switchCurrent(3, "inverted_index");
            refresher.refreshOnce();
            assertEquals(3, holder.current().orElseThrow().version());
        }
    }

    @Test
    void theRunningRefresherPicksUpANewVersionByItself() throws Exception {
        holder.reload();
        try (SnapshotRefresher refresher = new SnapshotRefresher(holder, Duration.ofMillis(50))) {
            refresher.start();

            switchCurrent(3, "inverted_index");

            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (holder.current().orElseThrow().version() != 3 && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(3, holder.current().orElseThrow().version());
        }
    }

    private void switchCurrent(int version, String collection) {
        db.getCollection(IndexSnapshotLoader.INDEX_META).updateOne(
                Filters.eq("_id", IndexSnapshotLoader.CURRENT),
                Updates.combine(Updates.set("version", version), Updates.set("collection", collection)));
    }

    private void changeTitleInMongo(int bookId, String title) {
        db.getCollection("books").updateOne(Filters.eq("_id", bookId), Updates.set("title", title));
    }
}
