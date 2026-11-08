package es.ulpgc.bigdata.datamart.indexlog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import es.ulpgc.bigdata.model.BookIds;
import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * IndexLog on MongoDB: collection "index_log" of the search_engine database, one document
 * per book (plan §6.5):
 *
 * <pre>
 *   {_id: 1342, indexed_at: ISODate("2026-11-04T17:02:13.517Z")}
 * </pre>
 *
 *  - The book id IS the _id, as in "books": Mongo keeps it unique, so a book is in the log once.
 *  - indexed_at is a MongoDB date, not text: it sorts and compares as a time, and mongosh,
 *    Python or the Control read it as a date.
 *  - An index on indexed_at answers lastIndexedAt by reading ONE entry of the index instead of
 *    the whole collection. It is ascending, and MongoDB walks it backwards for the descending
 *    sort. It is created at the first write, not in the constructor (see below).
 *
 * Like the other repositories of the services, it does not own the MongoClient: it receives a
 * database of the client the service shares, and it does not talk to MongoDB until it is used,
 * so the service starts while MongoDB is down. Its errors are the driver's MongoException,
 * which the APIs already answer with 503.
 *
 * Thread-safe: the driver's collection is; the only state is a flag that at worst makes two
 * threads create the same index, which MongoDB accepts (createIndex is idempotent).
 */
public class MongoIndexLog implements IndexLog {

    public static final String DEFAULT_COLLECTION = "index_log";

    static final String ID = "_id";
    static final String INDEXED_AT = "indexed_at";

    private static final UpdateOptions UPSERT = new UpdateOptions().upsert(true);

    private final MongoCollection<Document> collection;

    /** false until this object has created the index on indexed_at. */
    private volatile boolean indexReady = false;

    /** The contract's collection, "index_log". */
    public MongoIndexLog(MongoDatabase database) {
        this(database, DEFAULT_COLLECTION);
    }

    /** Configurable collection (the tests use a new one each time). */
    public MongoIndexLog(MongoDatabase database, String collectionName) {
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(collectionName, "collectionName");
        this.collection = database.getCollection(collectionName);
    }

    /**
     * One upsert per book with $max: inserts the book if it is not there; if it is, its
     * indexed_at moves to the new time only if the new time is later. With $set, two updates of
     * the same book whose writes reach MongoDB in the opposite order to their end times would
     * move indexed_at BACKWARDS; $max makes the result independent of that order. All in ONE
     * unordered bulkWrite (a single round trip): the ids are different, so the order does not
     * matter.
     *
     * The ids are checked, and duplicates removed, BEFORE sending anything: an invalid id
     * would later be published (DR-08), so a batch with one writes nothing.
     */
    @Override
    public void markIndexed(Collection<Integer> bookIds, Instant indexedAt) {
        Objects.requireNonNull(bookIds, "bookIds");
        Objects.requireNonNull(indexedAt, "indexedAt");
        TreeSet<Integer> ids = new TreeSet<>();
        for (Integer id : bookIds) {
            if (id == null || !BookIds.isValid(id)) {
                throw new IllegalArgumentException("book_id fuera de rango (1.." + BookIds.MAX + "): " + id);
            }
            ids.add(id);
        }
        if (ids.isEmpty()) {
            return;                                     // bulkWrite rejects an empty list
        }
        if (!indexReady) {
            collection.createIndex(Indexes.ascending(INDEXED_AT));
            indexReady = true;                          // only if MongoDB created it
        }
        // A MongoDB date (like java.util.Date) has millisecond precision: Date.from drops the
        // rest, so what is read back is the time truncated to the millisecond.
        Date at = Date.from(indexedAt);
        List<WriteModel<Document>> writes = new ArrayList<>(ids.size());
        for (int id : ids) {
            writes.add(new UpdateOneModel<>(Filters.eq(ID, id), Updates.max(INDEXED_AT, at), UPSERT));
        }
        collection.bulkWrite(writes, new BulkWriteOptions().ordered(false));
    }

    /** Exact, not estimated: it is what GET /index/status shows as books_indexed. */
    @Override
    public long count() {
        return collection.countDocuments();
    }

    /** The first entry of the indexed_at index, walked backwards; only that field is read. */
    @Override
    public Optional<Instant> lastIndexedAt() {
        Document last = collection.find()
                .sort(Sorts.descending(INDEXED_AT))
                .projection(Projections.include(INDEXED_AT))
                .limit(1)
                .first();
        return Optional.ofNullable(last)
                .map(doc -> doc.getDate(INDEXED_AT))
                .map(Date::toInstant);
    }

    /** Only the _id travels: MongoDB answers it from the _id index, without reading the documents. */
    @Override
    public Set<Integer> indexedIds() {
        Set<Integer> ids = new TreeSet<>();
        collection.find()
                .projection(Projections.include(ID))
                .forEach(doc -> ids.add(doc.get(ID, Number.class).intValue()));
        return ids;
    }
}
