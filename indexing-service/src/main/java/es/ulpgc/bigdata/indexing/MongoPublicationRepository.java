package es.ulpgc.bigdata.indexing;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import es.ulpgc.bigdata.datamart.publication.MongoPublishedBooks;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import es.ulpgc.bigdata.datamart.indexlog.MongoIndexLog;
import org.bson.Document;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;

/**
 * PublicationRepository on MongoDB (contract §6.5), on the database of the client the service
 * shares, like the other repositories: it does not own the client and does not talk to MongoDB
 * until it is used.
 *
 * <pre>
 *   index_meta          {_id: "CURRENT", version: 42, collection: "inverted_index", books: 187, published_at: "2026-11-03T10:15:00Z"}
 *   index_books         {_id: 1342, version: 41}
 *   index_publications  {_id: 42, books: [...], books_published: 3, total_books: 187, published_at: "...", collection: "..."}
 * </pre>
 *
 * The same document and field names Search reads (IndexSnapshotLoader), so both services see
 * the same published version. Numbers are read as Number: another program may store int64.
 * published_at is an ISO-8601 text (D6): what Search's fixture writes, what the IndexPublished
 * event carries and what the API returns; nothing compares it inside MongoDB.
 */
public final class MongoPublicationRepository implements PublicationRepository {

    // The names the Control reads too (CF-10): defined once, in common's MongoPublishedBooks.
    public static final String INDEX_META = MongoPublishedBooks.INDEX_META;
    public static final String INDEX_BOOKS = MongoPublishedBooks.INDEX_BOOKS;
    public static final String INDEX_PUBLICATIONS = "index_publications";
    public static final String CURRENT = MongoPublishedBooks.CURRENT;

    private static final String ID = "_id";
    private static final String VERSION = "version";
    private static final String BOOKS = "books";
    private static final String COLLECTION = "collection";
    private static final String PUBLISHED_AT = "published_at";
    private static final UpdateOptions UPSERT = new UpdateOptions().upsert(true);
    /** Ids per $in of visibleAmong: one query stays small whatever the backlog. */
    static final int IN_BATCH = 1_000;

    private final MongoCollection<Document> indexMeta;
    private final MongoCollection<Document> indexBooks;
    private final MongoCollection<Document> history;
    private final MongoCollection<Document> indexLog;

    /** false until this object has created the index on index_books.version. */
    private volatile boolean versionIndexReady = false;

    public MongoPublicationRepository(MongoDatabase database) {
        Objects.requireNonNull(database, "database");
        this.indexMeta = database.getCollection(INDEX_META);
        this.indexBooks = database.getCollection(INDEX_BOOKS);
        this.history = database.getCollection(INDEX_PUBLICATIONS);
        this.indexLog = database.getCollection(MongoIndexLog.DEFAULT_COLLECTION);
    }

    /**
     * One read of one document by _id, the cheapest query MongoDB has.
     *
     * @throws IllegalStateException if CURRENT exists but does not follow the contract (a bug
     *                               of whoever wrote it, not a temporary failure: the API answers 500)
     */
    @Override
    public Optional<Publication> current() {
        Document current = indexMeta.find(Filters.eq(ID, CURRENT)).first();
        if (current == null) {
            return Optional.empty();
        }
        Number version = current.get(VERSION, Number.class);
        Number books = current.get(BOOKS, Number.class);
        if (version == null || books == null) {
            throw new IllegalStateException("index_meta CURRENT debe tener version y books: " + current.toJson());
        }
        return Optional.of(new Publication(version.intValue(), books.longValue(),
                current.get(COLLECTION) instanceof String collection ? collection : null,
                isoText(current.get(PUBLISHED_AT))));
    }

    /** published_at as written by Indexing (text), or as a date by another program. */
    private static String isoText(Object value) {
        if (value instanceof String text) {
            return text;
        }
        if (value instanceof Date date) {
            return date.toInstant().truncatedTo(ChronoUnit.SECONDS).toString();
        }
        return null;
    }

    /**
     * D1: the books of index_log without a partner in index_books, found by MongoDB itself with
     * a $lookup by _id (an index lookup per book of index_log) in ONE query: only the new ids
     * travel back. D2: the books of index_books above CURRENT, through the index on version.
     */
    @Override
    public Unpublished unpublished(Integer currentVersion) {
        List<Integer> indexed = new ArrayList<>();
        indexLog.aggregate(List.of(
                        Aggregates.lookup(INDEX_BOOKS, ID, ID, "published"),
                        Aggregates.match(Filters.size("published", 0)),
                        Aggregates.project(Projections.include(ID))))
                .forEach(doc -> indexed.add(doc.get(ID, Number.class).intValue()));
        Collections.sort(indexed);

        List<Integer> interrupted = new ArrayList<>();
        indexBooks.find(Filters.gt(VERSION, currentVersion == null ? 0 : currentVersion))
                .projection(Projections.include(ID))
                .sort(Sorts.ascending(ID))
                .forEach(doc -> interrupted.add(doc.get(ID, Number.class).intValue()));
        return new Unpublished(indexed, interrupted);
    }

    /**
     * One upsert per book with $setOnInsert: inserts {_id, version} if the book is not there and
     * leaves it as it is if it is. All in ONE unordered bulkWrite: the ids are different, so the
     * order does not matter.
     */
    @Override
    public void addBooks(Collection<Integer> bookIds, int version) {
        Objects.requireNonNull(bookIds, "bookIds");
        TreeSet<Integer> ids = new TreeSet<>(bookIds);
        if (ids.isEmpty()) {
            return;                                     // bulkWrite rejects an empty list
        }
        ensureVersionIndex();
        List<WriteModel<Document>> writes = new ArrayList<>(ids.size());
        for (int id : ids) {
            writes.add(new UpdateOneModel<>(Filters.eq(ID, id), Updates.setOnInsert(VERSION, version), UPSERT));
        }
        indexBooks.bulkWrite(writes, new BulkWriteOptions().ordered(false));
    }

    /** The interrupted books (version above CURRENT) and booksOfVersion go through it. */
    private void ensureVersionIndex() {
        if (!versionIndexReady) {
            indexBooks.createIndex(Indexes.ascending(VERSION));
            versionIndexReady = true;                   // only if MongoDB created it
        }
    }

    @Override
    public long countVisible(int version) {
        return indexBooks.countDocuments(Filters.lte(VERSION, version));
    }

    /**
     * The update matches CURRENT only while its version is still the expected one, and one
     * update of one document is atomic in MongoDB: of two publishers that read the same V, only
     * one moves CURRENT to V+1. The first version is an insert, which the unique _id lets only
     * one publisher do.
     */
    @Override
    public boolean switchCurrent(Integer expectedVersion, Publication next) {
        Objects.requireNonNull(next, "next");
        if (expectedVersion == null) {
            try {
                indexMeta.insertOne(new Document(ID, CURRENT)
                        .append(VERSION, next.version())
                        .append(COLLECTION, next.collection())
                        .append(BOOKS, next.books())
                        .append(PUBLISHED_AT, next.publishedAt()));
                return true;
            } catch (MongoWriteException e) {
                if (e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
                    return false;                       // another publisher created it first
                }
                throw e;
            }
        }
        return indexMeta.updateOne(
                Filters.and(Filters.eq(ID, CURRENT), Filters.eq(VERSION, expectedVersion)),
                Updates.combine(
                        Updates.set(VERSION, next.version()),
                        Updates.set(COLLECTION, next.collection()),
                        Updates.set(BOOKS, next.books()),
                        Updates.set(PUBLISHED_AT, next.publishedAt())))
                .getMatchedCount() == 1;
    }

    /** replaceOne with upsert: writing the same version twice leaves one entry, the last one (D3). */
    @Override
    public void recordHistory(PublishedVersion entry) {
        Objects.requireNonNull(entry, "entry");
        history.replaceOne(Filters.eq(ID, entry.version()),
                new Document(ID, entry.version())
                        .append(BOOKS, entry.books())
                        .append("books_published", entry.books().size())
                        .append("total_books", entry.totalBooks())
                        .append(PUBLISHED_AT, entry.publishedAt())
                        .append(COLLECTION, entry.collection()),
                new ReplaceOptions().upsert(true));
    }

    @Override
    public boolean hasHistory(int version) {
        return history.countDocuments(Filters.eq(ID, version)) > 0;
    }

    @Override
    public List<Integer> booksOfVersion(int version) {
        List<Integer> ids = new ArrayList<>();
        indexBooks.find(Filters.eq(VERSION, version))
                .projection(Projections.include(ID))
                .sort(Sorts.ascending(ID))
                .forEach(doc -> ids.add(doc.get(ID, Number.class).intValue()));
        return ids;
    }

    /**
     * One query by _id (the primary index) per IN_BATCH ids. Without repeated ids, so that an id
     * repeated in two batches is not returned twice; the order does not matter.
     */
    @Override
    public List<Integer> visibleAmong(Collection<Integer> bookIds, int version) {
        List<Integer> all = new ArrayList<>(new LinkedHashSet<>(Objects.requireNonNull(bookIds, "bookIds")));
        List<Integer> visible = new ArrayList<>();
        for (int from = 0; from < all.size(); from += IN_BATCH) {
            List<Integer> batch = all.subList(from, Math.min(from + IN_BATCH, all.size()));
            indexBooks.find(Filters.and(Filters.in(ID, batch), Filters.lte(VERSION, version)))
                    .projection(Projections.include(ID))
                    .forEach(doc -> visible.add(doc.get(ID, Number.class).intValue()));
        }
        return visible;
    }
}
