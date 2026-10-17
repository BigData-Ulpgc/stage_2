package es.ulpgc.bigdata.datamart.index;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * Inverted index in MongoDB (shared/SPEC.md, section 6):
 *
 *   database:       search_engine
 *   collection:     inverted_index
 *   documents:      {"term": "boat", "postings": [10, 20]}
 *   unique index on "term": a single document per term.
 *
 * addDocument accumulates in memory; flush sends ALL the pending terms in a
 * single bulkWrite operation, with one upsert + $addToSet per term (no duplicates).
 * Mongo does not guarantee the order of the array, so postings sorts before returning.
 *
 * Stage 2 uses this index instead of the monolithic JSON because the goal is hundreds of
 * thousands of books: the JSON must be held whole in memory and rewritten whole on every
 * flush, while here a flush only sends the terms that changed and a query reads only the
 * documents of its terms. Known limits, to be measured (DR-13):
 *  - a MongoDB document is at most 16 MB, so one posting list holds about a million ids;
 *  - $addToSet checks the existing array, so updating very common terms gets slower as the
 *    corpus grows.
 * Not thread-safe (the pending map): Indexing has a single writer (DR-09).
 */
public class MongoInvertedIndex implements InvertedIndex {

    public static final String DEFAULT_DATABASE = "search_engine";
    public static final String DEFAULT_COLLECTION = "inverted_index";

    private static final String TERM = "term";
    private static final String POSTINGS = "postings";

    private final MongoClient client;
    private final MongoDatabase database;
    private final String collectionName;
    private final MongoCollection<Document> collection;

    /** Ids added since the last flush, per term. */
    private final Map<String, SortedSet<Integer>> pending = new HashMap<>();

    /** false after clear: the unique index is recreated before the next write. */
    private boolean uniqueIndexReady = false;

    /** SPEC database and collection. */
    public MongoInvertedIndex(String connectionString) {
        this(connectionString, DEFAULT_DATABASE, DEFAULT_COLLECTION);
    }

    /** Configurable database and collection (the tests use their own database). */
    public MongoInvertedIndex(String connectionString, String databaseName, String collectionName) {
        Objects.requireNonNull(connectionString, "connectionString");
        this.collectionName = Objects.requireNonNull(collectionName, "collectionName");
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                // If Mongo is not running, fail in 3 s instead of waiting 30 s.
                .applyToClusterSettings(b -> b.serverSelectionTimeout(3, TimeUnit.SECONDS))
                .build();
        this.client = MongoClients.create(settings);
        this.database = client.getDatabase(Objects.requireNonNull(databaseName, "databaseName"));
        this.collection = database.getCollection(collectionName);
        try {
            ensureUniqueIndex();                       // also checks that Mongo answers
        } catch (RuntimeException e) {
            client.close();                            // do not leave the connection open if the constructor fails
            throw e;
        }
    }

    @Override
    public String name() {
        return "mongo";
    }

    /** One document per term: two upserts of the same term never create two documents. */
    private void ensureUniqueIndex() {
        collection.createIndex(Indexes.ascending(TERM), new IndexOptions().unique(true));
        uniqueIndexReady = true;
    }

    // ------------------------------------------------------------------
    // addDocument and postings
    // ------------------------------------------------------------------

    /** Only in memory: does not talk to Mongo until flush. */
    @Override
    public void addDocument(int bookId, Set<String> terms) {
        if (bookId < 0) {
            throw new IllegalArgumentException("book_id negativo: " + bookId);
        }
        Objects.requireNonNull(terms, "terms");
        for (String term : terms) {
            pending.computeIfAbsent(term, t -> new TreeSet<>()).add(bookId);
        }
    }

    /** A single Mongo document + what is pending, sorted and without repetitions. */
    @Override
    public List<Integer> postings(String term) {
        if (term == null) {
            return List.of();
        }
        SortedSet<Integer> ids = new TreeSet<>();      // Mongo stores the array in arrival order
        Document doc = collection.find(Filters.eq(TERM, term)).first();
        if (doc != null) {
            List<Object> stored = doc.getList(POSTINGS, Object.class);
            if (stored != null) {
                for (Object id : stored) {
                    ids.add(((Number) id).intValue()); // int32 or int64 (Python/C may store int64)
                }
            }
        }
        SortedSet<Integer> notYetSaved = pending.get(term);
        if (notYetSaved != null) {
            ids.addAll(notYetSaved);
        }
        return List.copyOf(ids);
    }

    // ------------------------------------------------------------------
    // Persistence
    // ------------------------------------------------------------------

    /**
     * One upsert per term with $addToSet + $each: if the document does not exist it is created;
     * if it exists, only the ids that were not there are added. They all go in ONE bulkWrite,
     * so it takes a few round trips to Mongo instead of one per term.
     */
    @Override
    public void flush() {
        if (pending.isEmpty()) {
            return;
        }
        if (!uniqueIndexReady) {
            ensureUniqueIndex();
        }
        List<WriteModel<Document>> updates = new ArrayList<>(pending.size());
        UpdateOptions upsert = new UpdateOptions().upsert(true);
        for (Map.Entry<String, SortedSet<Integer>> entry : pending.entrySet()) {
            updates.add(new UpdateOneModel<>(
                    Filters.eq(TERM, entry.getKey()),
                    Updates.addEachToSet(POSTINGS, new ArrayList<>(entry.getValue())),
                    upsert));
        }
        collection.bulkWrite(updates, new BulkWriteOptions().ordered(false));
        pending.clear();                                // only if Mongo accepted everything; if it fails, it can be retried
    }

    /** Deletes the whole collection; the unique index is recreated before the next write. */
    @Override
    public void clear() {
        pending.clear();
        collection.drop();
        uniqueIndexReady = false;
    }

    /**
     * Bytes Mongo reports on disk for this collection: data (storageSize)
     * plus indexes (totalIndexSize). 0 if the collection does not exist.
     *
     * An fsync is requested first: WiredTiger only moves the data to the collection file
     * at each checkpoint (about 60 s); right after writing, storageSize would still be
     * that of the almost empty collection.
     */
    @Override
    public long diskUsageBytes() {
        List<String> names = database.listCollectionNames().into(new ArrayList<>());
        if (!names.contains(collectionName)) {
            return 0;
        }
        try {
            client.getDatabase("admin").runCommand(new Document("fsync", 1));
        } catch (RuntimeException e) {
            // No permission for fsync (e.g. a managed Mongo): the number may lag behind.
        }
        Document stats = collection.aggregate(List.of(
                new Document("$collStats", new Document("storageStats", new Document())))).first();
        if (stats == null) {
            return 0;
        }
        Document storage = stats.get("storageStats", Document.class);
        return number(storage, "storageSize") + number(storage, "totalIndexSize");
    }

    private static long number(Document doc, String field) {
        Object value = doc == null ? null : doc.get(field);
        return value instanceof Number n ? n.longValue() : 0;
    }

    /** Closes the connection. It does not flush: whoever uses the index decides that. */
    @Override
    public void close() {
        client.close();
    }
}