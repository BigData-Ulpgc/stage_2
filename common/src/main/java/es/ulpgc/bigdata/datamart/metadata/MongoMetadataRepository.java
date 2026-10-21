package es.ulpgc.bigdata.datamart.metadata;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.WriteModel;
import es.ulpgc.bigdata.model.BookMetadata;
import es.ulpgc.bigdata.model.YearSource;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Supplier;

/**
 * MetadataRepository on MongoDB (contract §6.3): collection "books" of the search_engine
 * database, one document per book:
 *
 * <pre>
 *   {_id: 1342, title: "Pride and Prejudice", author: "Jane Austen", language: "en",
 *    year: 1998, year_source: "release_date", release_date: "June 1, 1998",
 *    header_path: "...", body_path: "..."}
 * </pre>
 *
 *  - The book id IS the _id: Mongo already keeps _id unique and indexed, so two documents
 *    of the same book are impossible and no other index is needed (Search filters in
 *    memory, it only reads by id).
 *  - All 9 fields are always present; a missing value is an explicit null, so every
 *    document has the same shape, like the rows of a table.
 *  - Field names in snake_case, as in the OpenAPI.
 *
 * It does not own the MongoClient: it receives a database of the client the service
 * shares (MongoSettings.openClient), and close() leaves that client open.
 * Thread-safe: it keeps no state of its own and the driver's collection is thread-safe.
 */
public class MongoMetadataRepository implements MetadataRepository {

    public static final String DEFAULT_COLLECTION = "books";

    /** Ids per $in query in findByIds: Search asks for every published book when it starts. */
    static final int IDS_PER_QUERY = 1000;

    static final String ID = "_id";
    static final String TITLE = "title";
    static final String AUTHOR = "author";
    static final String LANGUAGE = "language";
    static final String YEAR = "year";
    static final String YEAR_SOURCE = "year_source";
    static final String RELEASE_DATE = "release_date";
    static final String HEADER_PATH = "header_path";
    static final String BODY_PATH = "body_path";

    /** Replace the whole document, or insert it if that _id does not exist yet. */
    private static final ReplaceOptions UPSERT = new ReplaceOptions().upsert(true);

    private final MongoCollection<Document> collection;

    /** The contract's collection, "books". */
    public MongoMetadataRepository(MongoDatabase database) {
        this(database, DEFAULT_COLLECTION);
    }

    /** Configurable collection (the tests use a new one each time). */
    public MongoMetadataRepository(MongoDatabase database, String collectionName) {
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(collectionName, "collectionName");
        this.collection = database.getCollection(collectionName);
    }

    // ------------------------------------------------------------------
    // Writing
    // ------------------------------------------------------------------

    /** replaceOne + upsert: afterwards the document is EXACTLY this book, whatever was there. */
    @Override
    public void save(BookMetadata metadata) {
        Objects.requireNonNull(metadata, "metadata");
        run("No se pudo guardar el libro " + metadata.bookId(),
                () -> collection.replaceOne(byId(metadata.bookId()), toDocument(metadata), UPSERT));
    }

    /**
     * One ordered bulkWrite: a single round trip, and if the batch has the same id twice the
     * LAST one wins, as it would with save() one by one. (The index flush can be unordered
     * because adding ids to a set gives the same result in any order; replacing does not.)
     * Not atomic: if Mongo fails halfway, the first ones stay saved; repeating is safe.
     */
    @Override
    public void saveAll(List<BookMetadata> batch) {
        Objects.requireNonNull(batch, "batch");
        if (batch.isEmpty()) {
            return;                                     // bulkWrite rejects an empty list
        }
        List<WriteModel<Document>> writes = new ArrayList<>(batch.size());
        for (BookMetadata metadata : batch) {
            // Checked BEFORE sending anything: a null in the batch writes nothing.
            Objects.requireNonNull(metadata, "elemento null en el lote");
            writes.add(new ReplaceOneModel<>(byId(metadata.bookId()), toDocument(metadata), UPSERT));
        }
        run("No se pudo guardar el lote de " + batch.size() + " libros",
                () -> collection.bulkWrite(writes, new BulkWriteOptions().ordered(true)));
    }

    // ------------------------------------------------------------------
    // Reading
    // ------------------------------------------------------------------

    @Override
    public Optional<BookMetadata> findById(int bookId) {
        Document doc = run("No se pudo leer el libro " + bookId,
                () -> collection.find(byId(bookId)).first());
        return Optional.ofNullable(doc).map(MongoMetadataRepository::fromDocument);
    }

    /**
     * The ids are sorted and deduplicated first and asked for in blocks of IDS_PER_QUERY,
     * each block sorted by _id: the blocks come back in order, so the result is sorted.
     */
    @Override
    public List<BookMetadata> findByIds(Collection<Integer> bookIds) {
        Objects.requireNonNull(bookIds, "bookIds");
        List<Integer> ids = new ArrayList<>(new TreeSet<>(bookIds));
        List<BookMetadata> result = new ArrayList<>(ids.size());
        for (int from = 0; from < ids.size(); from += IDS_PER_QUERY) {
            List<Integer> block = ids.subList(from, Math.min(from + IDS_PER_QUERY, ids.size()));
            run("No se pudieron leer " + block.size() + " libros",
                    () -> collection.find(Filters.in(ID, block))
                            .sort(Sorts.ascending(ID))
                            .map(MongoMetadataRepository::fromDocument)
                            .into(result));
        }
        return List.copyOf(result);
    }

    @Override
    public long count() {
        return run("No se pudo contar los libros", collection::countDocuments);
    }

    /** Drops the collection; Mongo creates it again on the next write (there are no indexes to rebuild). */
    @Override
    public void clear() {
        run("No se pudo vaciar la colección de libros", () -> {
            collection.drop();
            return null;
        });
    }

    /** Nothing to close: the MongoClient belongs to whoever created it (the service). */
    @Override
    public void close() {
    }

    // ------------------------------------------------------------------
    // BookMetadata <-> Document
    // ------------------------------------------------------------------

    private static Bson byId(int bookId) {
        return Filters.eq(ID, bookId);
    }

    static Document toDocument(BookMetadata m) {
        return new Document(ID, m.bookId())
                .append(TITLE, m.title())
                .append(AUTHOR, m.author())
                .append(LANGUAGE, m.language())
                .append(YEAR, m.year())
                .append(YEAR_SOURCE, m.yearSource() == null ? null : m.yearSource().key())
                .append(RELEASE_DATE, m.releaseDate())
                .append(HEADER_PATH, m.headerPath() == null ? null : m.headerPath().toString())
                .append(BODY_PATH, m.bodyPath() == null ? null : m.bodyPath().toString());
    }

    static BookMetadata fromDocument(Document doc) {
        String headerPath = doc.getString(HEADER_PATH);
        String bodyPath = doc.getString(BODY_PATH);
        return new BookMetadata(
                integer(doc, ID),
                doc.getString(TITLE),
                doc.getString(AUTHOR),
                doc.getString(LANGUAGE),
                integer(doc, YEAR),
                YearSource.fromKey(doc.getString(YEAR_SOURCE)),
                doc.getString(RELEASE_DATE),
                bodyPath == null ? null : Path.of(bodyPath),
                headerPath == null ? null : Path.of(headerPath));
    }

    /**
     * Any number as an Integer. This class writes int32, but a document written by hand in
     * mongosh stores numbers as double (1342.0), and Python may write int64.
     */
    private static Integer integer(Document doc, String field) {
        Number value = doc.get(field, Number.class);
        return value == null ? null : value.intValue();
    }

    /** Runs a Mongo operation, wrapping its errors in MetadataRepositoryException. */
    private static <T> T run(String error, Supplier<T> operation) {
        try {
            return operation.get();
        } catch (MongoException e) {
            throw new MetadataRepositoryException(error, e);
        }
    }
}
