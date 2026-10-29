package es.ulpgc.bigdata.search.index;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import es.ulpgc.bigdata.datamart.metadata.MetadataRepository;
import es.ulpgc.bigdata.model.BookMetadata;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Builds the snapshot of the published index version from MongoDB (contract §6.5):
 *
 * <pre>
 *   index_meta   {_id: "CURRENT", version: 42, collection: "inverted_index", ...}   which version is visible
 *   index_books  {_id: 1342, version: 41}                                         one document per published book
 *   books        {_id: 1342, title: ..., author: ..., language: ..., year: ...}   metadata (MetadataRepository)
 * </pre>
 *
 * Order of the reads: CURRENT first, then the books with version &lt;= CURRENT.version.
 * Indexing publishes V+1 by inserting its books in index_books and only then switching
 * CURRENT (one document, so the switch is atomic). Whatever moment this runs, it sees either
 * all of version V or all of V+1, never part of V+1.
 *
 * Search only reads: it never writes to any of these collections.
 */
public final class IndexSnapshotLoader {

    public static final String INDEX_META = "index_meta";
    public static final String INDEX_BOOKS = "index_books";
    public static final String CURRENT = "CURRENT";

    private final MongoDatabase database;
    private final MetadataRepository metadata;

    public IndexSnapshotLoader(MongoDatabase database, MetadataRepository metadata) {
        this.database = Objects.requireNonNull(database, "database");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
    }

    /**
     * The snapshot of the version CURRENT points to.
     *
     * @return empty if nothing has been published yet (no CURRENT document): the API answers 503
     * @throws IllegalStateException if CURRENT does not follow the contract
     * @throws com.mongodb.MongoException if MongoDB does not answer
     */
    public Optional<IndexSnapshot> loadCurrent() {
        Document current = database.getCollection(INDEX_META).find(Filters.eq("_id", CURRENT)).first();
        if (current == null) {
            return Optional.empty();
        }
        Number version = current.get("version", Number.class);
        String collection = current.getString("collection");
        if (version == null || collection == null || collection.isBlank()) {
            throw new IllegalStateException("index_meta CURRENT must have version and collection: " + current.toJson());
        }
        List<Integer> ids = publishedIds(version.intValue());
        return Optional.of(new IndexSnapshot(version.intValue(), collection, metadataOf(ids)));
    }

    /** Ids of index_books with version &lt;= the given one (only the _id is transferred). */
    private List<Integer> publishedIds(int version) {
        List<Integer> ids = new ArrayList<>();
        database.getCollection(INDEX_BOOKS)
                .find(Filters.lte("version", version))
                .projection(Projections.include("_id"))
                .forEach(doc -> ids.add(doc.get("_id", Number.class).intValue()));
        return ids;
    }

    /**
     * Metadata of the published ids. books may be AHEAD of the index (Indexing saves the
     * metadata before publishing), never behind; if a published id has no metadata anyway,
     * the book stays visible with null fields instead of disappearing from the results.
     */
    private List<BookMetadata> metadataOf(List<Integer> ids) {
        Map<Integer, BookMetadata> found = new HashMap<>();
        for (BookMetadata book : metadata.findByIds(ids)) {
            found.put(book.bookId(), book);
        }
        List<BookMetadata> result = new ArrayList<>(ids.size());
        for (int id : ids) {
            result.add(found.getOrDefault(id, new BookMetadata(id, null, null, null, null, null, null, null, null)));
        }
        return result;
    }
}
