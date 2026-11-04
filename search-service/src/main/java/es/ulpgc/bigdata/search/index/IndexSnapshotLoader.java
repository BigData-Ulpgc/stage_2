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

    /** What the CURRENT document says right now: the visible version and its collection. */
    public record Published(int version, String collection) {
        boolean isServedBy(IndexSnapshot snapshot) {
            return snapshot != null && snapshot.version() == version && snapshot.collection().equals(collection);
        }
    }

    /**
     * Reads only the CURRENT document: one small query, cheap enough to repeat every few seconds.
     *
     * @return empty if nothing has been published yet (no CURRENT document): the API answers 503
     * @throws IllegalStateException if CURRENT does not follow the contract
     * @throws com.mongodb.MongoException if MongoDB does not answer
     */
    public Optional<Published> readCurrent() {
        Document current = database.getCollection(INDEX_META).find(Filters.eq("_id", CURRENT)).first();
        if (current == null) {
            return Optional.empty();
        }
        Number version = current.get("version", Number.class);
        String collection = current.getString("collection");
        if (version == null || collection == null || collection.isBlank()) {
            throw new IllegalStateException("index_meta CURRENT must have version and collection: " + current.toJson());
        }
        return Optional.of(new Published(version.intValue(), collection));
    }

    /** The whole snapshot of the version CURRENT points to; empty if nothing has been published yet. */
    public Optional<IndexSnapshot> loadCurrent() {
        return readCurrent().map(published -> load(published, null));
    }

    /**
     * Builds the snapshot of a published version.
     *
     * A book's metadata does not change once it is published, so when the index simply moved
     * forward the metadata already in memory is reused and only the new books are read (plan
     * DP-06). After anything else (a rebuild into another collection, a lower version) nothing
     * is assumed and all of it is read again.
     *
     * @param previous the snapshot being served, or null if there is none
     */
    public IndexSnapshot load(Published published, IndexSnapshot previous) {
        boolean movedForward = previous != null
                && published.version() > previous.version()
                && published.collection().equals(previous.collection());
        List<Integer> ids = publishedIds(published.version());
        return new IndexSnapshot(published.version(), published.collection(), metadataOf(ids, movedForward ? previous : null));
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
     * Books already in {@code reuse} (may be null) are not read again.
     */
    private List<BookMetadata> metadataOf(List<Integer> ids, IndexSnapshot reuse) {
        Map<Integer, BookMetadata> found = new HashMap<>();
        List<Integer> missing = new ArrayList<>();
        for (int id : ids) {
            Optional<BookMetadata> known = reuse == null ? Optional.empty() : reuse.book(id);
            if (known.isPresent()) {
                found.put(id, known.get());
            } else {
                missing.add(id);
            }
        }
        for (BookMetadata book : metadata.findByIds(missing)) {
            found.put(book.bookId(), book);
        }
        List<BookMetadata> result = new ArrayList<>(ids.size());
        for (int id : ids) {
            result.add(found.getOrDefault(id, new BookMetadata(id, null, null, null, null, null, null, null, null)));
        }
        return result;
    }
}
