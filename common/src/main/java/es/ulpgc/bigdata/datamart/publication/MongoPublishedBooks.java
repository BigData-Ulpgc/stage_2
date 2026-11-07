package es.ulpgc.bigdata.datamart.publication;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.bson.Document;

import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Reads which books are published (plan §6.5): the books of index_books whose version is not
 * above the version of index_meta CURRENT.
 *
 * <pre>
 *   index_meta   {_id: "CURRENT", version: 42, ...}
 *   index_books  {_id: 1342, version: 41}
 * </pre>
 *
 * The same rule as Search's IndexSnapshotLoader: CURRENT first, then index_books. A book of
 * index_books above CURRENT belongs to a publication that did not switch CURRENT yet (or was
 * interrupted before), so it is not visible and does not count. The version of a book never
 * changes ($setOnInsert), so the answer is never part of a publication.
 *
 * For the recovery of the Control (CF-10, coordination contract §7): a published book is
 * PUBLISHED again. Read-only. Like the other repositories it receives the database of the client
 * the service shares and does not talk to MongoDB until it is used; its errors are the driver's
 * MongoException. The names are Indexing's (MongoPublicationRepository), which writes them.
 */
public final class MongoPublishedBooks {

    public static final String INDEX_META = "index_meta";
    public static final String INDEX_BOOKS = "index_books";
    public static final String CURRENT = "CURRENT";

    private static final String ID = "_id";
    private static final String VERSION = "version";

    private final MongoDatabase database;

    public MongoPublishedBooks(MongoDatabase database) {
        this.database = Objects.requireNonNull(database, "database");
    }

    /**
     * @return the ids of the books visible to Search; empty if nothing was ever published
     * @throws IllegalStateException if CURRENT exists without a version (a bug of its writer)
     */
    public Set<Integer> publishedIds() {
        Document current = database.getCollection(INDEX_META).find(Filters.eq(ID, CURRENT)).first();
        if (current == null) {
            return Set.of();
        }
        Number version = current.get(VERSION, Number.class);
        if (version == null) {
            throw new IllegalStateException("index_meta CURRENT must have a version: " + current.toJson());
        }
        Set<Integer> ids = new TreeSet<>();
        database.getCollection(INDEX_BOOKS)
                .find(Filters.lte(VERSION, version.intValue()))
                .projection(Projections.include(ID))
                .forEach(doc -> ids.add(doc.get(ID, Number.class).intValue()));
        return ids;
    }
}
