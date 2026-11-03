package es.ulpgc.bigdata.indexing;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.util.Objects;
import java.util.Optional;

/**
 * PublicationRepository on MongoDB (contract §6.5), on the database of the client the service
 * shares, like the other repositories: it does not own the client and does not talk to MongoDB
 * until it is used.
 *
 *   index_meta   {_id: "CURRENT", version: 42, collection: "inverted_index", books: 187, published_at: "..."}
 *
 * The same document and field names Search reads (IndexSnapshotLoader), so both services see
 * the same published version. Numbers are read as Number: another program may store int64.
 */
public final class MongoPublicationRepository implements PublicationRepository {

    public static final String INDEX_META = "index_meta";
    public static final String CURRENT = "CURRENT";

    private final MongoCollection<Document> indexMeta;

    public MongoPublicationRepository(MongoDatabase database) {
        this.indexMeta = Objects.requireNonNull(database, "database").getCollection(INDEX_META);
    }

    /**
     * One read of one document by _id, the cheapest query MongoDB has.
     *
     * @throws IllegalStateException if CURRENT exists but does not follow the contract (a bug
     *                               of whoever wrote it, not a temporary failure: the API answers 500)
     */
    @Override
    public Optional<Publication> current() {
        Document current = indexMeta.find(Filters.eq("_id", CURRENT)).first();
        if (current == null) {
            return Optional.empty();
        }
        Number version = current.get("version", Number.class);
        Number books = current.get("books", Number.class);
        if (version == null || books == null) {
            throw new IllegalStateException("index_meta CURRENT debe tener version y books: " + current.toJson());
        }
        return Optional.of(new Publication(version.intValue(), books.longValue()));
    }
}
