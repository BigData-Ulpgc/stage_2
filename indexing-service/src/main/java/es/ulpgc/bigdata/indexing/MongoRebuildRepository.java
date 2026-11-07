package es.ulpgc.bigdata.indexing;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * RebuildRepository on MongoDB. The progress is one document of index_meta, next to CURRENT:
 * <pre>
 *   {_id: "REBUILD", state: "running", started_at, finished_at, processed, total, error,
 *    collection: "inverted_index_r02", pending_drop: "inverted_index_r01", drop_after}
 * </pre>
 * Search reads only the CURRENT document of index_meta, so this one does not disturb it.
 */
public final class MongoRebuildRepository implements RebuildRepository {

    public static final String REBUILD = "REBUILD";

    private static final String ID = "_id";

    private final MongoDatabase database;
    private final MongoCollection<Document> indexMeta;

    public MongoRebuildRepository(MongoDatabase database) {
        this.database = Objects.requireNonNull(database, "database");
        this.indexMeta = database.getCollection(MongoPublicationRepository.INDEX_META);
    }

    @Override
    public Optional<Rebuild> last() {
        Document doc = indexMeta.find(Filters.eq(ID, REBUILD)).first();
        if (doc == null) {
            return Optional.empty();
        }
        return Optional.of(new Rebuild(
                Rebuild.State.valueOf(doc.getString("state")),
                instant(doc.getDate("started_at")),
                instant(doc.getDate("finished_at")),
                doc.get("processed", Number.class).intValue(),
                doc.get("total", Number.class).intValue(),
                doc.getString("error"),
                doc.getString("collection"),
                doc.getString("pending_drop"),
                instant(doc.getDate("drop_after"))));
    }

    @Override
    public void save(Rebuild rebuild) {
        Objects.requireNonNull(rebuild, "rebuild");
        indexMeta.replaceOne(Filters.eq(ID, REBUILD), new Document(ID, REBUILD)
                        .append("state", rebuild.state().name())
                        .append("started_at", date(rebuild.startedAt()))
                        .append("finished_at", date(rebuild.finishedAt()))
                        .append("processed", rebuild.processed())
                        .append("total", rebuild.total())
                        .append("error", rebuild.error())
                        .append("collection", rebuild.collection())
                        .append("pending_drop", rebuild.pendingDrop())
                        .append("drop_after", date(rebuild.dropAfter())),
                new ReplaceOptions().upsert(true));
    }

    @Override
    public List<String> collections() {
        return database.listCollectionNames().into(new ArrayList<>());
    }

    @Override
    public void drop(String collection) {
        database.getCollection(Objects.requireNonNull(collection, "collection")).drop();
    }

    private static Date date(Instant instant) {
        return instant == null ? null : Date.from(instant);
    }

    private static Instant instant(Date date) {
        return date == null ? null : date.toInstant();
    }
}
