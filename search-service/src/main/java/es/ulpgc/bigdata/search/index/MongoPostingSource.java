package es.ulpgc.bigdata.search.index;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.bson.Document;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Reads posting lists from the inverted index collection of common's MongoInvertedIndex
 * ({term: "whale", postings: [76, 84, ...]}, unique index on term).
 *
 * ONE query per request, whatever the number of terms: find({term: {$in: [terms]}}). That
 * is one round trip to MongoDB instead of one per term, and the unique index on term makes
 * each lookup an index seek. Only term and postings are transferred.
 *
 * Thread-safe: it keeps no state, and the driver's database object is thread-safe.
 */
public final class MongoPostingSource implements PostingSource {

    private final MongoDatabase database;

    public MongoPostingSource(MongoDatabase database) {
        this.database = Objects.requireNonNull(database, "database");
    }

    @Override
    public Map<String, int[]> postings(String collection, Set<String> terms) {
        Map<String, int[]> result = new HashMap<>();
        if (terms.isEmpty()) {
            return result;
        }
        database.getCollection(collection)
                .find(Filters.in("term", terms))
                .projection(Projections.fields(Projections.include("term", "postings"), Projections.excludeId()))
                .forEach(doc -> result.put(doc.getString("term"), sortedIds(doc)));
        return result;
    }

    /** MongoDB keeps the array in arrival order ($addToSet), so it is sorted here. */
    private static int[] sortedIds(Document doc) {
        List<?> stored = doc.getList("postings", Object.class);
        if (stored == null) {
            return new int[0];
        }
        int[] ids = new int[stored.size()];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = ((Number) stored.get(i)).intValue();   // int32 or int64, as common's index
        }
        Arrays.sort(ids);
        return ids;
    }
}
