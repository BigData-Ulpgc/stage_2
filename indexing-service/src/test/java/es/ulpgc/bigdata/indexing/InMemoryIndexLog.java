package es.ulpgc.bigdata.indexing;

import com.mongodb.MongoException;
import es.ulpgc.bigdata.datamart.indexlog.IndexLog;
import es.ulpgc.bigdata.model.BookIds;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fake IndexLog for the tests: a thread-safe map with the same rules as MongoIndexLog (valid
 * ids or nothing is written, indexed_at only moves forward, millisecond precision), so
 * IndexingService can be tested without MongoDB. failing(true) makes EVERY operation fail with
 * the MongoException of a MongoDB that does not answer.
 */
class InMemoryIndexLog implements IndexLog {

    private final Map<Integer, Instant> log = new ConcurrentHashMap<>();
    private volatile boolean failing = false;

    void failing(boolean failing) {
        this.failing = failing;
    }

    boolean contains(int bookId) {
        return log.containsKey(bookId);
    }

    /** The ids of the log, what the $lookup of MongoPublicationRepository reads from index_log. */
    Set<Integer> ids() {
        failIfDown();
        return Set.copyOf(log.keySet());
    }

    private void failIfDown() {
        if (failing) {
            throw new MongoException("MongoDB no responde (simulado)");
        }
    }

    @Override
    public void markIndexed(Collection<Integer> bookIds, Instant indexedAt) {
        Objects.requireNonNull(bookIds, "bookIds");
        Objects.requireNonNull(indexedAt, "indexedAt");
        for (Integer id : bookIds) {
            if (id == null || !BookIds.isValid(id)) {
                throw new IllegalArgumentException("book_id fuera de rango (1.." + BookIds.MAX + "): " + id);
            }
        }
        if (bookIds.isEmpty()) {
            return;
        }
        failIfDown();
        Instant at = indexedAt.truncatedTo(ChronoUnit.MILLIS);
        for (int id : bookIds) {
            log.merge(id, at, (old, now) -> now.isAfter(old) ? now : old);      // $max
        }
    }

    @Override
    public long count() {
        failIfDown();
        return log.size();
    }

    @Override
    public Optional<Instant> lastIndexedAt() {
        failIfDown();
        return log.values().stream().max(Instant::compareTo);
    }
}
