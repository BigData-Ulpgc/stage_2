package es.ulpgc.bigdata.indexing;

import es.ulpgc.bigdata.datamart.index.InvertedIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * RebuildRepository in memory, for the tests. Its collections are the keys of the map where
 * the test keeps one index per collection: dropping one removes it from there.
 */
public class InMemoryRebuildRepository implements RebuildRepository {

    private final Map<String, ? extends InvertedIndex> collections;
    private final AtomicReference<Rebuild> last = new AtomicReference<>();
    /** Every save, in order: the progress as GET /index/status saw it. */
    final List<Rebuild> saved = new CopyOnWriteArrayList<>();
    final List<String> dropped = new CopyOnWriteArrayList<>();

    public InMemoryRebuildRepository(Map<String, ? extends InvertedIndex> collections) {
        this.collections = collections;
    }

    @Override
    public Optional<Rebuild> last() {
        return Optional.ofNullable(last.get());
    }

    @Override
    public void save(Rebuild rebuild) {
        last.set(rebuild);
        saved.add(rebuild);
    }

    @Override
    public List<String> collections() {
        return new ArrayList<>(collections.keySet());
    }

    @Override
    public void drop(String collection) {
        if (collections.remove(collection) != null) {
            dropped.add(collection);
        }
    }
}
