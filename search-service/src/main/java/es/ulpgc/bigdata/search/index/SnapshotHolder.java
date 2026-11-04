package es.ulpgc.bigdata.search.index;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The snapshot this process is serving. Every request calls current() ONCE when it starts
 * and works only with what it got, even if a new version is swapped in meanwhile.
 *
 * AtomicReference: replacing the snapshot is a single atomic write, and every thread sees
 * the new one as soon as it is written (no locks, no half-built object visible).
 * SnapshotRefresher calls reload() every few seconds; a request never waits for it.
 */
public final class SnapshotHolder {

    private final IndexSnapshotLoader loader;
    private final AtomicReference<IndexSnapshot> current = new AtomicReference<>();

    public SnapshotHolder(IndexSnapshotLoader loader) {
        this.loader = Objects.requireNonNull(loader, "loader");
    }

    /** The snapshot being served; empty until a version has been published and loaded. */
    public Optional<IndexSnapshot> current() {
        return Optional.ofNullable(current.get());
    }

    /**
     * Reads CURRENT from MongoDB and, only if it no longer matches the served snapshot, builds
     * the new one and swaps it in. It is built completely BEFORE the requests can see it, and
     * requests already running keep the snapshot they took.
     *
     * synchronized: two simultaneous reloads (the periodic check and, later, the IndexPublished
     * event) must not build the same version twice or leave an older one in place. It blocks
     * only other reloads, because requests read the AtomicReference without any lock.
     *
     * @return the version now served, or empty if nothing has been published yet
     * @throws RuntimeException if the read fails; the served snapshot stays as it was
     */
    public synchronized Optional<IndexSnapshot> reload() {
        IndexSnapshot served = current.get();
        loader.readCurrent()
                .filter(published -> !published.isServedBy(served))
                .map(published -> loader.load(published, served))
                .ifPresent(current::set);
        return current();
    }

    /** Fields of GET /status: null while there is no published version. */
    public Map<String, Object> status() {
        IndexSnapshot snapshot = current.get();
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("index_version", snapshot == null ? null : snapshot.version());
        fields.put("books_indexed", snapshot == null ? null : snapshot.bookCount());
        return fields;
    }
}
