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
 * DP-03 loads it when the service starts; DP-06 adds the reload on each publication.
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
     * Reads CURRENT from MongoDB and, if there is a version, swaps the new snapshot in.
     * The new snapshot is built completely BEFORE it is published to the requests.
     *
     * @return the version now served, or empty if nothing has been published yet
     */
    public Optional<IndexSnapshot> reload() {
        Optional<IndexSnapshot> loaded = loader.loadCurrent();
        loaded.ifPresent(current::set);
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
