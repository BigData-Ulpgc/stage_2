package es.ulpgc.bigdata.indexing;

import com.mongodb.MongoException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Fake PublicationRepository for the tests, with the same rules as MongoPublicationRepository:
 * index_books keeps the FIRST version of each book ($setOnInsert), CURRENT only switches if it
 * is still at the expected version (CAS) and the history keeps one entry per version.
 *
 * The indexed books are read from an InMemoryIndexLog, like the $lookup reads index_log. The
 * test can also:
 *  - setCurrent: write CURRENT as another program would;
 *  - beforeSwitch: run something just before the CAS (another replica that publishes first);
 *  - beforeCurrent: run something every time CURRENT is read (count the reads, make a read
 *    slow or fail);
 *  - failSwitch / failHistory: make that step fail like a MongoDB that stops answering (a
 *    publication that dies halfway).
 */
class InMemoryPublicationRepository implements PublicationRepository {

    private final InMemoryIndexLog indexLog;
    private final AtomicReference<Publication> current = new AtomicReference<>();
    private final Map<Integer, Integer> indexBooks = new ConcurrentHashMap<>();
    private final Map<Integer, PublishedVersion> history = new ConcurrentHashMap<>();

    private volatile Runnable beforeSwitch = () -> { };
    private volatile Runnable beforeCurrent = () -> { };
    private volatile boolean failSwitch = false;
    private volatile boolean failHistory = false;
    private volatile int switchAttempts = 0;

    InMemoryPublicationRepository() {
        this(new InMemoryIndexLog());
    }

    InMemoryPublicationRepository(InMemoryIndexLog indexLog) {
        this.indexLog = Objects.requireNonNull(indexLog, "indexLog");
    }

    /** null = nothing published yet. */
    void setCurrent(Publication current) {
        this.current.set(current);
    }

    void beforeSwitch(Runnable action) {
        this.beforeSwitch = Objects.requireNonNull(action, "action");
    }

    void beforeCurrent(Runnable action) {
        this.beforeCurrent = Objects.requireNonNull(action, "action");
    }

    void failSwitch(boolean fail) {
        this.failSwitch = fail;
    }

    void failHistory(boolean fail) {
        this.failHistory = fail;
    }

    int switchAttempts() {
        return switchAttempts;
    }

    /** The version index_books has for the book; null if it is not there. */
    Integer versionOf(int bookId) {
        return indexBooks.get(bookId);
    }

    /** The history entry of the version; null if it is not there. */
    PublishedVersion historyOf(int version) {
        return history.get(version);
    }

    /** Removes the history entry, as a publication that died before writing it leaves it. */
    void forgetHistory(int version) {
        history.remove(version);
    }

    @Override
    public Optional<Publication> current() {
        beforeCurrent.run();
        return Optional.ofNullable(current.get());
    }

    @Override
    public Unpublished unpublished(Integer currentVersion) {
        int visible = currentVersion == null ? 0 : currentVersion;
        List<Integer> indexed = new ArrayList<>();
        for (int id : indexLog.ids()) {
            if (!indexBooks.containsKey(id)) {
                indexed.add(id);
            }
        }
        indexed.sort(null);
        List<Integer> interrupted = new ArrayList<>();
        indexBooks.forEach((id, version) -> {
            if (version > visible) {
                interrupted.add(id);
            }
        });
        interrupted.sort(null);
        return new Unpublished(indexed, interrupted);
    }

    @Override
    public void addBooks(Collection<Integer> bookIds, int version) {
        Objects.requireNonNull(bookIds, "bookIds");
        for (int id : bookIds) {
            indexBooks.putIfAbsent(id, version);                       // $setOnInsert
        }
    }

    @Override
    public long countVisible(int version) {
        return indexBooks.values().stream().filter(v -> v <= version).count();
    }

    @Override
    public boolean switchCurrent(Integer expectedVersion, Publication next) {
        Objects.requireNonNull(next, "next");
        switchAttempts++;
        beforeSwitch.run();
        if (failSwitch) {
            throw new MongoException("MongoDB no responde (simulado)");
        }
        // One compareAndSet on the value read, like the filter {version: expected} of the update.
        Publication seen = current.get();
        Integer seenVersion = seen == null ? null : seen.version();
        if (!Objects.equals(seenVersion, expectedVersion)) {
            return false;
        }
        return current.compareAndSet(seen, next);
    }

    @Override
    public void recordHistory(PublishedVersion entry) {
        Objects.requireNonNull(entry, "entry");
        if (failHistory) {
            throw new MongoException("MongoDB no responde (simulado)");
        }
        history.put(entry.version(), entry);
    }

    @Override
    public boolean hasHistory(int version) {
        return history.containsKey(version);
    }

    @Override
    public List<Integer> booksOfVersion(int version) {
        List<Integer> ids = new ArrayList<>();
        indexBooks.forEach((id, v) -> {
            if (v == version) {
                ids.add(id);
            }
        });
        ids.sort(null);
        return ids;
    }

    @Override
    public List<Integer> visibleAmong(Collection<Integer> bookIds, int version) {
        List<Integer> visible = new ArrayList<>();
        for (int id : new TreeSet<>(bookIds)) {
            Integer v = indexBooks.get(id);
            if (v != null && v <= version) {
                visible.add(id);
            }
        }
        return visible;
    }
}
