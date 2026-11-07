package es.ulpgc.bigdata.indexing;

import com.mongodb.MongoException;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.IndexWriterLock;
import es.ulpgc.bigdata.datalake.Datalake;
import es.ulpgc.bigdata.datamart.metadata.MetadataRepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * POST /index/rebuild (DR-10): indexes every complete book of the datalake into a NEW
 * collection, inverted_index_rNN, and then switches CURRENT to it with one publication, so
 * Search keeps reading the current index the whole time and never sees a half-built one.
 *
 * <ol>
 *   <li>The marker (start): the "rebuild" key of coordination-meta, putIfAbsent, so only one
 *       rebuild runs in the cluster, started by any replica. While it exists no writer claims
 *       from index-queue and update and publish answer 409 in every replica. The progress is
 *       written to index_meta REBUILD, which GET /index/status of every replica reads.</li>
 *   <li>The writers stop: the rebuild takes index-writer, which the writer of index-queue
 *       releases when it sees the marker. The books that arrive meanwhile wait in the queue.</li>
 *   <li>The collections nobody reads are dropped (what a rebuild that died left), then every
 *       book of the datalake is indexed into rNN in batches of INDEX_WRITE_BATCH. The metadata
 *       and index_log are the same as always (idempotent).</li>
 *   <li>Safety pass: a book written into the old collection during the rebuild (an update that
 *       passed the 409 check just before the marker, or a replica without Hazelcast, which
 *       does not see the marker) and published or about to be, is indexed into rNN too.</li>
 *   <li>The switch: {@link Publisher#publishInto}, version V+1 on rNN with every book.</li>
 *   <li>The old collection is dropped after INDEX_REBUILD_DROP_SECONDS: a Search replica may
 *       still be serving requests from the snapshot it loaded from it.</li>
 * </ol>
 * If anything fails before the switch, rNN is dropped and CURRENT is untouched: Search did not
 * notice anything, and calling again starts from scratch.
 *
 * The marker has a TTL (MARKER_TTL) that the rebuild renews while it waits for the lock and
 * before each batch. If the replica dies the marker expires and the writers go on. If a
 * renewal finds it gone (the replica stalled longer than the TTL), the rebuild stops: the
 * writers may be writing again.
 */
public final class Rebuilder implements AutoCloseable {

    /** How long the marker lives without a renewal. */
    static final Duration MARKER_TTL = Duration.ofSeconds(30);
    /** How long one attempt to take index-writer waits; the marker is renewed between attempts. */
    static final Duration LOCK_STEP = Duration.ofSeconds(2);
    /** The error of a rebuild stopped by close(). */
    static final String STOPPED = "the service stopped";

    private static final Logger LOG = LoggerFactory.getLogger(Rebuilder.class);

    private final Supplier<Optional<Coordination>> coordination;
    private final IndexingService service;
    private final Datalake datalake;
    private final PublicationRepository publications;
    private final Publisher publisher;
    private final RebuildRepository repository;
    private final String defaultCollection;
    private final Pattern rebuilt;
    private final int batchSize;
    private final Duration dropDelay;
    private final Clock clock;
    private final ScheduledExecutorService worker;

    /** The rebuild of this replica runs now (the marker only says that one runs somewhere). */
    private volatile boolean runningHere = false;
    /** close() was called: Hazelcast wraps the interrupt of shutdownNow in a HazelcastException. */
    private volatile boolean closing = false;
    /** The last rebuild started here; close() ends it if the worker never ran it. */
    private volatile Job job = null;

    /**
     * @param coordination      the connection to Hazelcast once it is there; empty until then
     * @param defaultCollection MONGO_COLLECTION: the rebuilt ones are MONGO_COLLECTION_rNN
     * @param batchSize         books indexed together (INDEX_WRITE_BATCH)
     * @param dropDelay         how long the old collection is kept after the switch
     *                          (INDEX_REBUILD_DROP_SECONDS)
     */
    public Rebuilder(Supplier<Optional<Coordination>> coordination, IndexingService service, Datalake datalake,
                     PublicationRepository publications, Publisher publisher, RebuildRepository repository,
                     String defaultCollection, int batchSize, Duration dropDelay, Clock clock) {
        this.coordination = Objects.requireNonNull(coordination, "coordination");
        this.service = Objects.requireNonNull(service, "service");
        this.datalake = Objects.requireNonNull(datalake, "datalake");
        this.publications = Objects.requireNonNull(publications, "publications");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.repository = Objects.requireNonNull(repository, "repository");
        this.defaultCollection = Objects.requireNonNull(defaultCollection, "defaultCollection");
        this.rebuilt = Pattern.compile(Pattern.quote(defaultCollection) + "_r(\\d+)");
        if (batchSize < 1) {
            throw new IllegalArgumentException("el lote debe tener al menos un libro: " + batchSize);
        }
        this.batchSize = batchSize;
        this.dropDelay = Objects.requireNonNull(dropDelay, "dropDelay");
        this.clock = Objects.requireNonNull(clock, "clock");
        // One thread: the rebuild, then the drops it schedules. Daemon: it never keeps the JVM alive.
        this.worker = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "indexing-service-rebuild");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * POST /index/rebuild: takes the marker, records the rebuild as running and starts it in
     * the background. Returns at once.
     *
     * @return the rebuild as it starts (202)
     * @throws RebuildInProgressException      if a rebuild runs in some replica (409)
     * @throws CoordinationUnavailableException if there is no Hazelcast yet (503)
     * @throws MongoException                  if MongoDB does not answer (503); nothing started
     */
    public Rebuild start() {
        Coordination cluster = coordination.get().orElseThrow(CoordinationUnavailableException::new);
        OptionalLong marker = cluster.tryStartRebuild(MARKER_TTL);
        if (marker.isEmpty()) {
            throw new RebuildInProgressException();
        }
        long token = marker.getAsLong();
        try {
            Instant now = clock.instant();
            Optional<Rebuild> last = repository.last();
            int startVersion = publications.current().map(Publication::version).orElse(0);
            List<Integer> ids = datalake.listBookIds();
            String target = nextCollection(last);
            // What the last one left to drop is still waiting: keep it, the cleanup respects it.
            Rebuild started = last.filter(previous -> previous.keepsPendingDrop(now))
                    .map(previous -> new Rebuild(Rebuild.State.RUNNING, now, null, 0, ids.size(), null, target,
                            previous.pendingDrop(), previous.dropAfter()))
                    .orElse(new Rebuild(Rebuild.State.RUNNING, now, null, 0, ids.size(), null, target, null, null));
            repository.save(started);
            runningHere = true;
            job = new Job(cluster, token, started, ids, startVersion);
            worker.execute(job);
            LOG.info("rebuild started: {} books into {}", ids.size(), target);
            return started;
        } catch (RuntimeException e) {
            runningHere = false;
            quietly("end the marker", () -> cluster.endRebuild(token));
            throw e;
        }
    }

    /**
     * The rebuild field of GET /index/status: the last one of the cluster. One that says
     * running while no replica holds the marker belonged to a replica that died: it is shown
     * as failed (the document is fixed by the next rebuild).
     *
     * @throws MongoException if MongoDB does not answer (503, as the rest of the status)
     */
    public Optional<Rebuild> status() {
        Optional<Rebuild> last = repository.last();
        if (last.isPresent() && last.get().state() == Rebuild.State.RUNNING && !runningHere && !markerKnownToExist()) {
            return Optional.of(last.get().failed(null, "the replica that ran it stopped"));
        }
        return last;
    }

    /**
     * Whether update and publish must answer 409. Without Hazelcast (not connected yet, or it
     * does not answer) it says false: indexing goes on without the cluster (D4), and the
     * safety pass of a rebuild catches what is written meanwhile.
     */
    public boolean isRebuilding() {
        if (runningHere) {
            return true;
        }
        Optional<Coordination> cluster = coordination.get();
        if (cluster.isEmpty()) {
            return false;
        }
        try {
            return cluster.get().isRebuilding();
        } catch (RuntimeException e) {
            LOG.warn("could not check the rebuild marker ({}); going on as if none ran", e.toString());
            return false;
        }
    }

    /** Stops a running rebuild (it fails and drops its collection) and the pending drops. */
    @Override
    public void close() {
        closing = true;
        worker.shutdownNow();
        try {
            worker.awaitTermination(5, TimeUnit.SECONDS);          // it drops its collection and says failed
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        Job last = job;
        if (last != null && last.claim()) {                       // shutdownNow took it from the queue
            Rebuild failed = last.started.failed(clock.instant(), STOPPED);
            quietly("record the failure of the rebuild", saver(failed));
            runningHere = false;
            quietly("end the marker", () -> last.cluster.endRebuild(last.token));
        }
    }

    /** One rebuild: run by the worker, or ended by close() if the worker never got to it. */
    private final class Job implements Runnable {
        private final Coordination cluster;
        private final long token;
        private final Rebuild started;
        private final List<Integer> ids;
        private final int startVersion;
        private final AtomicBoolean claimed = new AtomicBoolean();

        Job(Coordination cluster, long token, Rebuild started, List<Integer> ids, int startVersion) {
            this.cluster = cluster;
            this.token = token;
            this.started = started;
            this.ids = ids;
            this.startVersion = startVersion;
        }

        /** Only one of the worker and close() handles it. */
        boolean claim() {
            return claimed.compareAndSet(false, true);
        }

        @Override
        public void run() {
            if (claim()) {
                Rebuilder.this.run(cluster, token, started, ids, startVersion);
            }
        }
    }

    /** Waits for the running rebuild to end, for the tests. */
    boolean awaitIdle(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (runningHere) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            Thread.sleep(10);
        }
        return true;
    }

    // ------------------------------------------------------------------
    // The rebuild, in the worker thread
    // ------------------------------------------------------------------

    private void run(Coordination cluster, long token, Rebuild started, List<Integer> ids, int startVersion) {
        IndexWriterLock lock = cluster.indexWriterLock();
        boolean locked = false;
        boolean switched = false;
        Rebuild progress = started;
        String target = started.collection();
        try {
            locked = waitForTheWriters(cluster, token, lock);
            Optional<Publication> current = publications.current();
            String previous = current.map(this::collectionOf).orElse(defaultCollection);
            dropUnused(previous, target, started);

            Set<Integer> indexed = new HashSet<>();
            for (int from = 0; from < ids.size(); from += batchSize) {
                renew(cluster, token);
                List<Integer> batch = ids.subList(from, Math.min(from + batchSize, ids.size()));
                service.indexBatchInto(target, batch).indexed().forEach(book -> indexed.add(book.bookId()));
                progress = progress.withProgress(indexed.size(), ids.size(), target);
                repository.save(progress);
            }

            renew(cluster, token);
            List<Integer> missing = writtenElsewhere(startVersion, indexed);
            if (!missing.isEmpty()) {
                LOG.info("rebuild: {} books written into {} meanwhile, indexing them into {} too",
                        missing.size(), previous, target);
                service.indexBatchInto(target, missing).indexed().forEach(book -> indexed.add(book.bookId()));
                progress = progress.withProgress(indexed.size(), ids.size(), target);
            }

            renew(cluster, token);
            PublishResult published = publisher.publishInto(target);
            switched = true;
            Instant now = clock.instant();
            progress = progress.done(now, previous.equals(target) ? null : previous, now.plus(dropDelay));
            quietly("record the end of the rebuild", saver(progress));
            LOG.info("rebuild done: version {} on {} with {} books; {} is dropped in {} s",
                    published.version(), target, published.totalBooks(), previous, dropDelay.toSeconds());
            if (!previous.equals(target)) {
                worker.schedule(() -> dropIfUnused(previous), dropDelay.toMillis(), TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            progress = failed(progress, switched, target, STOPPED);
        } catch (RebuildAbortedException e) {
            progress = failed(progress, switched, target, e.reason());
        } catch (MongoException | MetadataRepositoryException e) {
            progress = failed(progress, switched, target,
                    "the index storage (MongoDB) does not answer: " + e.getMessage());
        } catch (RuntimeException e) {
            if (closing) {
                // Hazelcast wraps the interrupt of shutdownNow: HazelcastException(InterruptedException)
                progress = failed(progress, switched, target, STOPPED);
            } else {
                LOG.error("rebuild into {} failed", target, e);
                progress = failed(progress, switched, target, e.toString());
            }
        } finally {
            if (locked) {
                quietly("release index-writer", lock::unlock);
            }
            // After its document says done or failed: never "running" without a marker.
            runningHere = false;
            quietly("end the marker", () -> cluster.endRebuild(token));
        }
    }

    /**
     * Takes index-writer, renewing the marker between attempts: the writer of index-queue
     * releases it at the end of its batch, when it sees the marker.
     */
    private boolean waitForTheWriters(Coordination cluster, long token, IndexWriterLock lock)
            throws InterruptedException {
        while (true) {
            renew(cluster, token);
            if (lock.tryLock(LOCK_STEP)) {
                return true;
            }
            LOG.info("rebuild: waiting for the index writer to release index-writer");
        }
    }

    private void renew(Coordination cluster, long token) {
        if (!cluster.renewRebuild(token, MARKER_TTL)) {
            throw new RebuildAbortedException("the rebuild marker expired, the index writers may be writing again");
        }
    }

    /** Books published since the rebuild started, or about to be, that it did not index into rNN. */
    private List<Integer> writtenElsewhere(int startVersion, Set<Integer> indexed) {
        Optional<Publication> current = publications.current();
        Set<Integer> candidates = new TreeSet<>(publications
                .unpublished(current.map(Publication::version).orElse(null)).all());
        int currentVersion = current.map(Publication::version).orElse(0);
        for (int version = startVersion + 1; version <= currentVersion; version++) {
            candidates.addAll(publications.booksOfVersion(version));
        }
        candidates.removeAll(indexed);
        return new ArrayList<>(candidates);
    }

    /**
     * Drops the collections of the inverted index nobody reads: what a rebuild that died left
     * (its rNN, or an old collection whose drop it never did). Never the one of CURRENT, the one
     * being filled, nor the one a finished rebuild left for Search while its margin lasts.
     */
    private void dropUnused(String current, String target, Rebuild started) {
        Instant now = clock.instant();
        for (String name : repository.collections()) {
            boolean ours = name.equals(defaultCollection) || rebuilt.matcher(name).matches();
            boolean waiting = started.keepsPendingDrop(now) && name.equals(started.pendingDrop());
            if (ours && !name.equals(current) && !name.equals(target) && !waiting) {
                LOG.info("rebuild: dropping {}, which nobody reads", name);
                repository.drop(name);
            }
        }
    }

    /** The scheduled drop of the old collection: not if a later switch made it CURRENT again. */
    private void dropIfUnused(String collection) {
        try {
            String current = publications.current().map(this::collectionOf).orElse(defaultCollection);
            if (!collection.equals(current)) {
                repository.drop(collection);
                LOG.info("rebuild: dropped {}, the collection before the switch", collection);
            }
        } catch (RuntimeException e) {
            LOG.warn("could not drop {} ({}); the next rebuild drops it", collection, e.toString());
        }
    }

    /**
     * inverted_index_rNN with NN one more than any used: the collections there, CURRENT and the
     * last rebuild, which may have failed and dropped its own. A name is never reused, so a
     * replica never writes into a recreated collection with an index it opened before.
     */
    private String nextCollection(Optional<Rebuild> last) {
        int highest = 0;
        List<String> names = new ArrayList<>(repository.collections());
        publications.current().map(Publication::collection).ifPresent(names::add);
        last.map(Rebuild::collection).ifPresent(names::add);
        for (String name : names) {
            Matcher matcher = rebuilt.matcher(name);
            if (matcher.matches()) {
                highest = Math.max(highest, Integer.parseInt(matcher.group(1)));
            }
        }
        return String.format("%s_r%02d", defaultCollection, highest + 1);
    }

    private String collectionOf(Publication current) {
        return current.collection() == null ? defaultCollection : current.collection();
    }

    /** Before the switch: drop the half-built collection, CURRENT is untouched. */
    private Rebuild failed(Rebuild progress, boolean switched, String target, String why) {
        LOG.warn("rebuild into {} failed: {}", target, why);
        if (!switched) {
            quietly("drop " + target, () -> repository.drop(target));
        }
        Rebuild failed = progress.failed(clock.instant(), why);
        quietly("record the failure of the rebuild", saver(failed));
        return failed;
    }

    private boolean markerKnownToExist() {
        try {
            return coordination.get().map(Coordination::isRebuilding).orElse(true);   // unknown: trust the document
        } catch (RuntimeException e) {
            return true;
        }
    }

    private Runnable saver(Rebuild rebuild) {
        return () -> repository.save(rebuild);
    }

    private static void quietly(String what, Runnable action) {
        try {
            action.run();
        } catch (RuntimeException e) {
            LOG.warn("rebuild: could not {} ({})", what, e.toString());
        }
    }

    /** A rebuild that must stop; reason goes in the response, in English. */
    static final class RebuildAbortedException extends RuntimeException {
        private final String reason;

        RebuildAbortedException(String reason) {
            super("La reconstrucción se detuvo: " + reason);
            this.reason = reason;
        }

        String reason() {
            return reason;
        }
    }

    /** POST /index/rebuild before the connection to Hazelcast is there: 503. */
    public static final class CoordinationUnavailableException extends RuntimeException {
        public CoordinationUnavailableException() {
            super("Todavía no hay conexión con el clúster de Hazelcast");
        }
    }
}
