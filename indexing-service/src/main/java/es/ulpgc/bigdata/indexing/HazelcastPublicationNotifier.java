package es.ulpgc.bigdata.indexing;

import es.ulpgc.bigdata.coordination.BookState;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.IndexPublished;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The publications told through Hazelcast (DR-08b, coordination contract §4.1 T10 and §6):
 * <ul>
 *   <li>T10: the books INDEXED in book-state that are visible in CURRENT become PUBLISHED;</li>
 *   <li>IndexPublished on index-events, only when a new version was created.</li>
 * </ul>
 *
 * T10 is not "the books of this version", but a sync: of the INDEXED books, those that
 * index_books has with a version &lt;= CURRENT. So the same step also repairs a T10 that was
 * interrupted (the process died after the CAS, or Hazelcast was down): contract §7. It runs
 * after every publish call and once when the client connects. A book is never marked before
 * CURRENT contains it (contract principle 2): the version comes from CURRENT itself.
 *
 * Until the client connects (CoordinationSettings.openClient blocks until the cluster answers)
 * and while it is disconnected, the notifications are skipped at once, without waiting for a
 * cluster that is down: the next publication or the next connection marks the states, and
 * Search's periodic check of CURRENT replaces a lost event (contract §6).
 *
 * Thread-safe: Publisher calls afterPublish under its lock, and the connection thread calls
 * connected. Two syncs at the same time are harmless: markPublished is a compare-and-set.
 */
public final class HazelcastPublicationNotifier implements PublicationNotifier {

    private static final Logger LOG = LoggerFactory.getLogger(HazelcastPublicationNotifier.class);

    private final PublicationRepository publications;
    private final AtomicReference<Coordination> coordination = new AtomicReference<>();
    private volatile boolean up = false;
    private final AtomicLong skipped = new AtomicLong();

    public HazelcastPublicationNotifier(PublicationRepository publications) {
        this.publications = Objects.requireNonNull(publications, "publications");
    }

    /**
     * The client is connected: from now on the publications are notified. Also applies the
     * T10 that may be pending (contract §7). Called once, by the thread that connected.
     *
     * @return how many books became PUBLISHED with that repair
     */
    public int connected(Coordination connected) {
        Objects.requireNonNull(connected, "connected");
        if (!coordination.compareAndSet(null, connected)) {
            throw new IllegalStateException("ya conectado a Hazelcast");
        }
        up = true;
        connected.onConnectionChange(this::connectionChanged);
        return syncStates();
    }

    /** Called by Hazelcast when the client loses the cluster (false) or gets it back (true). */
    void connectionChanged(boolean connectedNow) {
        up = connectedNow;
    }

    /** For GET /status: connecting, connected or disconnected. */
    public String state() {
        if (coordination.get() == null) {
            return "connecting";
        }
        return up ? "connected" : "disconnected";
    }

    /** New versions whose notification was skipped because Hazelcast was not there. */
    public long skipped() {
        return skipped.get();
    }

    @Override
    public void afterPublish(PublishResult result) {
        Objects.requireNonNull(result, "result");
        Coordination current = coordination.get();
        if (current == null || !up) {
            if (result.booksPublished() > 0) {          // nothing new: no event to lose
                skipped.incrementAndGet();
                LOG.warn("Hazelcast is not connected: states and event of version {} skipped "
                        + "(the next publication or connection marks the states)", result.version());
            }
            return;
        }
        if (result.version() != null) {
            markVisible(current, result.version());
        }
        if (result.booksPublished() > 0) {
            current.publish(new IndexPublished(result.version(), result.booksPublished(),
                    Math.toIntExact(result.totalBooks()), result.publishedAt()));
        }
    }

    /**
     * T10 for every INDEXED book visible in the CURRENT of now.
     *
     * @return how many books became PUBLISHED; 0 if not connected or nothing is published
     */
    public int syncStates() {
        Coordination current = coordination.get();
        if (current == null) {
            return 0;
        }
        Optional<Publication> published = publications.current();
        return published.map(p -> markVisible(current, p.version())).orElse(0);
    }

    private int markVisible(Coordination current, int version) {
        Set<Integer> indexed = current.idsIn(BookState.INDEXED);
        if (indexed.isEmpty()) {
            return 0;
        }
        List<Integer> visible = publications.visibleAmong(indexed, version);
        int marked = visible.isEmpty() ? 0 : current.markPublished(visible);
        if (marked > 0) {
            LOG.info("{} books PUBLISHED in book-state (version {})", marked, version);
        }
        return marked;
    }
}
