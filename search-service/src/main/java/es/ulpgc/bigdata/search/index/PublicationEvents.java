package es.ulpgc.bigdata.search.index;

import es.ulpgc.bigdata.coordination.Coordination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Listens to the IndexPublished events of Hazelcast (coordination contract §6), so this replica
 * loads a new version right after it is published instead of at the next periodic check.
 *
 * The event is only a wake-up: its content is not used, and whoever receives it reads CURRENT
 * from MongoDB. A lost, repeated or out-of-order event therefore does no harm, and with the
 * cluster down the periodic check of SnapshotRefresher keeps the replica up to date.
 *
 * Connecting blocks until the cluster answers, so it runs in its own background thread and the
 * service never waits for Hazelcast.
 */
public final class PublicationEvents implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PublicationEvents.class);

    private final Supplier<Coordination> connector;
    private final Runnable onPublished;
    private Coordination connected;
    private boolean closed;

    /**
     * @param connector   opens the connection; may block for a long time
     * @param onPublished called for each event, on a Hazelcast thread: it must return quickly
     */
    public PublicationEvents(Supplier<Coordination> connector, Runnable onPublished) {
        this.connector = Objects.requireNonNull(connector, "connector");
        this.onPublished = Objects.requireNonNull(onPublished, "onPublished");
    }

    /** Connects and subscribes in the background. Returns at once. */
    public void start() {
        Thread thread = new Thread(this::connectAndSubscribe, "publication-events");
        thread.setDaemon(true);           // waiting for the cluster must not keep the JVM alive
        thread.start();
    }

    private void connectAndSubscribe() {
        try {
            Coordination coordination = connector.get();
            coordination.subscribe(event -> onPublished.run());
            if (keep(coordination)) {
                LOG.info("Listening to the index publications of Hazelcast");
            }
        } catch (RuntimeException e) {
            LOG.warn("Could not listen to Hazelcast, relying on the periodic check: {}", e.getMessage());
        }
    }

    /** False if the service stopped while it was connecting: the connection is closed here. */
    private boolean keep(Coordination coordination) {
        synchronized (this) {
            if (!closed) {
                connected = coordination;
                return true;
            }
        }
        coordination.close();
        return false;
    }

    @Override
    public void close() {
        Coordination coordination;
        synchronized (this) {
            closed = true;
            coordination = connected;
            connected = null;
        }
        if (coordination != null) {
            coordination.close();
        }
    }
}
