package es.ulpgc.bigdata.search.index;

import es.ulpgc.bigdata.coordination.Coordination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
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
 *
 * Each reconnection subscribes again and checks CURRENT once. A new cluster starts its ringbuffer
 * at sequence 0, while the old subscription keeps waiting at the sequence it had reached, so it
 * would not deliver anything until as many versions were published (measured: a restart of both
 * members). The check covers what was published while this replica was disconnected.
 */
public final class PublicationEvents implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PublicationEvents.class);

    private final Supplier<Coordination> connector;
    private final Runnable onPublished;
    /** Subscribing is a remote call, and Hazelcast must not block in its lifecycle thread. */
    private final ExecutorService subscriber = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "publication-events");
        thread.setDaemon(true);           // waiting for the cluster must not keep the JVM alive
        return thread;
    });
    private Coordination connected;
    private boolean closed;
    private UUID subscription;            // only touched by the subscriber thread

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
        subscriber.execute(this::connectAndSubscribe);
    }

    private void connectAndSubscribe() {
        Coordination coordination = null;
        try {
            coordination = connector.get();
            subscription = coordination.subscribe(event -> onPublished.run());
            if (keep(coordination)) {
                Coordination current = coordination;
                coordination.onConnectionChange(up -> {
                    if (up) {
                        resubscribeLater(current);
                    }
                });
                LOG.info("Listening to the index publications of Hazelcast");
            }
        } catch (RuntimeException e) {
            LOG.warn("Could not listen to Hazelcast, relying on the periodic check: {}", e.getMessage());
            closeAfterFailure(coordination);
        }
    }

    private void resubscribeLater(Coordination coordination) {
        try {
            subscriber.execute(() -> resubscribe(coordination));
        } catch (RejectedExecutionException e) {
            LOG.debug("Ignoring a reconnection: the service is stopping");
        }
    }

    /** If it fails, the next reconnection tries again; the periodic check covers the meantime. */
    private void resubscribe(Coordination coordination) {
        try {
            if (subscription != null) {
                coordination.unsubscribe(subscription);
            }
            subscription = coordination.subscribe(event -> onPublished.run());
            LOG.info("Reconnected to Hazelcast: listening to the index publications again");
        } catch (RuntimeException e) {
            LOG.warn("Could not listen to Hazelcast again, relying on the periodic check: {}", e.getMessage());
        }
        try {
            onPublished.run();
        } catch (RuntimeException e) {
            LOG.warn("The check after reconnecting failed: {}", e.getMessage());
        }
    }

    /** A client that is not going to be used must not keep its threads and its retries alive. */
    private static void closeAfterFailure(Coordination coordination) {
        if (coordination == null) {
            return;
        }
        try {
            coordination.close();
        } catch (RuntimeException e) {
            LOG.debug("Could not close the Hazelcast client: {}", e.getMessage());
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
        subscriber.shutdownNow();
        if (coordination != null) {
            coordination.close();
        }
    }
}
