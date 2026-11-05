package es.ulpgc.bigdata.coordination;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Connects a service to Hazelcast without blocking its start. Connecting blocks until the
 * cluster answers (CoordinationSettings.openClient), and GET /status must answer meanwhile: it
 * does not depend on Hazelcast (api/endpoints.md). So a service starts its HTTP server first and
 * then this, which connects in a daemon thread and calls {@code onConnected} once connected
 * (to start the workers, the lease check...).
 *
 * <pre>
 *   CONNECTING    until the cluster answers
 *   CONNECTED     connected; back here when a lost connection is recovered
 *   DISCONNECTED  the client lost the cluster and is reconnecting by itself
 *   FAILED        the connection could not even be attempted (a configuration error)
 *   CLOSED        close() was called
 * </pre>
 *
 * Written once for every service (Ingestion, Indexing and the Control had their own copy,
 * review of #32), with two races of those copies closed:
 * <ul>
 *   <li>The connection listener is registered BEFORE the state becomes CONNECTED, and the state
 *       only becomes CONNECTED if nothing changed meanwhile, so a disconnection right after
 *       connecting is not lost.</li>
 *   <li>close() while connecting: the client is closed as soon as it connects, and
 *       {@code onConnected} never runs.</li>
 * </ul>
 */
public final class BackgroundConnection implements AutoCloseable {

    public enum State {
        CONNECTING, CONNECTED, DISCONNECTED, FAILED, CLOSED;

        /** As GET /status shows it: "connecting", "connected"... */
        public String label() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.CONNECTING);
    private final AtomicReference<String> failure = new AtomicReference<>();
    private Coordination coordination;          // guarded by this
    private boolean closed;                     // guarded by this

    private BackgroundConnection() {
    }

    /** Connects with these settings in a daemon thread; {@code onConnected} runs in that thread. */
    public static BackgroundConnection start(CoordinationSettings settings, Consumer<Coordination> onConnected) {
        Objects.requireNonNull(settings, "settings");
        return start(() -> Coordination.connect(settings), onConnected);
    }

    /** The same with any way of connecting (the tests use it to control when it returns). */
    public static BackgroundConnection start(Supplier<Coordination> connector, Consumer<Coordination> onConnected) {
        Objects.requireNonNull(connector, "connector");
        Objects.requireNonNull(onConnected, "onConnected");
        BackgroundConnection connection = new BackgroundConnection();
        Thread thread = new Thread(() -> connection.connect(connector, onConnected), "hazelcast-connect");
        thread.setDaemon(true);                     // a JVM that stops does not wait for it
        thread.start();
        return connection;
    }

    private void connect(Supplier<Coordination> connector, Consumer<Coordination> onConnected) {
        Coordination connected;
        try {
            connected = connector.get();
        } catch (RuntimeException e) {
            failure.set(e.toString());
            state.compareAndSet(State.CONNECTING, State.FAILED);
            return;
        }
        synchronized (this) {
            if (closed) {
                connected.close();                  // closed while connecting
                return;
            }
            coordination = connected;
        }
        connected.onConnectionChange(up -> state.getAndUpdate(s ->
                s == State.CLOSED ? s : up ? State.CONNECTED : State.DISCONNECTED));
        state.compareAndSet(State.CONNECTING, State.CONNECTED);
        try {
            onConnected.accept(connected);
        } catch (RuntimeException e) {
            failure.set("onConnected failed: " + e);
        }
    }

    public State state() {
        return state.get();
    }

    /** Why the connection FAILED, or what failed in {@code onConnected}; empty if nothing did. */
    public Optional<String> failure() {
        return Optional.ofNullable(failure.get());
    }

    /** The coordination once connected (also while DISCONNECTED: the client reconnects by itself). */
    public synchronized Optional<Coordination> coordination() {
        return Optional.ofNullable(coordination);
    }

    /** Closes the client if it is connected, or as soon as it connects. Closing twice does nothing. */
    @Override
    public void close() {
        Coordination toClose;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            toClose = coordination;
            coordination = null;
        }
        state.set(State.CLOSED);
        if (toClose != null) {
            toClose.close();
        }
    }
}
