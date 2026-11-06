package es.ulpgc.bigdata.indexing;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import es.ulpgc.bigdata.config.DataLayout;
import es.ulpgc.bigdata.config.Env;
import es.ulpgc.bigdata.config.MongoSettings;
import es.ulpgc.bigdata.coordination.BackgroundConnection;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.CoordinationSettings;
import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.datamart.index.MongoInvertedIndex;
import es.ulpgc.bigdata.datamart.index.Tokenizer;
import es.ulpgc.bigdata.datamart.indexlog.MongoIndexLog;
import es.ulpgc.bigdata.datamart.metadata.MetadataParser;
import es.ulpgc.bigdata.datamart.metadata.MongoMetadataRepository;
import es.ulpgc.bigdata.http.ServiceServer;
import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Entry point of the Indexing Service: the only place that knows the concrete classes
 * (MongoDB, range datalake). It reads the configuration, creates every piece ONCE, wires them
 * together and starts the HTTP server; everything else receives interfaces.
 *
 * Configuration (environment variables, the same in Docker Compose and with java -jar):
 * <pre>
 *   INDEXING_PORT          HTTP port (default 7002; 0 = a free one, for tests)
 *   INDEXING_HTTP_THREADS  maximum Jetty request threads (default ServiceServer.DEFAULT_MAX_THREADS)
 *   INDEXING_THREADS       threads that prepare (read, parse, tokenize) the books of a batch (default 4)
 *   INDEX_WRITE_BATCH      the most books of index-queue written together (default 100)
 *   DATA_DIR               root of the shared volume; the datalake is DATA_DIR/datalake/range (default ./data)
 *   MONGO_URI              mongodb://localhost:27017 (inside Docker: mongodb://mongo:27017)
 *   MONGO_DATABASE         search_engine
 *   MONGO_COLLECTION       inverted_index
 *   HZ_MEMBERS, HZ_CLUSTER_NAME  the Hazelcast cluster (CoordinationSettings)
 * </pre>
 *
 * One MongoClient per process (D6): App creates it, shares its database with the four
 * repositories (books, inverted_index, index_log, publications) and closes it when the service
 * stops ("who creates, closes"). No constructor talks to MongoDB, so the service starts even
 * if MongoDB is down: GET /status answers and the endpoints that need MongoDB answer 503 until
 * it comes back (D4).
 *
 * Once the server is up, a background thread completes a publication interrupted by a previous
 * crash (DR-08a, D2 and D3). In the background so that a MongoDB that does not answer never
 * delays or stops the start; if it fails, the next POST /index/publish does the same work.
 *
 * The Hazelcast client connects in the background too (BackgroundConnection, CF-09): to mark
 * the published books (T10) and announce each version on index-events (DR-08b). Connecting
 * blocks until the cluster answers (CoordinationSettings.openClient), and GET /status must
 * answer meanwhile, so it starts after the server; /status shows "coordination" (connecting,
 * connected, disconnected, failed or closed) and "coordination_failure" (why it failed, or
 * what failed once connected; null if nothing did). Without Hazelcast the service publishes
 * anyway (HazelcastPublicationNotifier).
 *
 * Once connected, the same thread starts the index writer (DR-06b, IndexingWriter): the
 * consumer of index-queue, active while this replica holds the lock index-writer and a
 * standby otherwise. It writes in batches of up to INDEX_WRITE_BATCH books, prepared on
 * INDEXING_THREADS parser threads (DR-09). /status shows what it does in "writer" (null until
 * connected).
 */
public final class App {

    static final String SERVICE_NAME = "indexing-service";
    static final String PORT = "INDEXING_PORT";
    static final int DEFAULT_PORT = 7002;
    static final String HTTP_THREADS = "INDEXING_HTTP_THREADS";
    static final String THREADS = "INDEXING_THREADS";
    static final int DEFAULT_THREADS = 4;
    static final String WRITE_BATCH = "INDEX_WRITE_BATCH";
    static final int DEFAULT_WRITE_BATCH = 100;

    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    private App() {
    }

    public static void main(String[] args) {
        RunningService service = start(Env.system());
        // docker stop sends SIGTERM: the JVM runs this hook before exiting.
        Runtime.getRuntime().addShutdownHook(new Thread(service::close, SERVICE_NAME + "-shutdown"));
    }

    /**
     * Wires and starts the service. Every variable is read and validated BEFORE opening
     * anything, so a malformed one (INDEXING_PORT=abc) stops the start without leaving a
     * connection open.
     */
    public static RunningService start(Env env) {
        Objects.requireNonNull(env, "env");
        int port = env.port(PORT, DEFAULT_PORT);
        int httpThreads = env.integerAtLeast(HTTP_THREADS,
                ServiceServer.DEFAULT_MAX_THREADS, ServiceServer.MIN_THREADS);
        int threads = env.integerAtLeast(THREADS, DEFAULT_THREADS, 1);
        int writeBatch = env.integerAtLeast(WRITE_BATCH, DEFAULT_WRITE_BATCH, 1);
        MongoSettings mongo = MongoSettings.fromEnv(env);
        DataLayout layout = DataLayout.fromEnv(env);
        CoordinationSettings coordination = CoordinationSettings.fromEnv(env);

        MongoClient client = mongo.openClient();
        ExecutorService parsers = parserPool(threads);
        try {
            MongoDatabase db = client.getDatabase(mongo.database());
            PublicationRepository publications = new MongoPublicationRepository(db);
            IndexingService service = new IndexingService(
                    new RangeBasedDatalake(layout.datalakeDir()),
                    new MongoMetadataRepository(db),
                    // The collection of CURRENT (D8, DR-10), on the shared client that App closes
                    collection -> new MongoInvertedIndex(db, collection),
                    mongo.collection(),
                    new MongoIndexLog(db),
                    publications,
                    new MetadataParser(),
                    Tokenizer.standard(),
                    parsers);
            // The first version uses MONGO_COLLECTION; later ones keep the one of CURRENT (D8).
            HazelcastPublicationNotifier notifier = new HazelcastPublicationNotifier(publications);
            Publisher publisher = new Publisher(publications, mongo.collection(), Clock.systemUTC(), notifier);
            AtomicReference<IndexingWriter> writer = new AtomicReference<>();
            AtomicReference<BackgroundConnection> connection = new AtomicReference<>();
            Javalin server = new IndexingApi(service, publisher)
                    .create(SERVICE_NAME, httpThreads, () -> extraStatus(connection.get(), writer.get()))
                    .start(port);

            // Hosts only, never the whole URI: in production it may carry user and password.
            LOG.info("{} on port {} · datalake {} · MongoDB {} db={} index={}",
                    SERVICE_NAME, server.port(), layout.datalakeDir().toAbsolutePath(),
                    new ConnectionString(mongo.uri()).getHosts(), mongo.database(), mongo.collection());
            startRecovery(publisher);
            RunningService running = new RunningService(server, client, writer, notifier, parsers);
            running.connectInBackground(coordination, connection, service, writeBatch);
            return running;
        } catch (RuntimeException e) {
            parsers.shutdownNow();
            client.close();                                  // the start failed: do not leave it open
            throw e;
        }
    }

    /** DR-09: daemon threads, so they never keep the JVM alive; close() shuts them down. */
    private static ExecutorService parserPool(int threads) {
        AtomicInteger next = new AtomicInteger();
        return Executors.newFixedThreadPool(threads, task -> {
            Thread thread = new Thread(task, SERVICE_NAME + "-parser-" + next.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }

    /** The fields of GET /status beyond service and status: cheap, never MongoDB nor Hazelcast. */
    static Map<String, Object> extraStatus(BackgroundConnection connection, IndexingWriter writer) {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("coordination", connection == null
                ? BackgroundConnection.State.CONNECTING.label()       // the instant before it starts
                : connection.state().label());
        status.put("coordination_failure", connection == null ? null : connection.failure().orElse(null));
        status.put("writer", writer == null ? null : writer.status());
        return status;
    }

    /** D2, D3: daemon, so it never keeps the JVM alive; close() makes it fail if still running. */
    private static void startRecovery(Publisher publisher) {
        Thread recovery = new Thread(() -> {
            try {
                publisher.recover().ifPresent(result -> LOG.info(
                        "interrupted publication completed: version {} with {} books",
                        result.version(), result.booksPublished()));
            } catch (RuntimeException e) {
                LOG.warn("could not check for an interrupted publication at start ({}); "
                        + "the next POST /index/publish will", e.getMessage());
            }
        }, SERVICE_NAME + "-publish-recovery");
        recovery.setDaemon(true);
        recovery.start();
    }

    /**
     * The running service. close() stops it in order: first the HTTP server (no new requests),
     * then the index writer (after its current batch, see WRITER_GRACE) and the parser threads,
     * then the notifier (what it has queued, see HazelcastPublicationNotifier.CLOSE_GRACE), then
     * the Hazelcast client, then the MongoClient they use. Closing twice does nothing (the shutdown hook and
     * a test may both call it).
     */
    public static final class RunningService implements AutoCloseable {

        /**
         * How long close() waits for the writer to finish its batch; then it is interrupted, and
         * the books of the batch stay INDEXING until the Control requeues them (contract §7).
         */
        static final Duration WRITER_GRACE = Duration.ofSeconds(10);

        private final Javalin server;
        private final MongoClient client;
        private final AtomicReference<IndexingWriter> writer;
        private final HazelcastPublicationNotifier notifier;
        private final ExecutorService parsers;
        private BackgroundConnection connection;             // guarded by this
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private Thread writerThread;                         // guarded by this

        private RunningService(Javalin server, MongoClient client, AtomicReference<IndexingWriter> writer,
                               HazelcastPublicationNotifier notifier, ExecutorService parsers) {
            this.server = server;
            this.client = client;
            this.writer = writer;
            this.notifier = notifier;
            this.parsers = parsers;
        }

        /**
         * D2: in BackgroundConnection's daemon thread, so a JVM that stops does not wait for a
         * cluster that never answers. Once connected, in that thread: the notifier (it only
         * queues the announcement of what was skipped and the repair of the states) and the
         * writer. If one of them fails, /status shows it in coordination_failure.
         */
        private synchronized void connectInBackground(CoordinationSettings settings,
                                                      AtomicReference<BackgroundConnection> forStatus,
                                                      IndexingService service, int writeBatch) {
            connection = BackgroundConnection.start(settings, connected -> {
                notifier.connected(connected);
                LOG.info("connected to Hazelcast {}", settings.members());
                startWriter(connected, service, writeBatch);
            });
            forStatus.set(connection);
        }

        /** D6, D9: the writer starts as soon as there is a cluster, unless the service already stopped. */
        private synchronized void startWriter(Coordination connected, IndexingService service, int writeBatch) {
            if (closed.get()) {
                return;
            }
            IndexingWriter started = new IndexingWriter(connected, service,
                    Coordination.owner("indexing", 0), d -> Thread.sleep(d.toMillis()), writeBatch);
            connected.onConnectionChange(started::connectionChanged);
            writerThread = new Thread(started, SERVICE_NAME + "-writer");
            writer.set(started);
            writerThread.start();
        }

        /** Asks the writer to stop after its book, waits WRITER_GRACE, then interrupts it. */
        private void stopWriter() {
            IndexingWriter running;
            Thread thread;
            synchronized (this) {
                running = writer.get();
                thread = writerThread;
            }
            if (running == null) {
                return;
            }
            running.stop();
            try {
                thread.join(WRITER_GRACE.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (thread.isAlive()) {
                thread.interrupt();
            }
        }

        /** The real port, also when it was started with port 0. */
        public int port() {
            return server.port();
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            LOG.info("{} stopping", SERVICE_NAME);
            try {
                server.stop();
            } finally {
                try {
                    stopWriter();
                    parsers.shutdownNow();                   // idle: only the writer uses them
                    notifier.close();
                    BackgroundConnection started;
                    synchronized (this) {
                        started = connection;
                    }
                    if (started != null) {
                        started.close();                     // or as soon as it connects
                    }
                } finally {
                    client.close();                          // even if stopping the rest failed
                }
            }
            LOG.info("{} stopped", SERVICE_NAME);
        }
    }
}
