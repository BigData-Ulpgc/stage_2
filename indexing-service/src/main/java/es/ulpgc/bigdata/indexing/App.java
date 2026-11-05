package es.ulpgc.bigdata.indexing;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import es.ulpgc.bigdata.config.DataLayout;
import es.ulpgc.bigdata.config.Env;
import es.ulpgc.bigdata.config.MongoSettings;
import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.datamart.index.Tokenizer;
import es.ulpgc.bigdata.datamart.indexlog.MongoIndexLog;
import es.ulpgc.bigdata.datamart.metadata.MetadataParser;
import es.ulpgc.bigdata.datamart.metadata.MongoMetadataRepository;
import es.ulpgc.bigdata.http.ServiceServer;
import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Entry point of the Indexing Service: the only place that knows the concrete classes
 * (MongoDB, range datalake). It reads the configuration, creates every piece ONCE, wires them
 * together and starts the HTTP server; everything else receives interfaces.
 *
 * Configuration (environment variables, the same in Docker Compose and with java -jar):
 * <pre>
 *   INDEXING_PORT          HTTP port (default 7002; 0 = a free one, for tests)
 *   INDEXING_HTTP_THREADS  maximum Jetty request threads (default ServiceServer.DEFAULT_MAX_THREADS)
 *   DATA_DIR               root of the shared volume; the datalake is DATA_DIR/datalake/range (default ./data)
 *   MONGO_URI              mongodb://localhost:27017 (inside Docker: mongodb://mongo:27017)
 *   MONGO_DATABASE         search_engine
 *   MONGO_COLLECTION       inverted_index
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
 */
public final class App {

    static final String SERVICE_NAME = "indexing-service";
    static final String PORT = "INDEXING_PORT";
    static final int DEFAULT_PORT = 7002;
    static final String HTTP_THREADS = "INDEXING_HTTP_THREADS";

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
        MongoSettings mongo = MongoSettings.fromEnv(env);
        DataLayout layout = DataLayout.fromEnv(env);

        MongoClient client = mongo.openClient();
        try {
            MongoDatabase db = client.getDatabase(mongo.database());
            PublicationRepository publications = new MongoPublicationRepository(db);
            IndexingService service = new IndexingService(
                    new RangeBasedDatalake(layout.datalakeDir()),
                    new MongoMetadataRepository(db),
                    mongo.openIndex(client),                 // shared client: does not own it
                    new MongoIndexLog(db),
                    publications,
                    new MetadataParser(),
                    Tokenizer.standard());
            // The first version uses MONGO_COLLECTION; later ones keep the one of CURRENT (D8).
            Publisher publisher = new Publisher(publications, mongo.collection(), Clock.systemUTC());
            Javalin server = new IndexingApi(service, publisher).create(SERVICE_NAME, httpThreads).start(port);

            // Hosts only, never the whole URI: in production it may carry user and password.
            LOG.info("{} on port {} · datalake {} · MongoDB {} db={} index={}",
                    SERVICE_NAME, server.port(), layout.datalakeDir().toAbsolutePath(),
                    new ConnectionString(mongo.uri()).getHosts(), mongo.database(), mongo.collection());
            startRecovery(publisher);
            return new RunningService(server, client);
        } catch (RuntimeException e) {
            client.close();                                  // the start failed: do not leave it open
            throw e;
        }
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
     * then the MongoClient those requests use. Closing twice does nothing (the shutdown hook
     * and a test may both call it).
     */
    public static final class RunningService implements AutoCloseable {

        private final Javalin server;
        private final MongoClient client;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private RunningService(Javalin server, MongoClient client) {
            this.server = server;
            this.client = client;
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
                client.close();                              // even if stopping the server failed
            }
            LOG.info("{} stopped", SERVICE_NAME);
        }
    }
}
