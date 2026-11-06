package es.ulpgc.bigdata.ingestion;

import es.ulpgc.bigdata.config.DataLayout;
import es.ulpgc.bigdata.config.Env;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.CoordinationSettings;
import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.http.ServiceServer;
import es.ulpgc.bigdata.ingestion.source.BookSource;
import es.ulpgc.bigdata.ingestion.source.BookSources;
import es.ulpgc.bigdata.ingestion.worker.IngestionWorkers;
import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Entry point of the Ingestion Service: the HTTP API (CF-06) and the ingest-queue workers
 * (CF-08), on the book source (CF-05) and the range datalake.
 *
 * Start order: the HTTP server first, then the Hazelcast client in the background, then the
 * workers. Connecting blocks until the cluster answers (CoordinationSettings.openClient), and
 * GET /status must answer meanwhile: it does not depend on Hazelcast (api/endpoints.md). It
 * shows "coordination": disabled | connecting | connected | disconnected.
 *
 * Configuration (environment variables):
 * <pre>
 *   INGESTION_PORT           HTTP port (default 7001)
 *   INGESTION_HTTP_THREADS   maximum Jetty request threads (default ServiceServer.DEFAULT_MAX_THREADS)
 *   DATA_DIR                 the datalake is DATA_DIR/datalake/range (default ./data)
 *   INGESTION_SOURCE ...     where books come from, see BookSources
 *   INGESTION_WORKERS ...    workers of the replica, see IngestionWorkers (0: no Hazelcast)
 *   HZ_MEMBERS ...           the cluster, see CoordinationSettings
 * </pre>
 */
public final class App {

    static final String SERVICE_NAME = "ingestion-service";
    static final String PORT = "INGESTION_PORT";
    static final int DEFAULT_PORT = 7001;
    static final String HTTP_THREADS = "INGESTION_HTTP_THREADS";

    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    private App() {
    }

    public static void main(String[] args) {
        RunningService service = start(Env.system());
        // docker stop sends SIGTERM: the JVM runs this hook before exiting.
        Runtime.getRuntime().addShutdownHook(new Thread(service::close, SERVICE_NAME + "-shutdown"));
    }

    /**
     * Wires and starts the service. Every variable is read and validated BEFORE starting
     * anything, so a wrong one stops the start.
     */
    public static RunningService start(Env env) {
        Objects.requireNonNull(env, "env");
        int port = env.port(PORT, DEFAULT_PORT);
        int httpThreads = env.integerAtLeast(HTTP_THREADS,
                ServiceServer.DEFAULT_MAX_THREADS, ServiceServer.MIN_THREADS);
        DataLayout layout = DataLayout.fromEnv(env);
        BookSource source = BookSources.fromEnv(env);
        int workers = IngestionWorkers.count(env);
        int fetchRetries = IngestionWorkers.fetchRetries(env);
        CoordinationSettings coordination = workers > 0 ? CoordinationSettings.fromEnv(env) : null;

        IngestionService ingestion = new IngestionService(source, new BookSplitter(),
                new RangeBasedDatalake(layout.datalakeDir()));
        RunningService service = new RunningService();
        service.server = new IngestionApi(ingestion, layout.dataDir())
                .create(SERVICE_NAME, httpThreads, service::status).start(port);
        LOG.info("{} on port {} · datalake {} · source {} · {} workers", SERVICE_NAME, service.server.port(),
                layout.datalakeDir().toAbsolutePath(), source.describe(), workers);

        if (coordination == null) {
            service.coordinationState.set("disabled");
        } else {
            service.coordinationState.set("connecting");
            Thread connector = new Thread(() -> service.connect(coordination, ingestion, workers, fetchRetries),
                    SERVICE_NAME + "-connect");
            connector.setDaemon(true);                       // a JVM that stops does not wait for it
            connector.start();
        }
        return service;
    }

    /**
     * The running service. close() stops it in order: the workers (after their current book),
     * the Hazelcast client, the HTTP server. Closing twice does nothing.
     */
    public static final class RunningService implements AutoCloseable {

        private Javalin server;
        private final AtomicReference<String> coordinationState = new AtomicReference<>("disabled");
        private final AtomicReference<Coordination> coordination = new AtomicReference<>();
        private final AtomicReference<IngestionWorkers> workers = new AtomicReference<>();
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private RunningService() {
        }

        /** Runs in the background: blocks until the cluster answers, then starts the workers. */
        private void connect(CoordinationSettings settings, IngestionService ingestion, int count, int fetchRetries) {
            Coordination connected = Coordination.connect(settings);
            if (closed.get()) {
                connected.close();
                return;
            }
            coordination.set(connected);
            connected.onConnectionChange(up -> coordinationState.set(up ? "connected" : "disconnected"));
            coordinationState.set("connected");
            workers.set(IngestionWorkers.start(connected, ingestion, count, fetchRetries, Clock.systemUTC()));
            LOG.info("connected to Hazelcast {} · {} workers started", settings.members(), count);
        }

        /** The extra fields of GET /status. */
        Map<String, Object> status() {
            Map<String, Object> status = new LinkedHashMap<>();
            status.put("coordination", coordinationState.get());
            IngestionWorkers running = workers.get();
            if (running != null) {
                status.put("workers", running.status());
            }
            return status;
        }

        public int port() {
            return server.port();
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            IngestionWorkers running = workers.get();
            if (running != null) {
                running.close();
            }
            Coordination connected = coordination.get();
            if (connected != null) {
                connected.close();
            }
            server.stop();
        }
    }
}
