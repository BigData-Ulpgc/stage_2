package es.ulpgc.bigdata.control;

import com.mongodb.client.MongoClient;
import es.ulpgc.bigdata.config.DataLayout;
import es.ulpgc.bigdata.config.Env;
import es.ulpgc.bigdata.config.MongoSettings;
import es.ulpgc.bigdata.coordination.BackgroundConnection;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.CoordinationSettings;
import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.datamart.indexlog.MongoIndexLog;
import es.ulpgc.bigdata.datamart.publication.MongoPublishedBooks;
import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Entry point of the Control Module (CF-09): the HTTP API (ControlApi), the periodic tasks
 * (ControlTasks: the expired-lease check and the publication) and the recovery of a cluster that
 * lost its state (Recovery, CF-10).
 *
 * Start order: the HTTP server first, then the Hazelcast client in the background
 * (BackgroundConnection), and once connected the periodic tasks (the first recovery check and
 * lease check run at once) and, with CONTROL_SEED_FILE, the seed of that file. GET /status
 * answers from the first moment, with "coordination": connecting | connected | disconnected |
 * failed | closed.
 *
 * Configuration (environment variables): ControlSettings (port, publication, INDEXING_URL,
 * CONTROL_SEED_FILE), CoordinationSettings (HZ_MEMBERS, LEASE_CHECK_SECONDS...), and for the
 * recovery MongoSettings (MONGO_URI, MONGO_DATABASE) and DataLayout (DATA_DIR, read-only).
 */
public final class App {

    static final String SERVICE_NAME = "control-module";

    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    private App() {
    }

    public static void main(String[] args) {
        RunningService service = start(Env.system());
        // docker stop sends SIGTERM: the JVM runs this hook before exiting.
        Runtime.getRuntime().addShutdownHook(new Thread(service::close, SERVICE_NAME + "-shutdown"));
    }

    /**
     * Wires and starts the service. Every variable, and the seed file, is read and validated
     * BEFORE starting anything, so a wrong one stops the start.
     */
    public static RunningService start(Env env) {
        Objects.requireNonNull(env, "env");
        ControlSettings settings = ControlSettings.fromEnv(env);
        CoordinationSettings coordination = CoordinationSettings.fromEnv(env);
        MongoSettings mongo = MongoSettings.fromEnv(env);
        DataLayout layout = DataLayout.fromEnv(env);
        List<Integer> seed = settings.seedFile().map(App::readSeed).orElse(List.of());

        // The client does not connect until it is used: the Control starts while MongoDB is down
        MongoClient mongoClient = mongo.openClient();
        Recovery.Evidence evidence = new StorageEvidence(
                new MongoPublishedBooks(mongoClient.getDatabase(mongo.database())),
                new MongoIndexLog(mongoClient.getDatabase(mongo.database())),
                new RangeBasedDatalake(layout.datalakeDir()));
        RunningService service = new RunningService(settings, coordination, seed, mongoClient, evidence);
        service.connection = BackgroundConnection.start(coordination, service::onConnected);
        try {
            service.server = new ControlApi(service.connection, service::tasksStatus)
                    .create(SERVICE_NAME, settings.httpThreads()).start(settings.port());
        } catch (RuntimeException e) {
            service.connection.close();                       // e.g. the port is taken
            mongoClient.close();
            throw e;
        }
        LOG.info("{} on port {} · Hazelcast {} · Indexing {} · publish every {} books or {} s · seed {} · recovery from MongoDB {} and {}",
                SERVICE_NAME, service.server.port(), coordination.members(), settings.indexingUrl(),
                settings.publishBatch(), settings.publishIntervalSeconds(),
                settings.seedFile().map(f -> f + " (" + seed.size() + " ids)").orElse("none"),
                mongo.database(), layout.datalakeDir().toAbsolutePath());
        return service;
    }

    private static List<Integer> readSeed(Path file) {
        try {
            return BookIdsFile.read(file);
        } catch (IOException e) {
            throw new UncheckedIOException(ControlSettings.SEED_FILE + " cannot be read: " + file, e);
        }
    }

    /**
     * The running service. close() stops it in order: the periodic tasks (after the current
     * run), the Hazelcast client, the MongoDB client, the HTTP server. Closing twice does nothing.
     */
    public static final class RunningService implements AutoCloseable {

        private final ControlSettings settings;
        private final CoordinationSettings coordinationSettings;
        private final List<Integer> seed;
        private final MongoClient mongoClient;
        private final Recovery.Evidence evidence;
        private BackgroundConnection connection;
        private Javalin server;
        private ControlTasks tasks;             // guarded by this
        private Recovery recovery;              // guarded by this
        private ControlScheduler scheduler;     // guarded by this
        private boolean closed;                 // guarded by this

        private RunningService(ControlSettings settings, CoordinationSettings coordinationSettings, List<Integer> seed,
                               MongoClient mongoClient, Recovery.Evidence evidence) {
            this.settings = settings;
            this.coordinationSettings = coordinationSettings;
            this.seed = seed;
            this.mongoClient = mongoClient;
            this.evidence = evidence;
        }

        /**
         * Runs in the connection thread once the cluster answers. The tasks start before the
         * seed: a seed that fails (the cluster went away again) is only logged, it can be
         * repeated with POST /control/jobs, but the lease check must run in any case.
         */
        private void onConnected(Coordination coordination) {
            synchronized (this) {
                if (closed) {
                    return;
                }
                tasks = new ControlTasks(coordination, new IndexingClient(settings.indexingUrl()), Clock.systemUTC(),
                        settings.publishBatch(), Duration.ofSeconds(settings.publishIntervalSeconds()));
                recovery = new Recovery(coordination, evidence, seed, Clock.systemUTC());
                scheduler = ControlScheduler.start(tasks, recovery,
                        Duration.ofSeconds(coordinationSettings.leaseCheckSeconds()));
            }
            LOG.info("connected to Hazelcast {} · lease check every {} s", coordinationSettings.members(),
                    coordinationSettings.leaseCheckSeconds());
            if (!seed.isEmpty()) {
                try {
                    int added = coordination.seed(seed);
                    LOG.info("seeded {}: {} new books, {} already known", settings.seedFile().orElseThrow(), added,
                            new HashSet<>(seed).size() - added);
                } catch (RuntimeException e) {
                    LOG.warn("seed of {} failed ({}); repeat it with POST /control/jobs",
                            settings.seedFile().orElseThrow(), e.toString());
                }
            }
        }

        private synchronized Map<String, Object> tasksStatus() {
            if (tasks == null) {
                return Map.of();
            }
            Map<String, Object> status = new LinkedHashMap<>(tasks.status());
            status.put("recovery", recovery.status());
            return status;
        }

        public BackgroundConnection.State coordinationState() {
            return connection.state();
        }

        public int port() {
            return server.port();
        }

        @Override
        public void close() {
            ControlScheduler running;
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                running = scheduler;
            }
            if (running != null) {
                running.close();
            }
            connection.close();
            mongoClient.close();
            server.stop();
        }
    }
}
