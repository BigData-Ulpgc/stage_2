package es.ulpgc.bigdata.search;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import es.ulpgc.bigdata.config.Env;
import es.ulpgc.bigdata.config.MongoSettings;
import es.ulpgc.bigdata.datamart.metadata.MongoMetadataRepository;
import es.ulpgc.bigdata.http.ServiceServer;
import es.ulpgc.bigdata.search.index.IndexSnapshotLoader;
import es.ulpgc.bigdata.search.index.SnapshotHolder;
import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point of the Search Service: loads the published index version from MongoDB and
 * serves GET /status. The search endpoints of api/openapi.yaml are added in DP-04 and DP-05.
 *
 * Configuration (environment variables):
 *   SEARCH_PORT       HTTP port (default 7003)
 *   SEARCH_THREADS    maximum Jetty request threads (default ServiceServer.DEFAULT_MAX_THREADS)
 *   MONGO_URI         MongoDB (default mongodb://localhost:27017; in Docker mongodb://mongo:27017)
 *   MONGO_DATABASE    database (default search_engine)
 *
 * Search mounts no volume and never writes: everything it needs is in MongoDB.
 */
public final class App {

    static final String SERVICE_NAME = "search-service";
    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    private App() {
    }

    public static void main(String[] args) {
        Env env = Env.system();
        int port = env.port("SEARCH_PORT", 7003);
        int threads = env.integerAtLeast("SEARCH_THREADS", ServiceServer.DEFAULT_MAX_THREADS, ServiceServer.MIN_THREADS);
        MongoSettings mongo = MongoSettings.fromEnv(env);

        // One MongoClient per process: it is thread-safe and keeps its own connection pool.
        MongoClient client = mongo.openClient();
        MongoDatabase database = client.getDatabase(mongo.database());
        SnapshotHolder snapshots = new SnapshotHolder(
                new IndexSnapshotLoader(database, new MongoMetadataRepository(database)));
        loadFirstSnapshot(snapshots);

        Javalin server = ServiceServer.create(SERVICE_NAME, threads, snapshots::status);
        server.start(port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop();
            client.close();
        }));
    }

    /**
     * The service starts even if MongoDB is down or nothing is published yet: /status answers
     * and the API returns 503 until a version is loaded (reload in DP-06).
     */
    private static void loadFirstSnapshot(SnapshotHolder snapshots) {
        try {
            snapshots.reload().ifPresentOrElse(
                    s -> LOG.info("Serving index version {} ({} books, collection {})", s.version(), s.bookCount(), s.collection()),
                    () -> LOG.info("No index version published yet"));
        } catch (RuntimeException e) {
            LOG.warn("Could not load the published index from MongoDB: {}", e.getMessage());
        }
    }
}
