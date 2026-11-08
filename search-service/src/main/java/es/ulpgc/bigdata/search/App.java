package es.ulpgc.bigdata.search;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import es.ulpgc.bigdata.config.Env;
import es.ulpgc.bigdata.config.MongoSettings;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.CoordinationSettings;
import es.ulpgc.bigdata.datamart.metadata.MongoMetadataRepository;
import es.ulpgc.bigdata.datamart.index.Tokenizer;
import es.ulpgc.bigdata.http.ServiceServer;
import es.ulpgc.bigdata.search.http.CorsPolicy;
import es.ulpgc.bigdata.search.http.SearchApi;
import es.ulpgc.bigdata.search.index.IndexSnapshotLoader;
import es.ulpgc.bigdata.search.index.MongoPostingSource;
import es.ulpgc.bigdata.search.index.PublicationEvents;
import es.ulpgc.bigdata.search.index.SnapshotHolder;
import es.ulpgc.bigdata.search.index.SnapshotRefresher;
import es.ulpgc.bigdata.search.query.SearchEngine;
import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Entry point of the Search Service: loads the published index version from MongoDB and
 * serves the API of api/openapi.yaml: GET /search, /books/{book_id}, /search/filters and /status.
 *
 * Configuration (environment variables):
 *   SEARCH_PORT       HTTP port (default 7003)
 *   SEARCH_HTTP_THREADS  maximum Jetty request threads (default ServiceServer.DEFAULT_MAX_THREADS)
 *   SEARCH_MONGO_POOL_SIZE  maximum MongoDB connections of this replica (default 50)
 *   CORS_ORIGINS      origins allowed to call the API from a browser (CorsPolicy; default none)
 *   SEARCH_POLL_SECONDS  seconds between checks of the published version (default 5)
 *   MONGO_URI         MongoDB (default mongodb://localhost:27017; in Docker mongodb://mongo:27017)
 *   MONGO_DATABASE    database (default search_engine)
 *   HZ_MEMBERS, HZ_CLUSTER_NAME  Hazelcast cluster that announces new versions (CoordinationSettings)
 *
 * Search mounts no volume and never writes: everything it needs is in MongoDB. Hazelcast only
 * makes it react sooner to a publication: with the cluster down it keeps working on the
 * periodic check.
 */
public final class App {

    static final String SERVICE_NAME = "search-service";
    private static final int DEFAULT_POLL_SECONDS = 5;
    /**
     * A search uses one connection for one short query (the metadata are in memory), so far fewer
     * connections than HTTP threads are enough, and each one costs memory in MongoDB: with 4
     * replicas this allows 200. To be checked in the load tests (DP-12).
     */
    private static final int DEFAULT_MONGO_POOL_SIZE = 50;
    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    private App() {
    }

    public static void main(String[] args) {
        Env env = Env.system();
        int port = env.port("SEARCH_PORT", 7003);
        int threads = env.integerAtLeast("SEARCH_HTTP_THREADS", ServiceServer.DEFAULT_MAX_THREADS, ServiceServer.MIN_THREADS);
        int mongoPoolSize = env.integerAtLeast("SEARCH_MONGO_POOL_SIZE", DEFAULT_MONGO_POOL_SIZE, 1);
        CorsPolicy cors = CorsPolicy.parse(env.string(CorsPolicy.VARIABLE, null));
        int pollSeconds = env.integerAtLeast("SEARCH_POLL_SECONDS", DEFAULT_POLL_SECONDS, 1);
        MongoSettings mongo = MongoSettings.fromEnv(env);
        CoordinationSettings coordination = CoordinationSettings.fromEnv(env);

        // One MongoClient per process: it is thread-safe and keeps its own connection pool.
        MongoClient client = mongo.openClient(mongoPoolSize);
        MongoDatabase database = client.getDatabase(mongo.database());
        SnapshotHolder snapshots = new SnapshotHolder(
                new IndexSnapshotLoader(database, new MongoMetadataRepository(database)));
        loadFirstSnapshot(snapshots);

        SearchApi api = new SearchApi(snapshots, new SearchEngine(new MongoPostingSource(database)), Tokenizer.standard());
        Javalin server = api.create(SERVICE_NAME, threads, cors);
        server.start(port);
        SnapshotRefresher refresher = new SnapshotRefresher(snapshots, Duration.ofSeconds(pollSeconds));
        refresher.start();
        // After the HTTP server: connecting blocks until the cluster answers, and /status must
        // answer before that.
        PublicationEvents events = new PublicationEvents(() -> Coordination.connect(coordination), refresher::wakeUp);
        events.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            events.close();
            refresher.close();
            server.stop();
            client.close();
        }));
    }

    /**
     * The service starts even if MongoDB is down or nothing is published yet: /status answers
     * and the API returns 503 until SnapshotRefresher manages to load a version.
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
