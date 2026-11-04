package es.ulpgc.bigdata.ingestion;

import es.ulpgc.bigdata.config.DataLayout;
import es.ulpgc.bigdata.config.Env;
import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.http.ServiceServer;
import es.ulpgc.bigdata.ingestion.source.BookSource;
import es.ulpgc.bigdata.ingestion.source.BookSources;
import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Entry point of the Ingestion Service: the HTTP API of CF-06 on the book source of CF-05 and
 * the range datalake. The datalake readiness (CF-07) and the ingest-queue workers (CF-08) come
 * next.
 *
 * Configuration (environment variables):
 * <pre>
 *   INGESTION_PORT           HTTP port (default 7001)
 *   INGESTION_HTTP_THREADS   maximum Jetty request threads (default ServiceServer.DEFAULT_MAX_THREADS)
 *   DATA_DIR                 the datalake is DATA_DIR/datalake/range (default ./data)
 *   INGESTION_SOURCE ...     where books come from, see BookSources
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
        Javalin server = start(Env.system());
        // docker stop sends SIGTERM: stop accepting requests before the JVM exits.
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop, SERVICE_NAME + "-shutdown"));
    }

    /**
     * Wires and starts the service. Every variable is read and validated BEFORE starting, so a
     * wrong one (INGESTION_SOURCE=s3, a missing INGESTION_LOCAL_DIR) stops the start.
     */
    public static Javalin start(Env env) {
        Objects.requireNonNull(env, "env");
        int port = env.port(PORT, DEFAULT_PORT);
        int httpThreads = env.integerAtLeast(HTTP_THREADS,
                ServiceServer.DEFAULT_MAX_THREADS, ServiceServer.MIN_THREADS);
        DataLayout layout = DataLayout.fromEnv(env);
        BookSource source = BookSources.fromEnv(env);

        IngestionService service = new IngestionService(source, new BookSplitter(),
                new RangeBasedDatalake(layout.datalakeDir()));
        Javalin server = new IngestionApi(service, layout.dataDir()).create(SERVICE_NAME, httpThreads).start(port);
        LOG.info("{} on port {} · datalake {} · source {}", SERVICE_NAME, server.port(),
                layout.datalakeDir().toAbsolutePath(), source.describe());
        return server;
    }
}
