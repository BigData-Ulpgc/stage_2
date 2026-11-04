package es.ulpgc.bigdata.ingestion;

import es.ulpgc.bigdata.config.Env;
import es.ulpgc.bigdata.http.ServiceServer;
import es.ulpgc.bigdata.ingestion.source.BookSource;
import es.ulpgc.bigdata.ingestion.source.BookSources;
import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point of the Ingestion Service.
 *
 * For now it starts the HTTP server with GET /status and builds the book source (CF-05), so a
 * wrong source configuration stops the start. The endpoints (CF-06), the datalake readiness
 * (CF-07) and the workers (CF-08) use that source.
 *
 * Configuration (environment variables):
 * <pre>
 *   INGESTION_PORT           HTTP port (default 7001)
 *   INGESTION_HTTP_THREADS   maximum Jetty request threads (default ServiceServer.DEFAULT_MAX_THREADS)
 *   INGESTION_SOURCE ...     where books come from, see BookSources
 * </pre>
 */
public final class App {

    static final String SERVICE_NAME = "ingestion-service";

    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    private App() {
    }

    public static void main(String[] args) {
        Env env = Env.system();
        int port = env.port("INGESTION_PORT", 7001);
        int httpThreads = env.integerAtLeast("INGESTION_HTTP_THREADS",
                ServiceServer.DEFAULT_MAX_THREADS, ServiceServer.MIN_THREADS);
        BookSource source = BookSources.fromEnv(env);

        Javalin server = ServiceServer.create(
                SERVICE_NAME,
                httpThreads);

        server.start(port);
        LOG.info("{} on port {} · source {}", SERVICE_NAME, server.port(), source.describe());
    }
}
