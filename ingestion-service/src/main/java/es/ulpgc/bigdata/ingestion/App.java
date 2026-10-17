package es.ulpgc.bigdata.ingestion;

import es.ulpgc.bigdata.config.Env;
import es.ulpgc.bigdata.http.ServiceServer;
import io.javalin.Javalin;

/**
 * Entry point of the Ingestion Service.
 *
 * DR-01: for now it only starts the HTTP server with GET /status. The service
 * endpoints and
 * workers are added by CF-05 to CF-08.
 *
 * Configuration (environment variables):
 * INGESTION_PORT HTTP port (default 7001)
 * INGESTION_HTTP_THREADS maximum Jetty request threads (default
 * ServiceServer.DEFAULT_MAX_THREADS)
 */
public final class App {

    static final String SERVICE_NAME = "ingestion-service";

    private App() {
    }

    public static void main(String[] args) {
        Env env = Env.system();
        int port = env.port("INGESTION_PORT", 7001);
        int httpThreads = env.integerAtLeast("INGESTION_HTTP_THREADS",
                ServiceServer.DEFAULT_MAX_THREADS, ServiceServer.MIN_THREADS);

        Javalin server = ServiceServer.create(
                SERVICE_NAME,
                httpThreads);

        server.start(port);
    }
}
