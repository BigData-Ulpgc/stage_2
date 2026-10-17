package es.ulpgc.bigdata.indexing;

import es.ulpgc.bigdata.config.Env;
import es.ulpgc.bigdata.http.ServiceServer;
import io.javalin.Javalin;

/**
 * Entry point of the Indexing Service.
 *
 * DR-01: for now it only starts the HTTP server with GET /status. The service
 * endpoints and
 * workers are added by DR-05 to DR-10.
 *
 * Configuration (environment variables):
 * INDEXING_PORT HTTP port (default 7002)
 * INDEXING_HTTP_THREADS maximum Jetty request threads (default
 * ServiceServer.DEFAULT_MAX_THREADS)
 */
public final class App {

    static final String SERVICE_NAME = "indexing-service";

    private App() {
    }

    public static void main(String[] args) {
        Env env = Env.system();
        int port = env.port("INDEXING_PORT", 7002);
        int httpThreads = env.integerAtLeast("INDEXING_HTTP_THREADS",
                ServiceServer.DEFAULT_MAX_THREADS, ServiceServer.MIN_THREADS);

        Javalin server = ServiceServer.create(
                SERVICE_NAME,
                httpThreads);

        server.start(port);
    }
}
