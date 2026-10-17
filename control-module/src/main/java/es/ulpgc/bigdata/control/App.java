package es.ulpgc.bigdata.control;

import es.ulpgc.bigdata.config.Env;
import es.ulpgc.bigdata.http.ServiceServer;
import io.javalin.Javalin;

/**
 * Entry point of the Control Module.
 *
 * DR-01: for now it only starts the HTTP server with GET /status. The service
 * endpoints and
 * workers are added by CF-09 and CF-10.
 *
 * Configuration (environment variables):
 * CONTROL_PORT HTTP port (default 7000)
 * CONTROL_HTTP_THREADS maximum Jetty request threads (default
 * ServiceServer.DEFAULT_MAX_THREADS)
 */
public final class App {

    static final String SERVICE_NAME = "control-module";

    private App() {
    }

    public static void main(String[] args) {
        Env env = Env.system();
        int port = env.port("CONTROL_PORT", 7000);
        int httpThreads = env.integerAtLeast("CONTROL_HTTP_THREADS",
                ServiceServer.DEFAULT_MAX_THREADS, ServiceServer.MIN_THREADS);

        Javalin server = ServiceServer.create(
                SERVICE_NAME,
                httpThreads);

        server.start(port);
    }
}
