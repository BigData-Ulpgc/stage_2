package es.ulpgc.bigdata.http;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.json.JavalinGson;
import io.javalin.router.EndpointNotFound;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Creates the Javalin server of a service with the conventions every service shares:
 *
 *  - JSON with Gson (Javalin uses Jackson by default, which is not on our classpath), and null
 *    fields written as null instead of omitted, so a response always has the same fields;
 *  - an EXPLICIT Jetty thread pool, so the number of request threads of each process is a
 *    documented configuration value and not a hidden framework default;
 *  - GET /status, the health endpoint used by Docker healthchecks and the Control Module;
 *  - the common ERROR FORMAT {"error": {code, message, field}} for what every service shares:
 *      ApiException            its own status and code (400 INVALID_PARAMETER...);
 *      a route that does not   404 NOT_FOUND (also a method the route does not have), instead
 *      exist                   of Javalin's plain-text "Endpoint GET /x not found";
 *      any other exception     500 INTERNAL_ERROR: a bug of ours, the details go to the log
 *                              and never to the client.
 *
 * The caller adds its own routes, the handlers of its own exceptions (e.g. MongoDB down -> 503,
 * writing the body with writeError) and calls start(port). Javalin picks the handler of the
 * most specific class of the exception, so a service handler always wins over the 500 one,
 * whatever the order in which they are registered.
 *
 * Why not app.error(404, ...): Javalin runs that handler for EVERY 404 response, also the ones
 * a route answers on purpose ("book 84 is not in the published index"), and it would replace
 * their body. Javalin throws EndpointNotFound only when no route matches, so handling that
 * exception touches nothing else. (Javalin 6.1.3; verified in ServiceServerTest.)
 *
 * Threads: Jetty runs each HTTP request in one thread of this pool, so requests to the same
 * process run concurrently and share its memory. Javalin 6.1.3 would create
 * QueuedThreadPool(max 250, min 8, idle 60 s) if none were given; DEFAULT_MAX_THREADS and
 * MIN_THREADS keep those values unless the service configures another maximum.
 */
public final class ServiceServer {

    /** Javalin 6.1.3 default maximum (JettyServer.defaultThreadPool), made explicit. */
    public static final int DEFAULT_MAX_THREADS = 250;

    /**
     * Smallest pool accepted. Jetty reserves some threads for accepting connections and
     * selecting sockets, and refuses to start if the pool cannot hold them plus a worker.
     */
    public static final int MIN_THREADS = 8;

    private static final int IDLE_TIMEOUT_MS = 60_000;

    private static final Logger LOG = LoggerFactory.getLogger(ServiceServer.class);

    private static final Gson GSON = new GsonBuilder().serializeNulls().create();

    private ServiceServer() {
    }

    /** A server whose GET /status only reports the service name. */
    public static Javalin create(String serviceName, int maxThreads) {
        return create(serviceName, maxThreads, Map::of);
    }

    /**
     * @param serviceName  name reported by GET /status, e.g. "indexing-service"
     * @param maxThreads   maximum Jetty request threads of this process (>= MIN_THREADS)
     * @param extraStatus  service-specific fields added to GET /status (e.g. the index version);
     *                     called on every request, so it must be cheap and thread-safe
     */
    public static Javalin create(String serviceName, int maxThreads,
                                 Supplier<Map<String, Object>> extraStatus) {
        Objects.requireNonNull(serviceName, "serviceName");
        Objects.requireNonNull(extraStatus, "extraStatus");
        if (maxThreads < MIN_THREADS) {
            throw new IllegalArgumentException("maxThreads must be >= " + MIN_THREADS + ", got " + maxThreads);
        }

        Javalin app = Javalin.create(config -> {
            config.showJavalinBanner = false;
            config.http.defaultContentType = "application/json";
            config.jsonMapper(new JavalinGson(GSON, false));
            QueuedThreadPool pool = new QueuedThreadPool(maxThreads, MIN_THREADS, IDLE_TIMEOUT_MS);
            pool.setName(serviceName + "-http");
            config.jetty.threadPool = pool;
        });

        app.get("/status", ctx -> {
            Map<String, Object> status = new LinkedHashMap<>();
            status.put("service", serviceName);
            status.put("status", "running");
            status.putAll(extraStatus.get());
            ctx.json(status);
        });

        app.exception(ApiException.class, (e, ctx) -> writeError(ctx, e.status(), e.code(), e.getMessage(), e.field()));
        app.exception(EndpointNotFound.class, (e, ctx) -> writeError(ctx, 404, "NOT_FOUND",
                "no route " + ctx.method() + " " + ctx.path(), null));
        app.exception(Exception.class, (e, ctx) -> {
            LOG.error("Unexpected error on {} {}", ctx.method(), ctx.path(), e);
            writeError(ctx, 500, "INTERNAL_ERROR", "internal error", null);
        });
        return app;
    }

    /**
     * Writes the common error body with the given status. For the handlers that each service
     * adds for its own exceptions, so every error of every service has the same shape.
     *
     * @param field request parameter that caused the error, or null (written as null, not omitted)
     */
    public static void writeError(Context ctx, int status, String code, String message, String field) {
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("code", code);
        error.put("message", message);
        error.put("field", field);
        ctx.status(status).json(Map.of("error", error));
    }
}
