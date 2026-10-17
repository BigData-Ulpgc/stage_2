package es.ulpgc.bigdata.http;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.javalin.Javalin;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Starts a real server on a free port (0) and calls it over HTTP, as a Docker healthcheck would. */
class ServiceServerTest {

    private final HttpClient client = HttpClient.newHttpClient();
    private Javalin app;

    @AfterEach
    void stop() {
        if (app != null) {
            app.stop();
        }
    }

    @Test
    void statusReportsTheServiceAsRunning() throws Exception {
        app = ServiceServer.create("test-service", ServiceServer.DEFAULT_MAX_THREADS).start(0);

        HttpResponse<String> response = get("/status");

        assertEquals(200, response.statusCode());
        assertTrue(response.headers().firstValue("Content-Type").orElse("").startsWith("application/json"));
        JsonObject body = JsonParser.parseString(response.body()).getAsJsonObject();
        assertEquals("test-service", body.get("service").getAsString());
        assertEquals("running", body.get("status").getAsString());
    }

    @Test
    void extraStatusFieldsAreAddedAndNullsAreWritten() throws Exception {
        Map<String, Object> extra = new HashMap<>();
        extra.put("index_version", null);           // e.g. Search before the first publication
        extra.put("books_indexed", 15);
        app = ServiceServer.create("search-service", ServiceServer.DEFAULT_MAX_THREADS, () -> extra).start(0);

        JsonObject body = JsonParser.parseString(get("/status").body()).getAsJsonObject();

        assertTrue(body.has("index_version"));
        assertTrue(body.get("index_version").isJsonNull());
        assertEquals(15, body.get("books_indexed").getAsInt());
    }

    @Test
    void usesTheConfiguredThreadPool() {
        app = ServiceServer.create("test-service", 16).start(0);

        QueuedThreadPool pool = (QueuedThreadPool) app.jettyServer().server().getThreadPool();
        assertEquals(16, pool.getMaxThreads());
        assertEquals(ServiceServer.MIN_THREADS, pool.getMinThreads());
        assertEquals("test-service-http", pool.getName());
    }

    @Test
    void rejectsAPoolTooSmallForJetty() {
        assertThrows(IllegalArgumentException.class, () -> ServiceServer.create("test-service", 2));
    }

    private HttpResponse<String> get(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + app.port() + path)).GET().build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
