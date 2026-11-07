package es.ulpgc.bigdata.http;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.javalin.Javalin;
import io.javalin.http.BadRequestResponse;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    // ------------------------------------------------------------------
    // Common error format
    // ------------------------------------------------------------------

    @Test
    void anUnknownRouteAnswers404WithTheCommonFormat() throws Exception {
        app = ServiceServer.create("test-service", ServiceServer.DEFAULT_MAX_THREADS).start(0);

        HttpResponse<String> response = get("/does/not/exist");

        JsonObject error = assertError(response, 404, "NOT_FOUND");
        assertTrue(error.get("field").isJsonNull());
        assertTrue(error.get("message").getAsString().contains("/does/not/exist"));
        assertTrue(response.headers().firstValue("Content-Type").orElse("").startsWith("application/json"));
    }

    @Test
    void aMethodTheRouteDoesNotHaveAlsoAnswers404() throws Exception {
        app = ServiceServer.create("test-service", ServiceServer.DEFAULT_MAX_THREADS).start(0);

        assertError(send("POST", "/status"), 404, "NOT_FOUND");      // only GET /status exists
    }

    @Test
    void anApiExceptionAnswersItsOwnStatusCodeAndField() throws Exception {
        app = ServiceServer.create("test-service", ServiceServer.DEFAULT_MAX_THREADS);
        app.get("/items/{id}", ctx -> {
            throw ApiException.invalid("id", "id must be a positive integer");
        });
        app.start(0);

        JsonObject error = assertError(get("/items/abc"), 400, "INVALID_PARAMETER");
        assertEquals("id", error.get("field").getAsString());
        assertEquals("id must be a positive integer", error.get("message").getAsString());
    }

    @Test
    void aRouteThatAnswers404OnPurposeKeepsItsOwnBody() throws Exception {
        // The trap of app.error(404, ...): it would replace this body with the "no route" one.
        app = ServiceServer.create("test-service", ServiceServer.DEFAULT_MAX_THREADS);
        app.get("/books/{id}", ctx -> {
            throw new ApiException(404, "NOT_FOUND", "book 84 is not in the published index", "book_id");
        });
        app.start(0);

        JsonObject error = assertError(get("/books/84"), 404, "NOT_FOUND");
        assertEquals("book 84 is not in the published index", error.get("message").getAsString());
        assertEquals("book_id", error.get("field").getAsString());
    }

    @Test
    void anUnexpectedExceptionAnswers500WithoutItsDetails() throws Exception {
        app = ServiceServer.create("test-service", ServiceServer.DEFAULT_MAX_THREADS);
        app.get("/boom", ctx -> {
            throw new IllegalStateException("secret detail: /home/user/data");
        });
        app.start(0);

        HttpResponse<String> response = get("/boom");

        JsonObject error = assertError(response, 500, "INTERNAL_ERROR");
        assertEquals("internal error", error.get("message").getAsString());
        assertFalse(response.body().contains("secret"));
    }

    @Test
    void aServiceHandlerForItsOwnExceptionWinsOverThe500One() throws Exception {
        app = ServiceServer.create("test-service", ServiceServer.DEFAULT_MAX_THREADS);
        app.get("/storage", ctx -> {
            throw new UnsupportedOperationException("database down");   // stands for MongoException
        });
        app.exception(UnsupportedOperationException.class, (e, ctx) ->
                ServiceServer.writeError(ctx, 503, "STORAGE_UNAVAILABLE", "the storage does not answer", null));
        app.start(0);

        JsonObject error = assertError(get("/storage"), 503, "STORAGE_UNAVAILABLE");
        assertTrue(error.get("field").isJsonNull());
    }

    // --- Javalin's own errors (CF-09: the Control receives JSON) ----------------------------

    @Test
    void aBadRequestResponseOfJavalinUsesTheCommonFormat() throws Exception {
        app = ServiceServer.create("test-service", ServiceServer.DEFAULT_MAX_THREADS);
        app.get("/bad", ctx -> {
            throw new BadRequestResponse("bad input");
        });
        app.start(0);
        JsonObject error = assertError(get("/bad"), 400, "INVALID_PARAMETER");
        assertEquals("bad input", error.get("message").getAsString());
    }

    @Test
    void aTypedParameterThatDoesNotConvertIs400NamingIt() throws Exception {
        app = ServiceServer.create("test-service", ServiceServer.DEFAULT_MAX_THREADS);
        app.get("/items/{n}", ctx -> ctx.json(ctx.pathParamAsClass("n", Integer.class).get()));
        app.start(0);
        JsonObject error = assertError(get("/items/abc"), 400, "INVALID_PARAMETER");    // was a 500
        assertEquals("n", error.get("field").getAsString());
    }

    @Test
    void aBodyThatIsNotJsonIs400() throws Exception {
        app = ServiceServer.create("test-service", ServiceServer.DEFAULT_MAX_THREADS);
        app.post("/jobs", ctx -> ctx.json(ctx.bodyAsClass(Map.class)));
        app.start(0);
        JsonObject error = assertError(send("POST", "/jobs", "{not json"), 400, "INVALID_PARAMETER");   // was a 500
        assertEquals("body", error.get("field").getAsString());
    }

    @Test
    void aBodyOfTheWrongShapeIs400Too() throws Exception {
        app = ServiceServer.create("test-service", ServiceServer.DEFAULT_MAX_THREADS);
        app.post("/jobs", ctx -> ctx.json(ctx.bodyAsClass(Map.class)));
        app.start(0);
        assertError(send("POST", "/jobs", "[1, 2, 3]"), 400, "INVALID_PARAMETER");    // an array, not an object
    }

    @Test
    void aWellFormedBodyStillWorks() throws Exception {
        app = ServiceServer.create("test-service", ServiceServer.DEFAULT_MAX_THREADS);
        app.post("/jobs", ctx -> ctx.json(ctx.bodyAsClass(Map.class)));
        app.start(0);
        HttpResponse<String> response = send("POST", "/jobs", "{\"book_ids\": [1342]}");
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("1342"));
    }

    @Test
    void theCodeOfAJavalinStatus() {
        assertEquals("NOT_FOUND", ServiceServer.codeOf(404));
        assertEquals("INVALID_PARAMETER", ServiceServer.codeOf(400));
        assertEquals("INVALID_PARAMETER", ServiceServer.codeOf(413));
        assertEquals("INTERNAL_ERROR", ServiceServer.codeOf(503));
    }

    @Test
    void statusStillWorksWithTheErrorHandlers() throws Exception {
        app = ServiceServer.create("test-service", ServiceServer.DEFAULT_MAX_THREADS).start(0);

        assertEquals(200, get("/status").statusCode());
    }

    /** Checks status and code, and that the body has exactly the three fields of the contract. */
    private static JsonObject assertError(HttpResponse<String> response, int status, String code) {
        assertEquals(status, response.statusCode(), response.body());
        JsonObject body = JsonParser.parseString(response.body()).getAsJsonObject();
        assertEquals(1, body.size(), response.body());
        JsonObject error = body.getAsJsonObject("error");
        assertEquals(code, error.get("code").getAsString());
        assertEquals(3, error.size(), "code, message and field (field may be null): " + response.body());
        return error;
    }

    private HttpResponse<String> get(String path) throws Exception {
        return send("GET", path);
    }

    private HttpResponse<String> send(String method, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + app.port() + path))
                .method(method, HttpRequest.BodyPublishers.noBody())
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> send(String method, String path, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + app.port() + path))
                .method(method, HttpRequest.BodyPublishers.ofString(body))
                .header("Content-Type", "application/json")
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
