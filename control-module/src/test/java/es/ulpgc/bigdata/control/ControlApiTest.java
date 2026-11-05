package es.ulpgc.bigdata.control;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hazelcast.core.HazelcastInstance;
import es.ulpgc.bigdata.coordination.BackgroundConnection;
import es.ulpgc.bigdata.coordination.BookState;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.ErrorCode;
import es.ulpgc.bigdata.coordination.Stage;
import es.ulpgc.bigdata.http.ServiceServer;
import es.ulpgc.bigdata.testsupport.HazelcastTestCluster;
import io.javalin.Javalin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The routes of the Control, on the embedded cluster. */
class ControlApiTest {

    private static final Duration SHORT = Duration.ofMillis(200);

    private static HazelcastInstance client;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final MutableClock clock = new MutableClock();
    private Coordination coordination;
    private BackgroundConnection connection;
    private Javalin server;

    @BeforeAll
    static void startCluster() {
        client = HazelcastTestCluster.settings().openClient();
    }

    @AfterAll
    static void stopClient() {
        client.shutdown();
    }

    @BeforeEach
    void start() throws Exception {
        HazelcastTestCluster.reset();
        coordination = Coordination.on(client, HazelcastTestCluster.settings(), clock);
        connection = BackgroundConnection.start(() -> coordination, c -> { });
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (connection.state() != BackgroundConnection.State.CONNECTED && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        server = new ControlApi(connection, () -> Map.of("publication", Map.of("batch", 100)))
                .create("control-module", ServiceServer.MIN_THREADS).start(0);
    }

    @AfterEach
    void stop() {
        server.stop();
        connection.close();                     // Coordination.on: the shared client stays open
    }

    // --- POST /control/jobs --------------------------------------------------------------

    @Test
    void jobsSeedsEachBookOnce() throws Exception {
        HttpResponse<String> first = send("POST", "/control/jobs", "{\"book_ids\": [1342, 84, 1342]}");
        assertEquals(202, first.statusCode());
        assertEquals(json("{\"submitted\": 2, \"added\": 2, \"already_known\": 0}"), json(first.body()));

        HttpResponse<String> again = send("POST", "/control/jobs", "{\"book_ids\": [84, 11]}");
        assertEquals(json("{\"submitted\": 2, \"added\": 1, \"already_known\": 1}"), json(again.body()));
        assertEquals(3, coordination.queueSize(Stage.INGESTION));
        assertEquals(BookState.PENDING, coordination.task(11).orElseThrow().bookState());
    }

    @Test
    void aBadIdRejectsTheWholeRequest() throws Exception {
        for (String bad : List.of("\"84\"", "0", "-1", "1.5", "1000000000", "1e20", "null", "true", "[1]")) {
            HttpResponse<String> response = send("POST", "/control/jobs", "{\"book_ids\": [1342, " + bad + "]}");
            assertEquals(400, response.statusCode(), bad);
            JsonObject error = json(response.body()).getAsJsonObject("error");
            assertEquals("INVALID_PARAMETER", error.get("code").getAsString(), bad);
            assertEquals("book_ids", error.get("field").getAsString(), bad);
            assertTrue(error.get("message").getAsString().contains("book_ids[1]"), error.toString());
        }
        assertTrue(coordination.task(1342).isEmpty(), "nothing is seeded");
    }

    @Test
    void aBodyOfAnotherShapeIsRejected() throws Exception {
        assertError(send("POST", "/control/jobs", "{\"book_ids\": "), 400, "INVALID_PARAMETER", "body");
        assertError(send("POST", "/control/jobs", "[1342]"), 400, "INVALID_PARAMETER", "body");
        assertError(send("POST", "/control/jobs", "{}"), 400, "MISSING_PARAMETER", "book_ids");
        assertError(send("POST", "/control/jobs", "{\"book_ids\": 1342}"), 400, "INVALID_PARAMETER", "book_ids");
        assertError(send("POST", "/control/jobs", "{\"book_ids\": []}"), 400, "INVALID_PARAMETER", "book_ids");
        String tooMany = IntStream.rangeClosed(1, ControlApi.MAX_BOOK_IDS + 1).mapToObj(String::valueOf)
                .collect(Collectors.joining(",", "{\"book_ids\": [", "]}"));
        assertError(send("POST", "/control/jobs", tooMany), 400, "INVALID_PARAMETER", "book_ids");
    }

    // --- GET /control/status -------------------------------------------------------------

    @Test
    void statusCountsTheBooksPerStateAndTheQueues() throws Exception {
        coordination.seed(List.of(1, 2, 3));
        coordination.complete(coordination.claim(Stage.INGESTION, "ingestion-test/w0", SHORT).orElseThrow());

        HttpResponse<String> response = send("GET", "/control/status", null);
        assertEquals(200, response.statusCode());
        JsonObject status = json(response.body());
        assertEquals("connected", status.get("coordination").getAsString());
        JsonObject books = status.getAsJsonObject("books");
        assertEquals(2, books.get("PENDING").getAsInt());
        assertEquals(1, books.get("DOWNLOADED").getAsInt());
        assertEquals(0, books.get("FAILED").getAsInt());
        assertEquals(3, books.get("total").getAsInt());
        assertEquals(2, status.getAsJsonObject("queues").get("ingest-queue").getAsInt());
        assertEquals(1, status.getAsJsonObject("queues").get("index-queue").getAsInt());
        assertEquals(100, status.getAsJsonObject("publication").get("batch").getAsInt());
    }

    // --- GET /control/tasks/{book_id} ----------------------------------------------------

    @Test
    void oneBookInSnakeCase() throws Exception {
        coordination.seed(List.of(1342));
        JsonObject task = json(send("GET", "/control/tasks/1342", null).body());
        assertEquals(1342, task.get("book_id").getAsInt());
        assertEquals("PENDING", task.get("state").getAsString());
        assertEquals(0, task.get("attempts").getAsInt());
        assertEquals(clock.instant().toString(), task.get("enqueued_at").getAsString());
        assertTrue(task.get("owner").isJsonNull());
        assertTrue(task.get("lease_until").isJsonNull());
        assertTrue(task.get("started_at").isJsonNull());
    }

    @Test
    void anUnknownOrInvalidBookIsAnError() throws Exception {
        assertError(send("GET", "/control/tasks/1342", null), 404, "NOT_FOUND", "book_id");
        assertError(send("GET", "/control/tasks/01342", null), 400, "INVALID_PARAMETER", "book_id");
    }

    // --- POST /control/retry-failed ------------------------------------------------------

    @Test
    void retryFailedPutsTheFailedBooksBackInTheirQueue() throws Exception {
        coordination.seed(List.of(84));
        coordination.fail(coordination.claim(Stage.INGESTION, "ingestion-test/w0", SHORT).orElseThrow(),
                ErrorCode.NOT_FOUND, "gone");
        assertEquals(BookState.FAILED, coordination.task(84).orElseThrow().bookState());

        HttpResponse<String> response = send("POST", "/control/retry-failed", null);
        assertEquals(200, response.statusCode());
        assertEquals(json("{\"requeued\": 1}"), json(response.body()));
        assertEquals(BookState.PENDING, coordination.task(84).orElseThrow().bookState());
        assertEquals(json("{\"requeued\": 0}"), json(send("POST", "/control/retry-failed", null).body()));
    }

    // --- Without the cluster -------------------------------------------------------------

    @Test
    void withoutTheClusterControlAnswers503AndStatusStill200() throws Exception {
        CountDownLatch never = new CountDownLatch(1);
        BackgroundConnection connecting = BackgroundConnection.start(() -> {
            try {
                never.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("test over");
        }, c -> { });
        Javalin unconnected = new ControlApi(connecting, Map::of).create("control-module", ServiceServer.MIN_THREADS).start(0);
        try {
            int port = unconnected.port();
            for (String[] route : new String[][]{{"POST", "/control/jobs"}, {"GET", "/control/status"},
                    {"GET", "/control/tasks/1342"}, {"POST", "/control/retry-failed"}}) {
                String body = route[1].equals("/control/jobs") ? "{\"book_ids\": [1342]}" : null;
                assertError(send(port, route[0], route[1], body), 503, "COORDINATION_UNAVAILABLE", null);
            }
            HttpResponse<String> status = send(port, "GET", "/status", null);
            assertEquals(200, status.statusCode());
            assertEquals("connecting", json(status.body()).get("coordination").getAsString());
        } finally {
            unconnected.stop();
            never.countDown();
            connecting.close();
        }
    }

    // --- Helpers -------------------------------------------------------------------------

    private HttpResponse<String> send(String method, String path, String body) throws Exception {
        return send(server.port(), method, path, body);
    }

    private HttpResponse<String> send(int port, String method, String path, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(10))
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .header("Content-Type", "application/json")
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static void assertError(HttpResponse<String> response, int status, String code, String field) {
        assertEquals(status, response.statusCode(), response.body());
        JsonObject error = json(response.body()).getAsJsonObject("error");
        assertEquals(code, error.get("code").getAsString(), response.body());
        if (field == null) {
            assertTrue(error.get("field").isJsonNull(), response.body());
        } else {
            assertEquals(field, error.get("field").getAsString(), response.body());
        }
    }

    private static JsonObject json(String text) {
        return JsonParser.parseString(text).getAsJsonObject();
    }
}
