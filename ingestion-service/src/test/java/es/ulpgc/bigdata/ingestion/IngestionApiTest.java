package es.ulpgc.bigdata.ingestion;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import io.javalin.Javalin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The API over real HTTP: a server on a free port (0), called with java.net.http.HttpClient as
 * the Control Module would. The source is a fake, so no network.
 */
class IngestionApiTest {

    @TempDir
    Path dataDir;

    private final HttpClient http = HttpClient.newHttpClient();
    private final FakeSource source = new FakeSource();
    private Javalin app;

    @BeforeEach
    void setUp() {
        IngestionService service = new IngestionService(source, new BookSplitter(),
                new RangeBasedDatalake(dataDir.resolve("datalake/range")));
        app = new IngestionApi(service, dataDir).create("ingestion-service", 8).start(0);
    }

    @AfterEach
    void tearDown() {
        app.stop();
    }

    // --- POST /ingest/{book_id} -------------------------------------------------------

    @Test
    void ingestingDownloadsAndAnswersTheAssignmentFormat() throws Exception {
        source.book(1342);
        HttpResponse<String> response = post("/ingest/1342");
        assertEquals(200, response.statusCode());
        JsonObject body = json(response);
        assertEquals(1342, body.get("book_id").getAsInt());
        assertEquals("downloaded", body.get("status").getAsString());
        assertEquals("datalake/range/01000-01999/1342", body.get("path").getAsString());
    }

    @Test
    void ingestingTwiceIsAlreadyAvailableWithoutDownloading() throws Exception {
        source.book(84);
        post("/ingest/84");
        JsonObject again = json(post("/ingest/84"));
        assertEquals("already_available", again.get("status").getAsString());
        assertEquals("datalake/range/00000-00999/84", again.get("path").getAsString());
        assertEquals(1, source.fetches.get());
    }

    @Test
    void aBookTheSourceDoesNotHaveIs404() throws Exception {
        assertError(post("/ingest/999999"), 404, "NOT_FOUND", "book_id");
    }

    @Test
    void aBookWithoutMarkersIs422AndIsNotAvailable() throws Exception {
        source.text(8, "no markers here");
        assertError(post("/ingest/8"), 422, "INVALID_BOOK", "book_id");
        assertEquals("not_available", json(get("/ingest/status/8")).get("status").getAsString());
    }

    @Test
    void aFailingSourceIs502AndATimeoutIs504() throws Exception {
        source.unavailable(9).timeout(10);
        assertError(post("/ingest/9"), 502, "SOURCE_UNAVAILABLE", null);
        assertError(post("/ingest/10"), 504, "SOURCE_TIMEOUT", null);
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "+84", "0084", "abc", "1000000000", "%D9%A8%D9%A4"})
    void aBookIdThatIsNotCanonicalIs400(String id) throws Exception {
        assertError(post("/ingest/" + id), 400, "INVALID_PARAMETER", "book_id");
        assertError(get("/ingest/status/" + id), 400, "INVALID_PARAMETER", "book_id");
        assertEquals(0, source.fetches.get());                      // rejected before the source
    }

    // --- GET /ingest/status/{book_id} and GET /ingest/list --------------------------------

    @Test
    void statusSaysWhetherTheBookIsInTheDatalake() throws Exception {
        source.book(11);
        JsonObject before = json(get("/ingest/status/11"));
        assertEquals("not_available", before.get("status").getAsString());
        assertTrue(before.get("path").isJsonNull());
        post("/ingest/11");
        JsonObject after = json(get("/ingest/status/11"));
        assertEquals(11, after.get("book_id").getAsInt());
        assertEquals("available", after.get("status").getAsString());
        assertEquals("datalake/range/00000-00999/11", after.get("path").getAsString());
    }

    @Test
    void theListHasCountAndSortedIds() throws Exception {
        assertEquals(0, json(get("/ingest/list")).get("count").getAsInt());
        source.book(2701).book(5).book(1342);
        post("/ingest/2701");
        post("/ingest/5");
        post("/ingest/1342");
        JsonObject list = json(get("/ingest/list"));
        assertEquals(3, list.get("count").getAsInt());
        assertEquals("[5,1342,2701]", list.get("books").toString());
    }

    // --- Operational and unknown routes --------------------------------------------------

    @Test
    void statusOfTheServiceNamesTheSource() throws Exception {
        JsonObject status = json(get("/status"));
        assertEquals("ingestion-service", status.get("service").getAsString());
        assertEquals("fake", status.get("source").getAsString());
    }

    @Test
    void anUnknownRouteOrMethodIs404InTheCommonFormat() throws Exception {
        assertError(get("/ingest/1342"), 404, "NOT_FOUND", null);      // GET where only POST exists
        assertError(get("/ingest/nope/1"), 404, "NOT_FOUND", null);
    }

    // --- Helpers -------------------------------------------------------------------------

    private void assertError(HttpResponse<String> response, int status, String code, String field) {
        assertEquals(status, response.statusCode(), response.body());
        JsonObject error = json(response).getAsJsonObject("error");
        assertEquals(code, error.get("code").getAsString());
        if (field == null) {
            assertTrue(error.get("field").isJsonNull());
        } else {
            assertEquals(field, error.get("field").getAsString());
        }
    }

    private static JsonObject json(HttpResponse<String> response) {
        return JsonParser.parseString(response.body()).getAsJsonObject();
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(uri(path)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(uri(path)).POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private URI uri(String path) {
        return URI.create("http://localhost:" + app.port() + path);
    }
}
