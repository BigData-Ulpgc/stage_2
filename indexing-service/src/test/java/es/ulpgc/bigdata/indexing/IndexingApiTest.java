package es.ulpgc.bigdata.indexing;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.MongoException;
import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.datamart.index.Tokenizer;
import es.ulpgc.bigdata.datamart.metadata.MetadataParser;
import es.ulpgc.bigdata.model.RawBook;
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

import static org.junit.jupiter.api.Assertions.*;

/**
 * The API over real HTTP: a server on a free port (0), called with java.net.http.HttpClient
 * as the Control Module would. IndexingService uses the in-memory fakes, so no Docker.
 */
class IndexingApiTest {

    @TempDir Path dataDir;

    private final HttpClient http = HttpClient.newHttpClient();
    private RangeBasedDatalake datalake;
    private InMemoryMetadataRepository metadata;
    private IndexingServiceTest.SpyIndex index;
    private Javalin app;

    @BeforeEach
    void setUp() {
        datalake = new RangeBasedDatalake(dataDir.resolve("datalake/range"));
        metadata = new InMemoryMetadataRepository();
        index = new IndexingServiceTest.SpyIndex();
        IndexingService service = new IndexingService(datalake, metadata, index,
                new MetadataParser(), Tokenizer.standard());
        app = new IndexingApi(service).create("indexing-service", 8).start(0);
    }

    @AfterEach
    void tearDown() {
        app.stop();
    }

    // --- POST /index/update/{book_id} ------------------------------------------------

    @Test
    void updateDevuelve200ConElCuerpoDelContrato() throws Exception {
        datalake.save(new RawBook(1342, IndexingServiceTest.HEADER_1342, IndexingServiceTest.BODY_1342));

        HttpResponse<String> response = post("/index/update/1342");

        assertEquals(200, response.statusCode());
        assertTrue(response.headers().firstValue("Content-Type").orElse("").startsWith("application/json"));
        JsonObject body = json(response);
        assertEquals(1342, body.get("book_id").getAsInt());
        assertEquals("updated", body.get("index").getAsString());
        assertEquals(Tokenizer.standard().uniqueTerms(IndexingServiceTest.BODY_1342).size(),
                body.get("terms").getAsInt());
    }

    @ParameterizedTest
    @ValueSource(strings = {"abc", "0", "-5", "1.5", "99999999999"})
    void unBookIdQueNoEsUnEnteroPositivoDa400(String bookId) throws Exception {
        HttpResponse<String> response = post("/index/update/" + bookId);

        assertEquals(400, response.statusCode());
        assertError(response, "INVALID_PARAMETER", "book_id");
        assertEquals(0, index.flushes.get(), "no debe llegar al servicio");
    }

    @Test
    void unLibroQueNoEstaEnElDatalakeDa404() throws Exception {
        HttpResponse<String> response = post("/index/update/999999");

        assertEquals(404, response.statusCode());
        JsonObject error = assertError(response, "NOT_FOUND", "book_id");
        assertEquals("book 999999 is not ready in the datalake", error.get("message").getAsString());
    }

    @Test
    void siLosMetadatosNoRespondenDa503() throws Exception {
        datalake.save(new RawBook(1342, IndexingServiceTest.HEADER_1342, IndexingServiceTest.BODY_1342));
        metadata.failing(true);                               // MetadataRepositoryException

        HttpResponse<String> response = post("/index/update/1342");

        assertEquals(503, response.statusCode());
        assertError(response, "STORAGE_UNAVAILABLE", null);
    }

    @Test
    void siElIndiceNoRespondeDa503() throws Exception {
        datalake.save(new RawBook(1342, IndexingServiceTest.HEADER_1342, IndexingServiceTest.BODY_1342));
        index.failNextFlushWith = new MongoException("MongoDB no responde (simulado)");

        HttpResponse<String> response = post("/index/update/1342");

        assertEquals(503, response.statusCode());
        assertError(response, "STORAGE_UNAVAILABLE", null);
    }

    @Test
    void unFalloInesperadoDa500SinEnsenarElDetalleInterno() throws Exception {
        datalake.save(new RawBook(1342, IndexingServiceTest.HEADER_1342, IndexingServiceTest.BODY_1342));
        index.failNextFlushWith = new IllegalStateException("detalle interno: /data/secreto");

        HttpResponse<String> response = post("/index/update/1342");

        assertEquals(500, response.statusCode());
        JsonObject error = assertError(response, "INTERNAL_ERROR", null);
        assertEquals("internal error", error.get("message").getAsString());
        assertFalse(response.body().contains("secreto"), "el detalle va al log, no al cliente");
    }

    // --- 501 until DR-08 / DR-10 ----------------------------------------------------

    @Test
    void publishResponde501HastaDR08() throws Exception {
        HttpResponse<String> response = post("/index/publish");

        assertEquals(501, response.statusCode());
        JsonObject error = assertError(response, "NOT_IMPLEMENTED", null);
        assertEquals("POST /index/publish is not implemented yet (DR-08)", error.get("message").getAsString());
    }

    @Test
    void rebuildResponde501HastaDR10() throws Exception {
        HttpResponse<String> response = post("/index/rebuild");

        assertEquals(501, response.statusCode());
        JsonObject error = assertError(response, "NOT_IMPLEMENTED", null);
        assertEquals("POST /index/rebuild is not implemented yet (DR-10)", error.get("message").getAsString());
    }

    // --- GET /status (D4) -------------------------------------------------------------

    @Test
    void statusResponde200AunqueMongoNoResponda() throws Exception {
        metadata.failing(true);
        index.failNextFlushWith = new MongoException("MongoDB no responde (simulado)");

        HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(uri("/status")).GET().build(), HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        JsonObject body = json(response);
        assertEquals("indexing-service", body.get("service").getAsString());
        assertEquals("running", body.get("status").getAsString());
    }

    // --- Helpers -----------------------------------------------------------------------

    private URI uri(String path) {
        return URI.create("http://localhost:" + app.port() + path);
    }

    private HttpResponse<String> post(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(uri(path))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static JsonObject json(HttpResponse<String> response) {
        return JsonParser.parseString(response.body()).getAsJsonObject();
    }

    /** The common error format: {"error": {code, message, field}}; returns the inner object. */
    private static JsonObject assertError(HttpResponse<String> response, String code, String field) {
        assertTrue(response.headers().firstValue("Content-Type").orElse("").startsWith("application/json"));
        JsonObject error = json(response).getAsJsonObject("error");
        assertNotNull(error, "falta el objeto error: " + response.body());
        assertEquals(code, error.get("code").getAsString());
        assertFalse(error.get("message").getAsString().isBlank());
        if (field == null) {
            assertTrue(error.get("field").isJsonNull(), "field debe ser null explícito");
        } else {
            assertEquals(field, error.get("field").getAsString());
        }
        return error;
    }
}
