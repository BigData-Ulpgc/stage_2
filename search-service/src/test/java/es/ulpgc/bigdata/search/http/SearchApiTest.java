package es.ulpgc.bigdata.search.http;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import es.ulpgc.bigdata.datamart.index.Tokenizer;
import es.ulpgc.bigdata.datamart.metadata.MongoMetadataRepository;
import es.ulpgc.bigdata.http.ServiceServer;
import es.ulpgc.bigdata.search.fixture.TestBooks;
import es.ulpgc.bigdata.search.index.IndexSnapshotLoader;
import es.ulpgc.bigdata.search.index.MongoPostingSource;
import es.ulpgc.bigdata.search.index.PostingSource;
import es.ulpgc.bigdata.search.index.SnapshotHolder;
import es.ulpgc.bigdata.search.query.SearchEngine;
import es.ulpgc.bigdata.search.testsupport.MongoTestDb;
import io.javalin.Javalin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * GET /search over real HTTP, as the front-end calls it. The 400 and INDEX_NOT_READY cases
 * need no MongoDB; the rest run on the TestBooks fixture (skipped without MongoDB).
 */
class SearchApiTest {

    private final HttpClient http = HttpClient.newHttpClient();
    private MongoClient client;
    private String dbName;
    private Javalin app;

    @AfterEach
    void tearDown() {
        if (app != null) {
            app.stop();
        }
        if (client != null) {
            if (dbName != null) {
                client.getDatabase(dbName).drop();
            }
            client.close();
        }
    }

    /** A server whose MongoDB is never reached: nothing loaded, so no snapshot. */
    private void startWithoutIndex() {
        client = MongoClients.create("mongodb://localhost:1");   // lazy: never connects in these tests
        MongoDatabase db = client.getDatabase("unused");
        start(db, new MongoPostingSource(db), false);
    }

    /** A server on the TestBooks fixture, snapshot loaded. */
    private void startWithFixture(PostingSource postings) {
        assumeTrue(MongoTestDb.available(), "MongoDB is not running: start it with docker compose up -d mongo");
        dbName = MongoTestDb.newDatabaseName("api");
        TestBooks.load(MongoTestDb.URI, dbName);
        client = MongoClients.create(MongoTestDb.URI);
        MongoDatabase db = client.getDatabase(dbName);
        start(db, postings == null ? new MongoPostingSource(db) : postings, true);
    }

    private void start(MongoDatabase db, PostingSource postings, boolean load) {
        SnapshotHolder holder = new SnapshotHolder(new IndexSnapshotLoader(db, new MongoMetadataRepository(db)));
        if (load) {
            holder.reload();
        }
        app = new SearchApi(holder, new SearchEngine(postings), Tokenizer.standard())
                .create("search-service", ServiceServer.DEFAULT_MAX_THREADS).start(0);
    }

    private HttpResponse<String> get(String pathAndQuery) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + app.port() + pathAndQuery)).GET().build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static JsonObject json(HttpResponse<String> response) {
        return JsonParser.parseString(response.body()).getAsJsonObject();
    }

    private static List<Integer> ids(JsonObject body) {
        List<Integer> ids = new ArrayList<>();
        for (var result : body.getAsJsonArray("results")) {
            ids.add(result.getAsJsonObject().get("book_id").getAsInt());
        }
        return ids;
    }

    private static void assertError(HttpResponse<String> response, int status, String code, String field) {
        assertEquals(status, response.statusCode(), response.body());
        JsonObject error = json(response).getAsJsonObject("error");
        assertEquals(code, error.get("code").getAsString());
        if (field == null) {
            assertTrue(error.get("field").isJsonNull());
        } else {
            assertEquals(field, error.get("field").getAsString());
        }
        assertTrue(error.has("message"));
    }

    // ---------------------------------------------------------------- without MongoDB

    @Test
    void invalidParametersAre400WithTheCommonErrorFormat() throws Exception {
        startWithoutIndex();

        assertError(get("/search"), 400, "MISSING_PARAMETER", "q");
        assertError(get("/search?q=the+of"), 400, "INVALID_PARAMETER", "q");
        assertError(get("/search?q=river&limit=500"), 400, "INVALID_PARAMETER", "limit");
        assertError(get("/search?q=river&language=EN"), 400, "INVALID_PARAMETER", "language");
        assertError(get("/search?q=river&year=nineteen"), 400, "INVALID_PARAMETER", "year");
    }

    @Test
    void beforeAnyPublicationAValidSearchIs503IndexNotReady() throws Exception {
        startWithoutIndex();

        assertError(get("/search?q=river"), 503, "INDEX_NOT_READY", null);
    }

    // ---------------------------------------------------------------- with the fixture

    @Test
    void keywordSearchReturnsOnlyPublishedBooksSortedById() throws Exception {
        startWithFixture(null);

        HttpResponse<String> response = get("/search?q=adventure");
        JsonObject body = json(response);

        assertEquals(200, response.statusCode());
        assertTrue(response.headers().firstValue("Content-Type").orElse("").startsWith("application/json"));
        assertEquals(List.of(74, 76, 67979), ids(body));          // not 11 (version 3) nor 2701 (unpublished)
        assertEquals(3, body.get("total").getAsInt());
        assertEquals(TestBooks.CURRENT_VERSION, body.get("index_version").getAsInt());
        assertEquals("adventure", body.get("query").getAsString());
        assertEquals(0, body.getAsJsonObject("filters").size());
    }

    @Test
    void theResultsHaveTheBookSummaryFields() throws Exception {
        startWithFixture(null);

        JsonObject pride = json(get("/search?q=pride+prejudice")).getAsJsonArray("results").get(0).getAsJsonObject();

        assertEquals(1342, pride.get("book_id").getAsInt());
        assertEquals("Pride and Prejudice", pride.get("title").getAsString());
        assertEquals("Jane Austen", pride.get("author").getAsString());
        assertEquals("en", pride.get("language").getAsString());
        assertEquals(1998, pride.get("year").getAsInt());
        assertEquals(5, pride.size());
    }

    @Test
    void filtersAloneAndCombined() throws Exception {
        startWithFixture(null);

        assertEquals(List.of(74, 76), ids(json(get("/search?q=river&author=twain"))));
        assertEquals(List.of(17489), ids(json(get("/search?q=aventure&language=fr"))));
        assertEquals(List.of(67979), ids(json(get("/search?q=adventure&year=1926"))));
        JsonObject combined = json(get("/search?q=london&author=DICKENS&year=2004&language=en"));
        assertEquals(List.of(46), ids(combined));

        JsonObject filters = combined.getAsJsonObject("filters");
        assertEquals("DICKENS", filters.get("author").getAsString());   // echoed as received
        assertEquals(2004, filters.get("year").getAsInt());              // a number, as in the schema
    }

    @Test
    void paginationKeepsTheTotal() throws Exception {
        startWithFixture(null);

        JsonObject page = json(get("/search?q=river&author=twain&offset=1&limit=1"));

        assertEquals(2, page.get("total").getAsInt());
        assertEquals(1, page.get("offset").getAsInt());
        assertEquals(1, page.get("limit").getAsInt());
        assertEquals(List.of(76), ids(page));
    }

    @Test
    void aValidSearchWithNoMatchesIs200WithAnEmptyList() throws Exception {
        startWithFixture(null);

        HttpResponse<String> response = get("/search?q=zzzzqqq");
        JsonObject body = json(response);

        assertEquals(200, response.statusCode());
        assertEquals(0, body.get("total").getAsInt());
        assertEquals(new JsonArray(), body.getAsJsonArray("results"));
    }

    // ---------------------------------------------------------------- GET /books/{book_id} (DP-05)

    @Test
    void bookDetailHasTheYearSourceAndTheVersion() throws Exception {
        startWithFixture(null);

        HttpResponse<String> response = get("/books/67979");
        JsonObject castle = json(response);

        assertEquals(200, response.statusCode());
        assertEquals(67979, castle.get("book_id").getAsInt());
        assertEquals("The Blue Castle: a novel", castle.get("title").getAsString());
        assertEquals(1926, castle.get("year").getAsInt());
        assertEquals("original_publication", castle.get("year_source").getAsString());
        assertEquals("April 5, 2022", castle.get("release_date").getAsString());
        assertEquals(TestBooks.CURRENT_VERSION, castle.get("index_version").getAsInt());
    }

    @Test
    void aBookThatIsNotPublishedIs404EvenIfIndexed() throws Exception {
        startWithFixture(null);

        assertError(get("/books/2701"), 404, "NOT_FOUND", "book_id");     // indexed, never published
        assertError(get("/books/11"), 404, "NOT_FOUND", "book_id");       // version 3 > CURRENT
        assertError(get("/books/999999"), 404, "NOT_FOUND", "book_id");   // does not exist
    }

    @Test
    void aBookIdThatIsNotANonNegativeIntegerIs400() throws Exception {
        startWithoutIndex();

        assertError(get("/books/abc"), 400, "INVALID_PARAMETER", "book_id");
        assertError(get("/books/-1"), 400, "INVALID_PARAMETER", "book_id");
        assertError(get("/books/99999999999"), 400, "INVALID_PARAMETER", "book_id");
        assertError(get("/books/1342"), 503, "INDEX_NOT_READY", null);
    }

    // ---------------------------------------------------------------- GET /search/filters (DP-05)

    @Test
    void filtersListTheLanguagesAndYearsOfThePublishedBooks() throws Exception {
        startWithFixture(null);

        JsonObject filters = json(get("/search/filters"));

        assertEquals(7, filters.get("books").getAsInt());
        JsonArray languages = filters.getAsJsonArray("languages");
        assertEquals(2, languages.size());
        assertEquals("en", languages.get(0).getAsJsonObject().get("code").getAsString());
        assertEquals(6, languages.get(0).getAsJsonObject().get("books").getAsInt());
        assertEquals("fr", languages.get(1).getAsJsonObject().get("code").getAsString());
        assertEquals(1926, filters.getAsJsonObject("years").get("min").getAsInt());   // The Blue Castle
        assertEquals(2006, filters.getAsJsonObject("years").get("max").getAsInt());   // Les misérables
        assertEquals(TestBooks.CURRENT_VERSION, filters.get("index_version").getAsInt());
    }

    @Test
    void filtersBeforeAnyPublicationAre503() throws Exception {
        startWithoutIndex();

        assertError(get("/search/filters"), 503, "INDEX_NOT_READY", null);
    }

    // ---------------------------------------------------------------- MongoDB down

    @Test
    void mongoNotAnsweringIs503StorageUnavailable() throws Exception {
        startWithFixture((collection, terms) -> {
            throw new MongoTimeoutException("timed out after 3000 ms");
        });

        assertError(get("/search?q=river"), 503, "STORAGE_UNAVAILABLE", null);
    }
}
