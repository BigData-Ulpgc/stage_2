package es.ulpgc.bigdata.search.validation;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The APIs on the running stack (plan DP-11, assignment §3.4), over HTTP as a front-end calls
 * them. It needs the stack up with the 15 books of the sample dataset, see http/README.md:
 *
 * <pre>
 *   VALIDATE_URL            http://localhost:8080   nginx; without it, the class is skipped
 *   VALIDATE_INDEXING_URL   http://localhost:7002   docker-compose.validation.yml
 * </pre>
 */
class StackValidationTest {

    private static final String BASE = System.getenv("VALIDATE_URL");
    private static final String INDEXING = System.getenv("VALIDATE_INDEXING_URL");
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    private record Response(int status, String body, HttpResponse<String> raw) {
        JsonObject json() {
            return JsonParser.parseString(body).getAsJsonObject();
        }
    }

    @BeforeAll
    static void needsTheStack() {
        assumeTrue(BASE != null && !BASE.isBlank(), "VALIDATE_URL is not set: see http/README.md");
    }

    private static Response send(String method, String url, String origin) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(20))
                .method(method, HttpRequest.BodyPublishers.noBody());
        if (origin != null) {
            request.header("Origin", origin);
        }
        HttpResponse<String> response = HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
        return new Response(response.statusCode(), response.body(), response);
    }

    private static Response get(String pathAndQuery) throws Exception {
        return send("GET", BASE + pathAndQuery, null);
    }

    private static List<Integer> ids(JsonObject search) {
        List<Integer> ids = new ArrayList<>();
        for (JsonElement book : search.getAsJsonArray("results")) {
            ids.add(book.getAsJsonObject().get("book_id").getAsInt());
        }
        return ids;
    }

    private static void assertSearch(String query, int total, Integer... expectedIds) throws Exception {
        Response response = get("/search?" + query);
        assertEquals(200, response.status(), response.body());
        JsonObject json = response.json();
        assertEquals(total, json.get("total").getAsInt(), query);
        assertEquals(List.of(expectedIds), ids(json), query);
    }

    private static void assertError(Response response, int status, String code, String field) {
        assertEquals(status, response.status(), response.body());
        JsonObject error = response.json().getAsJsonObject("error");
        assertEquals(code, error.get("code").getAsString());
        assertFalse(error.get("message").getAsString().isBlank());
        if (field == null) {
            assertTrue(error.get("field").isJsonNull());
        } else {
            assertEquals(field, error.get("field").getAsString());
        }
    }

    // ---------------------------------------------------------------- keywords and filters

    @Test
    void keywordsAreAndAndTheOrderIsByBookId() throws Exception {
        assertSearch("q=frankenstein", 1, 84);
        assertSearch("q=FRANKENSTEIN", 1, 84);
        assertSearch("q=whale", 4, 76, 84, 345, 2701);
        assertSearch("q=whale+monster", 3, 84, 345, 2701);
    }

    @Test
    void filtersOneByOneAndCombined() throws Exception {
        assertSearch("q=river&author=twain", 2, 74, 76);
        assertSearch("q=river&author=TWAIN", 2, 74, 76);
        assertSearch("q=river&year=2004", 3, 46, 74, 76);
        assertSearch("q=frankenstein&language=en", 1, 84);
        assertSearch("q=river&author=dickens&year=1994", 1, 98);
        assertSearch("q=river&author=dickens&year=1994&language=en", 1, 98);
    }

    @Test
    void anEmptyResultIsA200WithAllTheFields() throws Exception {
        assertSearch("q=xyzzyqqq", 0);
        assertSearch("q=whale+xyzzyqqq", 0);
        assertSearch("q=whale&author=nobody", 0);
        assertSearch("q=whale&language=fr", 0);
    }

    @Test
    void theResponseHasTheFieldsOfTheContract() throws Exception {
        JsonObject json = get("/search?q=frankenstein&author=shelley").json();

        assertEquals(Set.of("query", "filters", "total", "offset", "limit", "index_version", "results"), json.keySet());
        assertEquals("frankenstein", json.get("query").getAsString());
        assertEquals("shelley", json.getAsJsonObject("filters").get("author").getAsString());
        assertEquals(20, json.get("limit").getAsInt());
        assertTrue(json.get("index_version").getAsInt() >= 1);
        JsonObject book = json.getAsJsonArray("results").get(0).getAsJsonObject();
        assertEquals(Set.of("book_id", "title", "author", "language", "year"), book.keySet());
    }

    // ---------------------------------------------------------------- pagination

    @Test
    void thePagesAddUpToAllTheMatchesWithoutRepeatingAny() throws Exception {
        assertSearch("q=river&limit=3", 10, 46, 74, 76);
        assertSearch("q=river&limit=3&offset=3", 10, 84, 98, 174);
        assertSearch("q=river&limit=3&offset=9", 10, 2701);
        assertSearch("q=river&offset=10", 10);

        List<Integer> paged = new ArrayList<>();
        for (int offset = 0; offset < 10; offset += 3) {
            paged.addAll(ids(get("/search?q=river&limit=3&offset=" + offset).json()));
        }
        assertEquals(ids(get("/search?q=river&limit=100").json()), paged);
    }

    // ---------------------------------------------------------------- invalid parameters

    @Test
    void invalidParametersAre400WithTheNameOfTheField() throws Exception {
        assertError(get("/search"), 400, "MISSING_PARAMETER", "q");
        assertError(get("/search?q=the"), 400, "INVALID_PARAMETER", "q");
        assertError(get("/search?q=whale&language=xx"), 400, "INVALID_PARAMETER", "language");
        assertError(get("/search?q=whale&year=99"), 400, "INVALID_PARAMETER", "year");
        assertError(get("/search?q=whale&offset=-1"), 400, "INVALID_PARAMETER", "offset");
        assertError(get("/search?q=whale&limit=0"), 400, "INVALID_PARAMETER", "limit");
        assertError(get("/search?q=whale&limit=101"), 400, "INVALID_PARAMETER", "limit");
    }

    @Test
    void aBookIdThatIsNotCanonicalIs400() throws Exception {
        for (String id : List.of("0", "+84", "0084", "abc", "-1", "1000000000")) {
            assertError(get("/books/" + id), 400, "INVALID_PARAMETER", "book_id");
        }
    }

    // ---------------------------------------------------------------- books and filter values

    @Test
    void aBookHasItsMetadataAndAnotherIsNotPublished() throws Exception {
        JsonObject book = get("/books/1342").json();
        assertEquals("Pride and Prejudice", book.get("title").getAsString());
        assertEquals("Jane Austen", book.get("author").getAsString());
        assertEquals(1998, book.get("year").getAsInt());
        assertEquals("release_date", book.get("year_source").getAsString());

        assertError(get("/books/999999"), 404, "NOT_FOUND", "book_id");
        assertError(get("/nope"), 404, "NOT_FOUND", null);
    }

    @Test
    void theFilterValuesDescribeThePublishedBooks() throws Exception {
        JsonObject json = get("/search/filters").json();

        assertEquals(15, json.get("books").getAsInt());
        JsonArray languages = json.getAsJsonArray("languages");
        assertEquals(1, languages.size());
        assertEquals("en", languages.get(0).getAsJsonObject().get("code").getAsString());
        assertEquals(15, languages.get(0).getAsJsonObject().get("books").getAsInt());
        assertEquals(1975, json.getAsJsonObject("years").get("min").getAsInt());
        assertEquals(2008, json.getAsJsonObject("years").get("max").getAsInt());
    }

    // ---------------------------------------------------------------- deployment

    @Test
    void nginxSaysWhichReplicaAnswered() throws Exception {
        assertTrue(get("/status").raw().headers().firstValue("X-Served-By").isPresent());
    }

    /** Only with several replicas (--scale search=N); with one it would prove nothing. */
    @Test
    void theRepliesOfEveryReplicaAreTheSame() throws Exception {
        Set<String> replicas = new TreeSet<>();
        Set<String> bodies = new TreeSet<>();
        for (int i = 0; i < 12; i++) {
            Response response = get("/search?q=whale");
            replicas.add(response.raw().headers().firstValue("X-Served-By").orElse(""));
            bodies.add(response.body());
        }
        assumeTrue(replicas.size() >= 2, "only one replica answered: " + replicas);
        assertEquals(1, bodies.size(), "the same query gave different answers: " + bodies);
    }

    @Test
    void aBrowserOnAnAllowedOriginGetsTheHeaderAndAnotherDoesNot() throws Exception {
        String allowed = "http://localhost:3000";
        assertEquals(allowed, send("GET", BASE + "/search?q=whale", allowed)
                .raw().headers().firstValue("Access-Control-Allow-Origin").orElse(null));
        assertTrue(send("GET", BASE + "/search?q=whale", "http://evil.example")
                .raw().headers().firstValue("Access-Control-Allow-Origin").isEmpty());
    }

    // ---------------------------------------------------------------- internal: Indexing

    @Test
    void indexingAgreesWithSearchAndValidatesItsParameters() throws Exception {
        assumeTrue(INDEXING != null && !INDEXING.isBlank(), "VALIDATE_INDEXING_URL is not set");

        JsonObject status = send("GET", INDEXING + "/index/status", null).json();
        assertEquals(15, status.get("books_published").getAsInt());
        assertEquals(15, status.get("books_indexed").getAsInt());
        assertEquals(get("/status").json().get("index_version").getAsInt(), status.get("published_version").getAsInt());

        assertError(send("POST", INDEXING + "/index/update/abc", null), 400, "INVALID_PARAMETER", "book_id");
        assertEquals(404, send("POST", INDEXING + "/index/update/999999", null).status());
    }
}
