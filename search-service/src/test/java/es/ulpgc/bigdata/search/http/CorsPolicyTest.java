package es.ulpgc.bigdata.search.http;

import es.ulpgc.bigdata.http.ApiException;
import es.ulpgc.bigdata.http.ServiceServer;
import io.javalin.Javalin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** What a browser sees, over real HTTP. */
class CorsPolicyTest {

    private static final String ALLOW_ORIGIN = "Access-Control-Allow-Origin";
    private static final String FRONT_END = "http://localhost:3000";

    private final HttpClient http = HttpClient.newHttpClient();
    private Javalin app;

    @AfterEach
    void tearDown() {
        if (app != null) {
            app.stop();
        }
    }

    private void start(String corsOrigins) {
        app = ServiceServer.create("cors-test", ServiceServer.MIN_THREADS, Map::of, CorsPolicy.parse(corsOrigins)::applyTo);
        app.get("/ok", ctx -> ctx.result("{}"));
        app.get("/bad", ctx -> {
            throw ApiException.invalid("q", "bad");
        });
        app.start(0);
    }

    private HttpResponse<String> send(String method, String path, String origin) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://localhost:" + app.port() + path))
                .method(method, HttpRequest.BodyPublishers.noBody())
                .header("Origin", origin);
        if (method.equals("OPTIONS")) {
            request.header("Access-Control-Request-Method", "GET");
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static Optional<String> allowOrigin(HttpResponse<String> response) {
        return response.headers().firstValue(ALLOW_ORIGIN);
    }

    @Test
    void anAllowedOriginGetsTheHeader() throws Exception {
        start(FRONT_END + ",https://example.org");

        assertEquals(Optional.of(FRONT_END), allowOrigin(send("GET", "/ok", FRONT_END)));
        assertEquals(Optional.of("https://example.org"), allowOrigin(send("GET", "/ok", "https://example.org")));
    }

    @Test
    void anOriginNotInTheListDoesNotGetIt() throws Exception {
        start(FRONT_END);

        assertTrue(allowOrigin(send("GET", "/ok", "http://evil.example")).isEmpty());
    }

    @Test
    void theBrowserPreflightIsAnswered() throws Exception {
        start(FRONT_END);

        HttpResponse<String> response = send("OPTIONS", "/ok", FRONT_END);

        assertEquals(200, response.statusCode());
        assertEquals(Optional.of(FRONT_END), allowOrigin(response));
    }

    @Test
    void errorsAlsoCarryTheHeaderOrTheBrowserWouldHideThem() throws Exception {
        start(FRONT_END);

        HttpResponse<String> response = send("GET", "/bad", FRONT_END);

        assertEquals(400, response.statusCode());
        assertEquals(Optional.of(FRONT_END), allowOrigin(response));
    }

    @Test
    void anyOriginWithTheWildcard() throws Exception {
        start("*");

        assertEquals(Optional.of("*"), allowOrigin(send("GET", "/ok", "http://anything.example")));
    }

    @Test
    void withoutTheVariableThereIsNoCors() throws Exception {
        start(null);

        assertTrue(allowOrigin(send("GET", "/ok", FRONT_END)).isEmpty());
    }

    @Test
    void parsingToleratesSpacesAndBlank() {
        assertEquals(List.of("http://a", "http://b:8080"), CorsPolicy.parse(" http://a , http://b:8080 ").origins());
        assertEquals(CorsPolicy.NONE, CorsPolicy.parse("   "));
    }

    @Test
    void anInvalidValueStopsTheStartNamingTheVariable() {
        for (String invalid : List.of("localhost:3000", "http://a/", "http://a,,http://b", "*,http://a", "ftp://a")) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> CorsPolicy.parse(invalid), invalid);
            assertTrue(e.getMessage().contains(CorsPolicy.VARIABLE), e.getMessage());
        }
    }
}
