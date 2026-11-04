package es.ulpgc.bigdata.ingestion.source;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * GutenbergClient against a local HTTP server that answers what each test needs: the tests
 * never touch Project Gutenberg.
 */
class GutenbergClientTest {

    private static final String BOOK = "Title: Red Boat\r\n*** START OF THE PROJECT GUTENBERG EBOOK X ***\r\nÑandú\r\n";

    private HttpServer server;
    private String base;
    /** path -> what to answer */
    private final Map<String, Answer> answers = new ConcurrentHashMap<>();
    private final Map<String, String> userAgents = new ConcurrentHashMap<>();

    private record Answer(int status, String body, String location, long delayMillis) {
        static Answer of(int status, String body) {
            return new Answer(status, body, null, 0);
        }
    }

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        server.start();
        base = "http://127.0.0.1:" + server.getAddress().getPort() + "/cache/epub";
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        userAgents.put(path, String.valueOf(exchange.getRequestHeaders().getFirst("User-Agent")));
        Answer answer = answers.getOrDefault(path, Answer.of(404, "no such book"));
        try {
            Thread.sleep(answer.delayMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (answer.location() != null) {
            exchange.getResponseHeaders().add("Location", answer.location());
        }
        byte[] body = answer.body().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(answer.status(), body.length == 0 ? -1 : body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    private GutenbergClient client() {
        return new GutenbergClient(base, Duration.ofSeconds(2), Duration.ofSeconds(2));
    }

    @Test
    void downloadsTheBookFromTheGutenbergPathUntouched() throws Exception {
        answers.put("/cache/epub/1342/pg1342.txt", Answer.of(200, BOOK));
        assertEquals(BOOK, client().fetch(1342));                   // \r\n and UTF-8 kept
    }

    @Test
    void theDefaultUrlIsTheOneOfStage1() {
        GutenbergClient real = new GutenbergClient(GutenbergClient.DEFAULT_BASE_URL, Duration.ofSeconds(1), Duration.ofSeconds(1));
        assertEquals("https://www.gutenberg.org/cache/epub/1342/pg1342.txt", real.uriOf(1342).toString());
    }

    @Test
    void aTrailingSlashInTheBaseUrlIsIgnored() throws Exception {
        answers.put("/cache/epub/84/pg84.txt", Answer.of(200, BOOK));
        assertEquals(BOOK, new GutenbergClient(base + "/", Duration.ofSeconds(2), Duration.ofSeconds(2)).fetch(84));
    }

    @Test
    void itSaysWhoIsDownloading() throws Exception {
        answers.put("/cache/epub/11/pg11.txt", Answer.of(200, BOOK));
        client().fetch(11);
        assertEquals(GutenbergClient.USER_AGENT, userAgents.get("/cache/epub/11/pg11.txt"));
    }

    @Test
    void notFoundAndGoneArePermanent() {
        answers.put("/cache/epub/7/pg7.txt", Answer.of(410, "gone"));
        BookNotFoundException missing = assertThrows(BookNotFoundException.class, () -> client().fetch(999_999));
        assertEquals(999_999, missing.bookId());
        assertTrue(missing.getMessage().contains("HTTP 404"));
        assertThrows(BookNotFoundException.class, () -> client().fetch(7));
    }

    @Test
    void serverErrorsThrottlingAndForbiddenAreTransient() {
        int id = 1;
        for (int status : new int[]{500, 503, 429, 403}) {
            answers.put("/cache/epub/" + id + "/pg" + id + ".txt", Answer.of(status, "busy"));
            int book = id++;
            SourceUnavailableException e = assertThrows(SourceUnavailableException.class, () -> client().fetch(book));
            assertTrue(e.getMessage().contains("HTTP " + status), e.getMessage());
        }
    }

    @Test
    void aRedirectIsFollowed() throws Exception {
        answers.put("/cache/epub/5/pg5.txt", new Answer(301, "", base + "/mirror/pg5.txt", 0));
        answers.put("/cache/epub/mirror/pg5.txt", Answer.of(200, BOOK));
        assertEquals(BOOK, client().fetch(5));
    }

    @Test
    void aSlowAnswerIsATransientTimeout() {
        answers.put("/cache/epub/98/pg98.txt", new Answer(200, BOOK, null, 3_000));
        GutenbergClient impatient = new GutenbergClient(base, Duration.ofSeconds(2), Duration.ofMillis(300));
        SourceUnavailableException e = assertThrows(SourceUnavailableException.class, () -> impatient.fetch(98));
        assertTrue(e.getMessage().startsWith("timeout"), e.getMessage());
    }

    @Test
    void aServerThatDoesNotAnswerIsTransient() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();                     // free, and nobody listens once closed
        }
        GutenbergClient nowhere = new GutenbergClient("http://127.0.0.1:" + closedPort, Duration.ofSeconds(1), Duration.ofSeconds(1));
        assertThrows(SourceUnavailableException.class, () -> nowhere.fetch(1));
    }

    @Test
    void aMalformedBaseUrlFailsAtStartup() {
        assertThrows(IllegalArgumentException.class,
                () -> new GutenbergClient("http://bad host/", Duration.ofSeconds(1), Duration.ofSeconds(1)));
    }
}
