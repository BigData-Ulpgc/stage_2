package es.ulpgc.bigdata.control;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** POST /index/publish against a local server that plays Indexing. */
class IndexingClientTest {

    private HttpServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    /** A server whose /index/publish answers {@code status} and {@code body}, after {@code bodyDelay}. */
    private URI indexing(int status, String body, Duration bodyDelay, AtomicReference<String> method) throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/index/publish", exchange -> {
            if (method != null) {
                method.set(exchange.getRequestMethod());
            }
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes, 0, 1);                     // the headers and one byte at once...
                out.flush();
                Thread.sleep(bodyDelay.toMillis());         // ...the rest later
                out.write(bytes, 1, bytes.length - 1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        server.start();
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    @Test
    void aPublicationIsParsed() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        URI url = indexing(200, "{\"version\": 4, \"books_published\": 120, \"total_books\": 950,"
                + " \"published_at\": \"2026-11-04T10:00:30Z\"}", Duration.ZERO, method);
        IndexingClient.PublishOutcome outcome = new IndexingClient(url).publish();
        assertEquals(new IndexingClient.PublishOutcome(4, 120, 950, "2026-11-04T10:00:30Z"), outcome);
        assertEquals("POST", method.get());
    }

    @Test
    void nothingEverPublishedHasNoVersion() throws Exception {
        URI url = indexing(200, "{\"version\": null, \"books_published\": 0, \"total_books\": 0,"
                + " \"published_at\": null}", Duration.ZERO, null);
        IndexingClient.PublishOutcome outcome = new IndexingClient(url).publish();
        assertNull(outcome.version());
        assertNull(outcome.publishedAt());
        assertEquals(0, outcome.booksPublished());
    }

    @Test
    void aRebuildOrAStorageErrorIsAFailureToRetry() throws Exception {
        URI url = indexing(409, "{\"error\": {\"code\": \"REBUILD_IN_PROGRESS\"}}", Duration.ZERO, null);
        IndexingClient.PublishFailedException e = assertThrows(IndexingClient.PublishFailedException.class,
                () -> new IndexingClient(url).publish());
        assertTrue(e.getMessage().contains("HTTP 409"), e.getMessage());
        stop();

        URI down = indexing(503, "{\"error\": {\"code\": \"STORAGE_UNAVAILABLE\"}}", Duration.ZERO, null);
        assertThrows(IndexingClient.PublishFailedException.class, () -> new IndexingClient(down).publish());
    }

    @Test
    void anAnswerWithoutItsFieldsIsAFailure() throws Exception {
        URI url = indexing(200, "{\"version\": 4}", Duration.ZERO, null);
        assertThrows(IndexingClient.PublishFailedException.class, () -> new IndexingClient(url).publish());
        stop();
        URI html = indexing(200, "<html>proxy</html>", Duration.ZERO, null);
        assertThrows(IndexingClient.PublishFailedException.class, () -> new IndexingClient(html).publish());
    }

    @Test
    void nobodyListeningIsAFailure() throws Exception {
        int nobody;
        try (ServerSocket socket = new ServerSocket(0)) {
            nobody = socket.getLocalPort();
        }
        assertThrows(IndexingClient.PublishFailedException.class,
                () -> new IndexingClient(URI.create("http://127.0.0.1:" + nobody)).publish());
    }

    /** The timeout covers the whole answer, not only its headers (see GutenbergClient). */
    @Test
    void aSlowBodyTimesOut() throws Exception {
        URI url = indexing(200, "{\"version\": 4, \"books_published\": 1, \"total_books\": 1}", Duration.ofSeconds(3), null);
        long start = System.nanoTime();
        IndexingClient.PublishFailedException e = assertThrows(IndexingClient.PublishFailedException.class,
                () -> new IndexingClient(url, Duration.ofMillis(500)).publish());
        long millis = Duration.ofNanos(System.nanoTime() - start).toMillis();
        assertTrue(millis < 2_000, "gave up after " + millis + " ms");
        assertTrue(e.getMessage().startsWith("no answer"), e.getMessage());
    }
}
