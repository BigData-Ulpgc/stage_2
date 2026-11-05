package es.ulpgc.bigdata.control;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hazelcast.core.HazelcastInstance;
import com.sun.net.httpserver.HttpServer;
import es.ulpgc.bigdata.config.Env;
import es.ulpgc.bigdata.coordination.BookState;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.CoordinationSettings;
import es.ulpgc.bigdata.coordination.Stage;
import es.ulpgc.bigdata.testsupport.HazelcastTestCluster;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The real App: start order, GET /status while connecting, and the seed, lease check and publication wired. */
class AppTest {

    @TempDir
    Path dir;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    /** Connecting blocks until the cluster answers; /status must not (review of #19). */
    @Test
    void statusAnswersAtOnceWhileTheClusterIsUnreachable() throws Exception {
        Map<String, String> env = new HashMap<>();
        env.put("CONTROL_PORT", "0");
        env.put("HZ_MEMBERS", "127.0.0.1:" + freePort());
        long start = System.nanoTime();
        try (App.RunningService service = App.start(Env.of(env))) {
            JsonObject status = get(service, "/status");
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertEquals("connecting", status.get("coordination").getAsString());
            assertTrue(millis < 5_000, "start + /status took " + millis + " ms");
        }
    }

    @Test
    void aSeedFileThatCannotBeReadStopsTheStart() {
        Map<String, String> env = new HashMap<>();
        env.put("CONTROL_PORT", "0");
        env.put("CONTROL_SEED_FILE", dir.resolve("missing.txt").toString());
        assertThrows(UncheckedIOException.class, () -> App.start(Env.of(env)));
    }

    /**
     * Seeded from CONTROL_SEED_FILE, a dead worker's book requeued by the lease check, and a
     * publication once a book is INDEXED.
     */
    @Test
    void theAppSeedsChecksLeasesAndPublishes() throws Exception {
        Path seed = Files.writeString(dir.resolve("book_ids.txt"), "# two books\n1342\n84\n");
        AtomicInteger publications = new AtomicInteger();
        HttpServer indexing = fakeIndexing(publications);
        CoordinationSettings settings = HazelcastTestCluster.settings();
        HazelcastTestCluster.reset();
        Map<String, String> env = new HashMap<>();
        env.put("CONTROL_PORT", "0");
        env.put("HZ_MEMBERS", String.join(",", settings.members()));
        env.put("HZ_CLUSTER_NAME", settings.clusterName());
        env.put("LEASE_SECONDS", "1");
        env.put("LEASE_CHECK_SECONDS", "1");
        env.put("PUBLISH_BATCH", "1");
        env.put("INDEXING_URL", "http://127.0.0.1:" + indexing.getAddress().getPort() + "/");
        env.put("CONTROL_SEED_FILE", seed.toString());
        HazelcastInstance workers = settings.openClient();                    // plays Ingestion and Indexing
        try (App.RunningService service = App.start(Env.of(env))) {
            Coordination coordination = Coordination.on(workers,
                    new CoordinationSettings(settings.members(), settings.clusterName(), 1, 3, 1), Clock.systemUTC());
            waitFor(() -> coordination.count(BookState.PENDING) == 2, "the seed");
            assertEquals("connected", get(service, "/status").get("coordination").getAsString());

            // A worker claims 1342 and dies: its lease (1 s) expires and the Control requeues it
            int claimed = coordination.claim(Stage.INGESTION, "ingestion-dead/w0", Duration.ofSeconds(1)).orElseThrow().bookId();
            waitFor(() -> coordination.task(claimed).orElseThrow().bookState() == BookState.PENDING
                    && coordination.task(claimed).orElseThrow().attempts() == 1, "the lease check");

            // Another one takes a book through both stages: with PUBLISH_BATCH=1 it is published
            coordination.complete(coordination.claim(Stage.INGESTION, "ingestion-ok/w0", Duration.ofSeconds(1)).orElseThrow());
            coordination.complete(coordination.claim(Stage.INDEXING, "indexing-ok/w0", Duration.ofSeconds(1)).orElseThrow());
            waitFor(() -> publications.get() > 0, "the publication");

            JsonObject status = get(service, "/control/status");
            assertTrue(status.getAsJsonObject("publication").get("publications").getAsInt() > 0, status.toString());
            assertEquals(1, status.getAsJsonObject("lease_check").get("requeued_or_failed").getAsInt(), status.toString());
        } finally {
            workers.shutdown();
            indexing.stop(0);
        }
    }

    // --- Helpers -------------------------------------------------------------------------

    /** Indexing's POST /index/publish, counting the calls. */
    private static HttpServer fakeIndexing(AtomicInteger publications) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/index/publish", exchange -> {
            int version = publications.incrementAndGet();
            byte[] body = ("{\"version\": " + version + ", \"books_published\": 1, \"total_books\": " + version
                    + ", \"published_at\": \"2026-11-04T10:00:30Z\"}").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.start();
        return server;
    }

    private static void waitFor(BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.sleep(50);
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private JsonObject get(App.RunningService service, String path) throws Exception {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(
                        URI.create("http://localhost:" + service.port() + path)).timeout(Duration.ofSeconds(5)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), response.body());
        return JsonParser.parseString(response.body()).getAsJsonObject();
    }
}
