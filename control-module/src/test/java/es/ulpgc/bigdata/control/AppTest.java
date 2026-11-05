package es.ulpgc.bigdata.control;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hazelcast.core.HazelcastInstance;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.sun.net.httpserver.HttpServer;
import es.ulpgc.bigdata.config.Env;
import es.ulpgc.bigdata.coordination.BookState;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.CoordinationSettings;
import es.ulpgc.bigdata.coordination.Stage;
import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.model.RawBook;
import es.ulpgc.bigdata.testsupport.HazelcastTestCluster;
import es.ulpgc.bigdata.testsupport.MongoTestSupport;
import org.bson.Document;
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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The real App: start order, GET /status while connecting, and the seed, lease check, publication
 * and recovery wired. Each test has its own MongoDB database and DATA_DIR, so the recovery never
 * reads the development data.
 */
class AppTest {

    @TempDir
    Path dir;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final String database = MongoTestSupport.newCollectionName("control_app_test");

    /** The variables of every test: port 0, a database and a DATA_DIR of its own. */
    private Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("CONTROL_PORT", "0");
        env.put("MONGO_URI", MongoTestSupport.URI);
        env.put("MONGO_DATABASE", database);
        env.put("DATA_DIR", dir.resolve("data").toString());
        return env;
    }

    /** The variables that point to the embedded cluster, with leases and checks of 1 s. */
    private Map<String, String> clusterEnv() {
        CoordinationSettings settings = HazelcastTestCluster.settings();
        Map<String, String> env = env();
        env.put("HZ_MEMBERS", String.join(",", settings.members()));
        env.put("HZ_CLUSTER_NAME", settings.clusterName());
        env.put("LEASE_SECONDS", "1");
        env.put("LEASE_CHECK_SECONDS", "1");
        return env;
    }

    /** Connecting blocks until the cluster answers; /status must not (review of #19). */
    @Test
    void statusAnswersAtOnceWhileTheClusterIsUnreachable() throws Exception {
        Map<String, String> env = env();
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
        Map<String, String> env = env();
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
        Map<String, String> env = clusterEnv();
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

    /**
     * CF-10 end to end: the App rebuilds book-state from a real MongoDB and datalake when it
     * starts on an empty cluster, and again when the cluster loses its state while it runs.
     */
    @Test
    void theAppRebuildsALostClusterFromMongoAndTheDatalake() throws Exception {
        assumeTrue(MongoTestSupport.available(), "MongoDB is not available at " + MongoTestSupport.URI);
        // 1342 published in version 1, 84 indexed, 11 only downloaded, 5 only in the seed file
        RangeBasedDatalake datalake = new RangeBasedDatalake(dir.resolve("data").resolve("datalake").resolve("range"));
        datalake.save(new RawBook(84, "header", "body"));
        datalake.save(new RawBook(11, "header", "body"));
        Path seed = Files.writeString(dir.resolve("book_ids.txt"), "1342\n84\n11\n5\n");
        CoordinationSettings settings = HazelcastTestCluster.settings();
        HazelcastTestCluster.reset();
        Map<String, String> env = clusterEnv();
        env.put("CONTROL_SEED_FILE", seed.toString());
        env.put("INDEXING_URL", "http://127.0.0.1:" + freePort());          // nobody: no publication
        HazelcastInstance observer = settings.openClient();
        try (MongoClient mongo = MongoClients.create(MongoTestSupport.URI)) {
            MongoDatabase db = mongo.getDatabase(database);
            db.getCollection("index_meta").insertOne(new Document("_id", "CURRENT").append("version", 1)
                    .append("collection", "inverted_index").append("books", 1));
            db.getCollection("index_books").insertOne(new Document("_id", 1342).append("version", 1));
            db.getCollection("index_log").insertMany(List.of(
                    new Document("_id", 1342).append("indexed_at", new Date()),
                    new Document("_id", 84).append("indexed_at", new Date())));
            try (App.RunningService service = App.start(Env.of(env))) {
                Coordination coordination = Coordination.on(observer, settings, Clock.systemUTC());
                waitFor(coordination::isInitialized, "the first recovery");
                assertStates(coordination);
                assertEquals(1, coordination.queueSize(Stage.INDEXING), "11 waits to be indexed");

                HazelcastTestCluster.reset();                               // both members restart
                waitFor(coordination::isInitialized, "the recovery after the loss");
                assertStates(coordination);
                JsonObject recovery = get(service, "/control/status").getAsJsonObject("recovery");
                assertEquals(2, recovery.get("runs").getAsInt(), recovery.toString());
                assertEquals(1, recovery.get("published").getAsInt(), recovery.toString());
            } finally {
                db.drop();
            }
        } finally {
            observer.shutdown();
        }
    }

    private static void assertStates(Coordination coordination) {
        assertEquals(BookState.PUBLISHED, coordination.task(1342).orElseThrow().bookState());
        assertEquals(BookState.INDEXED, coordination.task(84).orElseThrow().bookState());
        assertEquals(BookState.DOWNLOADED, coordination.task(11).orElseThrow().bookState());
        assertEquals(BookState.PENDING, coordination.task(5).orElseThrow().bookState());
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
