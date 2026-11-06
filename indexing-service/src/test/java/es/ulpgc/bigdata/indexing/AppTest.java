package es.ulpgc.bigdata.indexing;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import es.ulpgc.bigdata.config.DataLayout;
import es.ulpgc.bigdata.config.Env;
import es.ulpgc.bigdata.coordination.BackgroundConnection;
import es.ulpgc.bigdata.coordination.BookState;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.IndexPublished;
import es.ulpgc.bigdata.coordination.Stage;
import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.datamart.indexlog.MongoIndexLog;
import es.ulpgc.bigdata.model.RawBook;
import es.ulpgc.bigdata.testsupport.HazelcastTestCluster;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The whole service as App wires it, started with App.start(Env.of(...)) on a free port:
 * these tests check the wiring itself, not separate pieces.
 */
class AppTest {

    private static final String MONGO_URI = System.getenv().getOrDefault("MONGO_URI", "mongodb://localhost:27017");

    @TempDir Path dataDir;

    private final HttpClient http = HttpClient.newHttpClient();
    private App.RunningService running;

    @AfterEach
    void tearDown() {
        if (running != null) {
            running.close();
        }
    }

    private Map<String, String> baseEnv(String mongoUri) {
        Map<String, String> env = new HashMap<>();
        env.put("INDEXING_PORT", "0");
        env.put("INDEXING_HTTP_THREADS", "8");
        env.put("DATA_DIR", dataDir.toString());
        env.put("MONGO_URI", mongoUri);
        // The embedded test cluster, never the compose cluster of localhost:5701 (DR-08b).
        env.put("HZ_MEMBERS", String.join(",", HazelcastTestCluster.settings().members()));
        env.put("HZ_CLUSTER_NAME", HazelcastTestCluster.settings().clusterName());
        return env;
    }

    @Test
    void arrancaAunqueMongoEsteCaidoYSoloFallaLoQueLoNecesita() throws Exception {
        // Port 1: nobody listens, as if MongoDB were down. No Docker needed.
        running = App.start(Env.of(baseEnv("mongodb://127.0.0.1:1")));

        assertEquals(200, get("/status").statusCode(), "vivo aunque MongoDB no responda (D4)");
        assertEquals(503, get("/index/status").statusCode());
    }

    @Test
    void unaVariableMalFormadaImpideArrancar() {
        Map<String, String> env = baseEnv("mongodb://127.0.0.1:1");
        env.put("INDEXING_PORT", "abc");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> App.start(Env.of(env)));
        assertTrue(e.getMessage().contains("INDEXING_PORT"), e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"INDEXING_THREADS", "INDEX_WRITE_BATCH"})
    void sinHilosDeParseoOSinLoteNoArranca(String variable) {
        Map<String, String> env = baseEnv("mongodb://127.0.0.1:1");
        env.put(variable, "0");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> App.start(Env.of(env)));
        assertTrue(e.getMessage().contains(variable), e.getMessage());
    }

    @Test
    void closeLiberaElPuertoYSePuedeLlamarDosVeces() throws Exception {
        running = App.start(Env.of(baseEnv("mongodb://127.0.0.1:1")));
        int port = running.port();
        assertEquals(200, get("/status").statusCode());

        running.close();
        running.close();                                     // the hook and a test may both call it

        assertThrows(IOException.class, () -> http.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/status")).build(),
                HttpResponse.BodyHandlers.ofString()), "el servidor sigue escuchando");
    }

    @Test
    void deExtremoAExtremoConMongo() throws Exception {
        String database = "search_engine_test_app_" + System.nanoTime();
        try (MongoClient admin = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGO_URI))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(1500, TimeUnit.MILLISECONDS))
                .build())) {
            assumeTrue(answers(admin), "MongoDB no está disponible en " + MONGO_URI + ": test saltado");
            try {
                // A book in the datalake of DATA_DIR, where App will look for it.
                new RangeBasedDatalake(new DataLayout(dataDir).datalakeDir())
                        .save(new RawBook(1342, IndexingServiceTest.HEADER_1342, IndexingServiceTest.BODY_1342));
                Map<String, String> env = baseEnv(MONGO_URI);
                env.put("MONGO_DATABASE", database);
                running = App.start(Env.of(env));

                HttpResponse<String> update = post("/index/update/1342");
                HttpResponse<String> publish = post("/index/publish");
                JsonObject status = JsonParser.parseString(get("/index/status").body()).getAsJsonObject();

                assertEquals(200, update.statusCode(), update.body());
                assertEquals(200, publish.statusCode(), publish.body());
                assertEquals(1, status.get("books_indexed").getAsLong());
                assertEquals(1, status.get("published_version").getAsInt());
                assertEquals(1, status.get("books_published").getAsLong());
                Document current = admin.getDatabase(database).getCollection("index_meta")
                        .find(new Document("_id", "CURRENT")).first();
                assertEquals("inverted_index", current.getString("collection"), "MONGO_COLLECTION (D8)");
                assertNotNull(admin.getDatabase(database).getCollection("books")
                        .find(new Document("_id", 1342)).first(), "falta en books");
                assertNotNull(admin.getDatabase(database).getCollection("inverted_index")
                        .find(new Document("term", "elizabeth")).first(), "falta en inverted_index");
            } finally {
                admin.getDatabase(database).drop();
            }
        }
    }

    @Test
    void alArrancarCompletaUnaPublicacionInterrumpida() throws Exception {
        String database = "search_engine_test_app_" + System.nanoTime();
        try (MongoClient admin = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGO_URI))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(1500, TimeUnit.MILLISECONDS))
                .build())) {
            assumeTrue(answers(admin), "MongoDB no está disponible en " + MONGO_URI + ": test saltado");
            MongoDatabase db = admin.getDatabase(database);
            try {
                // What a publication of version 1 leaves if it dies before switching CURRENT,
                // plus book 6, indexed afterwards and never published.
                new MongoIndexLog(db).markIndexed(List.of(5, 6), Instant.now());
                db.getCollection("index_books").insertOne(new Document("_id", 5).append("version", 1));
                Map<String, String> env = baseEnv(MONGO_URI);
                env.put("MONGO_DATABASE", database);

                running = App.start(Env.of(env));

                // The history entry is written last, after CURRENT: wait for it.
                Document history = null;
                for (int i = 0; i < 100 && history == null; i++) {
                    history = db.getCollection("index_publications").find(new Document("_id", 1)).first();
                    if (history == null) {
                        Thread.sleep(50);
                    }
                }
                assertNotNull(history, "la publicación interrumpida no se completó al arrancar");
                assertEquals(List.of(5), history.getList("books", Integer.class));
                Document current = db.getCollection("index_meta").find(new Document("_id", "CURRENT")).first();
                assertEquals(1, current.getInteger("version"));
                assertEquals(1, current.get("books", Number.class).longValue());
                assertNull(db.getCollection("index_books").find(new Document("_id", 6)).first(),
                        "un libro indexado después espera a un publish explícito");
            } finally {
                db.drop();
            }
        }
    }

    @Test
    void statusDiceConnectingMientrasHazelcastNoResponde() throws Exception {
        Map<String, String> env = baseEnv("mongodb://127.0.0.1:1");
        env.put("HZ_MEMBERS", "127.0.0.1:1");                // nobody listens

        running = App.start(Env.of(env));

        HttpResponse<String> status = get("/status");
        assertEquals(200, status.statusCode());
        JsonObject body = JsonParser.parseString(status.body()).getAsJsonObject();
        assertEquals("connecting", body.get("coordination").getAsString());
        assertTrue(body.get("coordination_failure").isJsonNull());
        assertTrue(body.get("writer").isJsonNull(), "sin cluster no hay escritor");
    }

    @Test
    void siLaConexionFallaStatusDiceFailedYPorQue() throws Exception {
        // No configuration makes the real client fail at once (it retries forever): a connector
        // that fails, as BackgroundConnection would report it.
        BackgroundConnection failed = BackgroundConnection.start(
                () -> { throw new IllegalStateException("configuración de Hazelcast inválida (simulado)"); },
                connected -> { });
        for (int i = 0; i < 100 && failed.state() != BackgroundConnection.State.FAILED; i++) {
            Thread.sleep(20);
        }

        Map<String, Object> status = App.extraStatus(failed, null);

        assertEquals("failed", status.get("coordination"));
        assertEquals("java.lang.IllegalStateException: configuración de Hazelcast inválida (simulado)",
                status.get("coordination_failure"));
        assertNull(status.get("writer"));
    }

    @Test
    void deLaColaAPublishedElEscritorIndexaYLaPublicacionAvisa() throws Exception {
        String database = "search_engine_test_app_" + System.nanoTime();
        try (MongoClient admin = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGO_URI))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(1500, TimeUnit.MILLISECONDS))
                .build())) {
            assumeTrue(answers(admin), "MongoDB no está disponible en " + MONGO_URI + ": test saltado");
            HazelcastTestCluster.start();                     // also when this test runs alone
            HazelcastTestCluster.reset();
            HazelcastInstance hz = HazelcastTestCluster.settings().openClient();
            int clientsBefore = connectedClients();
            try {
                Coordination coordination = Coordination.on(hz, HazelcastTestCluster.settings(), Clock.systemUTC());
                BlockingQueue<IndexPublished> events = new LinkedBlockingQueue<>();
                coordination.subscribe(events::add);
                new RangeBasedDatalake(new DataLayout(dataDir).datalakeDir())
                        .save(new RawBook(1342, IndexingServiceTest.HEADER_1342, IndexingServiceTest.BODY_1342));
                Map<String, String> env = baseEnv(MONGO_URI);
                env.put("MONGO_DATABASE", database);
                running = App.start(Env.of(env));
                awaitCoordination("connected");

                // Book 1342 seeded and downloaded, as the Control and an Ingestion worker do (T1-T3).
                coordination.seed(List.of(1342));
                assertTrue(coordination.complete(coordination.claim(Stage.INGESTION, "test/w1", Duration.ofSeconds(5)).orElseThrow()));

                // The writer of this replica claims it from index-queue and indexes it (T6, T7).
                awaitState(coordination, 1342, BookState.INDEXED);
                JsonObject status = JsonParser.parseString(get("/status").body()).getAsJsonObject();
                assertTrue(status.get("coordination_failure").isJsonNull(), status.toString());
                JsonObject writer = status.getAsJsonObject("writer");
                assertEquals("active", writer.get("role").getAsString());
                assertEquals(1, writer.get("indexed").getAsLong());

                HttpResponse<String> publish = post("/index/publish");

                assertEquals(200, publish.statusCode(), publish.body());
                // The event and T10 leave right after the answer, from the notifier's thread.
                IndexPublished event = events.poll(5, TimeUnit.SECONDS);
                assertNotNull(event, "no llegó IndexPublished");
                JsonObject body = JsonParser.parseString(publish.body()).getAsJsonObject();
                assertEquals(new IndexPublished(1, 1, 1, body.get("published_at").getAsString()), event);
                awaitState(coordination, 1342, BookState.PUBLISHED);

                running.close();
                assertFalse(alive("indexing-service-writer"), "close() volvió con el escritor todavía vivo");
                for (int i = 0; i < 20 && aliveStartingWith("indexing-service-parser-"); i++) {
                    Thread.sleep(50);
                }
                assertFalse(aliveStartingWith("indexing-service-parser-"), "quedan hilos de parseo tras close()");
                // The executor ends its thread just after close() returns: a moment of margin.
                for (int i = 0; i < 20 && alive("indexing-service-notifier"); i++) {
                    Thread.sleep(50);
                }
                assertFalse(alive("indexing-service-notifier"), "el hilo del notifier sigue vivo tras close()");
                for (int i = 0; i < 100 && connectedClients() != clientsBefore; i++) {
                    Thread.sleep(50);
                }
                assertEquals(clientsBefore, connectedClients(), "close() debe cerrar el cliente de Hazelcast");
            } finally {
                running.close();                              // the writer stops before the client
                hz.shutdown();
                admin.getDatabase(database).drop();
            }
        }
    }

    @Test
    void conDosIndexingSoloUnoEscribeEnLotesYAlPararloElOtroTomaElRelevo() throws Exception {
        // Two replicas of the service on the same cluster and the same MongoDB (DR-09, plan §5):
        // the lock index-writer lets only one of them consume index-queue.
        String database = "search_engine_test_two_" + System.nanoTime();
        try (MongoClient admin = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGO_URI))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(1500, TimeUnit.MILLISECONDS))
                .build())) {
            assumeTrue(answers(admin), "MongoDB no está disponible en " + MONGO_URI + ": test saltado");
            HazelcastTestCluster.start();                     // also when this test runs alone
            HazelcastTestCluster.reset();
            HazelcastInstance hz = HazelcastTestCluster.settings().openClient();
            App.RunningService first = null;
            App.RunningService second = null;
            try {
                Coordination coordination = Coordination.on(hz, HazelcastTestCluster.settings(), Clock.systemUTC());
                RangeBasedDatalake datalake = new RangeBasedDatalake(new DataLayout(dataDir).datalakeDir());
                for (int id = 1; id <= 15; id++) {
                    datalake.save(new RawBook(id, IndexingServiceTest.HEADER_1342, IndexingServiceTest.BODY_1342 + " book" + id));
                }
                // 12 books waiting in index-queue BEFORE the replicas start: batches of 5, 5 and 2.
                downloaded(coordination, 1, 12);
                Map<String, String> env = baseEnv(MONGO_URI);
                env.put("MONGO_DATABASE", database);
                env.put("INDEX_WRITE_BATCH", "5");
                env.put("INDEXING_THREADS", "2");
                first = App.start(Env.of(env));
                second = App.start(Env.of(env));

                for (int id = 1; id <= 12; id++) {
                    awaitState(coordination, id, BookState.INDEXED);
                }
                JsonObject writerA = writerOf(first);
                JsonObject writerB = writerOf(second);
                JsonObject active = "active".equals(writerA.get("role").getAsString()) ? writerA : writerB;
                JsonObject standby = active == writerA ? writerB : writerA;
                assertEquals("active", active.get("role").getAsString());
                assertEquals("standby", standby.get("role").getAsString(), "solo una réplica tiene el lock");
                assertEquals(12, active.get("indexed").getAsLong());
                assertEquals(0, standby.get("indexed").getAsLong(), "la otra no escribió nada");
                assertEquals(2, active.get("last_batch").getAsInt(), "lotes de 5, 5 y 2");

                // The active replica stops: the standby takes the lock and indexes what comes next.
                App.RunningService stopped = active == writerA ? first : second;
                App.RunningService other = stopped == first ? second : first;
                stopped.close();
                awaitWriterRole(other, "active");
                downloaded(coordination, 13, 15);
                for (int id = 13; id <= 15; id++) {
                    awaitState(coordination, id, BookState.INDEXED);
                }
                assertEquals(3, writerOf(other).get("indexed").getAsLong());
            } finally {
                if (first != null) {
                    first.close();
                }
                if (second != null) {
                    second.close();
                }
                hz.shutdown();
                admin.getDatabase(database).drop();
            }
        }
    }

    /** Seeds the books from..to and takes them to DOWNLOADED, as the Control and Ingestion do. */
    private static void downloaded(Coordination coordination, int from, int to) throws InterruptedException {
        List<Integer> ids = new ArrayList<>();
        for (int id = from; id <= to; id++) {
            ids.add(id);
        }
        coordination.seed(ids);
        for (int i = 0; i < ids.size(); i++) {
            assertTrue(coordination.complete(
                    coordination.claim(Stage.INGESTION, "test/w1", Duration.ofSeconds(5)).orElseThrow()));
        }
    }

    private JsonObject writerOf(App.RunningService service) throws Exception {
        HttpResponse<String> status = http.send(HttpRequest.newBuilder(
                URI.create("http://localhost:" + service.port() + "/status")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        return JsonParser.parseString(status.body()).getAsJsonObject().getAsJsonObject("writer");
    }

    private void awaitWriterRole(App.RunningService service, String role) throws Exception {
        String now = null;
        for (int i = 0; i < 200 && !role.equals(now); i++) {
            JsonObject writer = writerOf(service);
            now = writer == null ? null : writer.get("role").getAsString();
            if (!role.equals(now)) {
                Thread.sleep(50);
            }
        }
        assertEquals(role, now);
    }

    @Test
    void sinMongoElEscritorArrancaIgualYSinClusterPasaAEspera() throws Exception {
        // A member of its own, which the test stops (stopping the shared one would break the
        // other tests). MongoDB down (port 1): the repair at connect fails, the writer starts anyway.
        String cluster = "indexing-app-" + UUID.randomUUID();
        HazelcastInstance member = HazelcastTestCluster.startStandaloneMember(cluster, 5971);
        try {
            Map<String, String> env = baseEnv("mongodb://127.0.0.1:1");
            env.put("HZ_MEMBERS", "127.0.0.1:" + member.getCluster().getLocalMember().getAddress().getPort());
            env.put("HZ_CLUSTER_NAME", cluster);
            running = App.start(Env.of(env));
            awaitWriterRole("active");

            member.shutdown();

            awaitWriterRole("standby");                       // stops claiming at once (contract §5)
            awaitCoordination("disconnected");

            // The cluster stays down longer than the first wait after a failure (1 s): even
            // then the writer is not stuck in a Hazelcast call, and it stops at once.
            Thread.sleep(2_000);
            long start = System.nanoTime();
            running.close();
            Duration closing = Duration.ofNanos(System.nanoTime() - start);
            assertTrue(closing.compareTo(App.RunningService.WRITER_GRACE.dividedBy(2)) < 0,
                    "close() tardó " + closing.toMillis() + " ms");
        } finally {
            running.close();
            member.shutdown();
        }
    }

    private void awaitWriterRole(String expected) throws Exception {
        String role = null;
        for (int i = 0; i < 200 && !expected.equals(role); i++) {
            JsonObject body = JsonParser.parseString(get("/status").body()).getAsJsonObject();
            role = body.get("writer").isJsonNull() ? null : body.getAsJsonObject("writer").get("role").getAsString();
            if (!expected.equals(role)) {
                Thread.sleep(50);
            }
        }
        assertEquals(expected, role);
    }

    /** Clients connected to the members of this JVM (the test cluster has two). */
    private static int connectedClients() {
        return Hazelcast.getAllHazelcastInstances().stream()
                .mapToInt(m -> m.getClientService().getConnectedClients().size())
                .sum();
    }

    private static boolean aliveStartingWith(String prefix) {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(t -> t.getName().startsWith(prefix) && t.isAlive());
    }

    private static boolean alive(String threadName) {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(t -> t.getName().equals(threadName) && t.isAlive());
    }

    private static void awaitState(Coordination coordination, int bookId, BookState expected) throws Exception {
        BookState state = null;
        for (int i = 0; i < 200 && state != expected; i++) {
            state = coordination.task(bookId).map(t -> t.bookState()).orElse(null);
            if (state != expected) {
                Thread.sleep(50);
            }
        }
        assertEquals(expected, state);
    }

    private void awaitCoordination(String expected) throws Exception {
        String state = null;
        for (int i = 0; i < 100 && !expected.equals(state); i++) {
            state = JsonParser.parseString(get("/status").body()).getAsJsonObject().get("coordination").getAsString();
            if (!expected.equals(state)) {
                Thread.sleep(50);
            }
        }
        assertEquals(expected, state);
    }

    private static boolean answers(MongoClient client) {
        try {
            client.getDatabase("admin").runCommand(new Document("ping", 1));
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(uri(path)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(uri(path)).POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private URI uri(String path) {
        return URI.create("http://localhost:" + running.port() + path);
    }
}
