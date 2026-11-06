package es.ulpgc.bigdata.indexing;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import es.ulpgc.bigdata.config.DataLayout;
import es.ulpgc.bigdata.config.Env;
import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.datamart.indexlog.MongoIndexLog;
import es.ulpgc.bigdata.model.RawBook;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
