package es.ulpgc.bigdata.indexing;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hazelcast.core.HazelcastInstance;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.datamart.index.Tokenizer;
import es.ulpgc.bigdata.datamart.metadata.MetadataParser;
import es.ulpgc.bigdata.model.RawBook;
import es.ulpgc.bigdata.testsupport.HazelcastTestCluster;
import io.javalin.Javalin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * POST /index/rebuild and the 409s of DR-10 over real HTTP, with the embedded Hazelcast
 * cluster (the marker is real) and the in-memory stores.
 */
class IndexingApiRebuildTest {

    @TempDir Path dataDir;

    private static HazelcastInstance client;

    private final HttpClient http = HttpClient.newHttpClient();
    private final Map<String, IndexingServiceTest.SpyIndex> collections = new ConcurrentHashMap<>();
    private Coordination coordination;
    private RangeBasedDatalake datalake;
    private Rebuilder rebuilder;
    private Javalin app;

    @BeforeAll
    static void connect() {
        client = HazelcastTestCluster.settings().openClient();
    }

    @AfterAll
    static void disconnect() {
        client.shutdown();
    }

    @BeforeEach
    void setUp() {
        HazelcastTestCluster.reset();
        coordination = Coordination.on(client, HazelcastTestCluster.settings(), Clock.systemUTC());
        datalake = new RangeBasedDatalake(dataDir.resolve("datalake/range"));
        InMemoryIndexLog indexLog = new InMemoryIndexLog();
        InMemoryPublicationRepository publications = new InMemoryPublicationRepository(indexLog);
        IndexingService service = new IndexingService(datalake, new InMemoryMetadataRepository(),
                name -> collections.computeIfAbsent(name, n -> new IndexingServiceTest.SpyIndex()), "inverted_index",
                indexLog, publications, new MetadataParser(), Tokenizer.standard(), Runnable::run);
        Publisher publisher = new Publisher(publications, "inverted_index", Clock.systemUTC());
        rebuilder = new Rebuilder(() -> Optional.of(coordination), service, datalake, publications, publisher,
                new InMemoryRebuildRepository(collections), "inverted_index", 100, Duration.ofSeconds(60),
                Clock.systemUTC());
        app = new IndexingApi(service, publisher, rebuilder).create("indexing-service", 8).start(0);
        datalake.save(new RawBook(1342, IndexingServiceTest.HEADER_1342, IndexingServiceTest.BODY_1342));
        datalake.save(new RawBook(11, IndexingServiceTest.HEADER_1342, "Alice and a fortune."));
    }

    @AfterEach
    void tearDown() {
        app.stop();
        rebuilder.close();
    }

    @Test
    void rebuildResponde202YStatusSigueSuProgresoHastaDone() throws Exception {
        HttpResponse<String> response = post("/index/rebuild");

        assertEquals(202, response.statusCode(), response.body());
        JsonObject started = json(response);
        assertEquals(Set.of("state", "started_at", "finished_at", "processed", "total", "error"), started.keySet());
        assertEquals("running", started.get("state").getAsString());
        assertTrue(started.get("started_at").getAsString().matches("\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\dZ"));
        assertTrue(started.get("finished_at").isJsonNull());
        assertEquals(0, started.get("processed").getAsInt());
        assertEquals(2, started.get("total").getAsInt());
        assertTrue(started.get("error").isJsonNull());

        JsonObject rebuild = null;
        for (int i = 0; i < 200; i++) {
            rebuild = json(get("/index/status")).getAsJsonObject("rebuild");
            if (!rebuild.get("state").getAsString().equals("running")) {
                break;
            }
            Thread.sleep(25);
        }
        assertEquals("done", rebuild.get("state").getAsString());
        assertEquals(2, rebuild.get("processed").getAsInt());
        assertTrue(rebuild.get("finished_at").isJsonPrimitive());
        JsonObject status = json(get("/index/status"));
        assertEquals(1, status.get("published_version").getAsInt());
        assertEquals(2, status.get("books_published").getAsLong());
        assertEquals(200, post("/index/update/1342").statusCode(), "al terminar se puede indexar otra vez");
    }

    @Test
    void mientrasReconstruyeRebuildUpdateYPublishResponden409() throws Exception {
        try (AutoCloseable writer = holdIndexWriter()) {            // the rebuild waits for it
            assertEquals(202, post("/index/rebuild").statusCode());

            for (String path : new String[]{"/index/rebuild", "/index/update/1342", "/index/publish"}) {
                HttpResponse<String> response = post(path);
                assertEquals(409, response.statusCode(), path);
                JsonObject error = json(response).getAsJsonObject("error");
                assertEquals("REBUILD_IN_PROGRESS", error.get("code").getAsString());
                assertEquals("a rebuild is running, retry later", error.get("message").getAsString());
                assertTrue(error.get("field").isJsonNull());
            }
            assertEquals("running", json(get("/index/status")).getAsJsonObject("rebuild").get("state").getAsString());
        }
    }

    @Test
    void laMarcaDeOtraReplicaTambienDa409() throws Exception {
        coordination.tryStartRebuild(Duration.ofSeconds(30)).orElseThrow();

        assertEquals(409, post("/index/update/1342").statusCode());
        assertEquals(409, post("/index/publish").statusCode());
        assertEquals(409, post("/index/rebuild").statusCode());
        assertEquals(400, post("/index/update/abc").statusCode(), "un id inválido sigue siendo 400");
    }

    /** index-writer held by another thread, as the writer of index-queue of a replica holds it. */
    private AutoCloseable holdIndexWriter() throws InterruptedException {
        CountDownLatch taken = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            coordination.indexWriterLock().lock();
            taken.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                coordination.indexWriterLock().unlock();
            }
        });
        thread.start();
        assertTrue(taken.await(5, TimeUnit.SECONDS));
        return () -> {
            release.countDown();
            thread.join(5000);
        };
    }

    private HttpResponse<String> post(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(uri(path)).POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(uri(path)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private URI uri(String path) {
        return URI.create("http://localhost:" + app.port() + path);
    }

    private static JsonObject json(HttpResponse<String> response) {
        return JsonParser.parseString(response.body()).getAsJsonObject();
    }
}
