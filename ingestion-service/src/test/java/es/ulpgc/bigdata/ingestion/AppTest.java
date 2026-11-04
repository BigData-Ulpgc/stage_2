package es.ulpgc.bigdata.ingestion;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hazelcast.core.HazelcastInstance;
import es.ulpgc.bigdata.config.Env;
import es.ulpgc.bigdata.coordination.BookState;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.CoordinationSettings;
import es.ulpgc.bigdata.testsupport.HazelcastTestCluster;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The real App: start order, GET /status while connecting, and one book end to end. */
class AppTest {

    @TempDir
    Path dataDir;
    @TempDir
    Path books;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    private Map<String, String> env(int workers) {
        Map<String, String> env = new HashMap<>();
        env.put("INGESTION_PORT", "0");
        env.put("DATA_DIR", dataDir.toString());
        env.put("INGESTION_SOURCE", "local");
        env.put("INGESTION_LOCAL_DIR", books.toString());
        env.put("INGESTION_WORKERS", String.valueOf(workers));
        return env;
    }

    @Test
    void withoutWorkersItDoesNotConnectToHazelcast() throws Exception {
        try (App.RunningService service = App.start(Env.of(env(0)))) {
            assertEquals("disabled", status(service).get("coordination").getAsString());
        }
    }

    /** Connecting blocks until the cluster answers; /status must not (review of #19). */
    @Test
    void statusAnswersAtOnceWhileTheClusterIsUnreachable() throws Exception {
        int nobody;
        try (ServerSocket socket = new ServerSocket(0)) {
            nobody = socket.getLocalPort();
        }
        Map<String, String> env = env(2);
        env.put("HZ_MEMBERS", "127.0.0.1:" + nobody);
        long start = System.nanoTime();
        try (App.RunningService service = App.start(Env.of(env))) {
            JsonObject status = status(service);
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertEquals("connecting", status.get("coordination").getAsString());
            assertTrue(millis < 5_000, "start + /status took " + millis + " ms");
        }
    }

    @Test
    void aSeededBookIsIngestedByTheWorkersOfTheApp() throws Exception {
        Files.writeString(books.resolve("pg1342.txt"), FakeSource.VALID_TEXT);
        CoordinationSettings settings = HazelcastTestCluster.settings();
        HazelcastTestCluster.reset();
        Map<String, String> env = env(2);
        env.put("HZ_MEMBERS", String.join(",", settings.members()));
        env.put("HZ_CLUSTER_NAME", settings.clusterName());
        HazelcastInstance control = settings.openClient();                     // plays the Control
        try (App.RunningService service = App.start(Env.of(env))) {
            Coordination coordination = Coordination.on(control, settings, Clock.systemUTC());
            coordination.seed(List.of(1342));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (coordination.task(1342).orElseThrow().bookState() != BookState.DOWNLOADED
                    && System.nanoTime() < deadline) {
                Thread.sleep(100);
            }
            assertEquals(BookState.DOWNLOADED, coordination.task(1342).orElseThrow().bookState());
            JsonObject status = status(service);
            assertEquals("connected", status.get("coordination").getAsString());
            JsonObject workers = status.getAsJsonObject("workers");
            assertEquals(2, workers.get("workers").getAsInt());
            assertEquals(1, workers.get("downloaded").getAsInt());
        } finally {
            control.shutdown();
        }
    }

    private JsonObject status(App.RunningService service) throws Exception {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(
                        URI.create("http://localhost:" + service.port() + "/status")).timeout(Duration.ofSeconds(5)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        return JsonParser.parseString(response.body()).getAsJsonObject();
    }
}
