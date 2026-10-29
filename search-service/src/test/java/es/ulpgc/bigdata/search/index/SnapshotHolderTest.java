package es.ulpgc.bigdata.search.index;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import es.ulpgc.bigdata.datamart.metadata.MongoMetadataRepository;
import es.ulpgc.bigdata.http.ServiceServer;
import es.ulpgc.bigdata.search.fixture.TestBooks;
import es.ulpgc.bigdata.search.testsupport.MongoTestDb;
import io.javalin.Javalin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** The holder and GET /status over HTTP, as the Docker healthcheck will call it. */
class SnapshotHolderTest {

    private String dbName;
    private MongoClient client;
    private SnapshotHolder holder;
    private Javalin app;

    @BeforeEach
    void setUp() {
        assumeTrue(MongoTestDb.available(), "MongoDB is not running: start it with docker compose up -d mongo");
        dbName = MongoTestDb.newDatabaseName("holder");
        client = MongoClients.create(MongoTestDb.URI);
        MongoDatabase db = client.getDatabase(dbName);
        holder = new SnapshotHolder(new IndexSnapshotLoader(db, new MongoMetadataRepository(db)));
        app = ServiceServer.create("search-service", ServiceServer.DEFAULT_MAX_THREADS, holder::status).start(0);
    }

    @AfterEach
    void tearDown() {
        if (app != null) {
            app.stop();
        }
        if (client != null) {
            client.getDatabase(dbName).drop();
            client.close();
        }
    }

    @Test
    void beforeAnyPublicationStatusSaysRunningWithNullVersion() throws Exception {
        assertTrue(holder.reload().isEmpty());

        JsonObject status = getStatus();

        assertEquals("running", status.get("status").getAsString());
        assertTrue(status.get("index_version").isJsonNull());
        assertTrue(status.get("books_indexed").isJsonNull());
    }

    @Test
    void afterReloadStatusReportsTheServedVersion() throws Exception {
        TestBooks.load(MongoTestDb.URI, dbName);

        holder.reload();
        JsonObject status = getStatus();

        assertEquals("search-service", status.get("service").getAsString());
        assertEquals(TestBooks.CURRENT_VERSION, status.get("index_version").getAsInt());
        assertEquals(TestBooks.VISIBLE_IDS.size(), status.get("books_indexed").getAsInt());
    }

    private JsonObject getStatus() throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + app.port() + "/status")).GET().build();
        HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        return JsonParser.parseString(response.body()).getAsJsonObject();
    }
}
