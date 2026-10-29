package es.ulpgc.bigdata.datamart.metadata;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import es.ulpgc.bigdata.model.BookMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What callers see when MongoDB does not answer. It points at a port where nothing
 * listens, so it does NOT need a running MongoDB: it always runs, also in CI.
 */
class MongoMetadataRepositoryErrorTest {

    private MongoClient client;
    private MetadataRepository repo;

    @BeforeEach
    void setUp() {
        client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://127.0.0.1:1"))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(200, TimeUnit.MILLISECONDS))
                .build());
        repo = new MongoMetadataRepository(client.getDatabase("search_engine_test"));
    }

    @AfterEach
    void tearDown() {
        client.close();
    }

    @Test
    void crearElRepositorioNoNecesitaQueMongoResponda() {
        // The constructor does not talk to Mongo: the first operation does.
        assertNotNull(repo);
    }

    @Test
    void losErroresDeMongoLleganComoMetadataRepositoryException() {
        MetadataRepositoryException e = assertThrows(MetadataRepositoryException.class, () -> repo.findById(1342));

        assertInstanceOf(MongoException.class, e.getCause());      // the original error is kept as the cause
        assertTrue(e.getMessage().contains("1342"));
    }

    @Test
    void tambienAlEscribir() {
        BookMetadata book = new BookMetadata(1, "A", null, null, null, null, null, null, null);

        assertThrows(MetadataRepositoryException.class, () -> repo.save(book));
        assertThrows(MetadataRepositoryException.class, () -> repo.saveAll(List.of(book)));
        assertThrows(MetadataRepositoryException.class, () -> repo.count());
    }
}
