package es.ulpgc.bigdata.indexing;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.datamart.index.MongoInvertedIndex;
import es.ulpgc.bigdata.datamart.index.Tokenizer;
import es.ulpgc.bigdata.datamart.metadata.MetadataParser;
import es.ulpgc.bigdata.datamart.metadata.MongoMetadataRepository;
import es.ulpgc.bigdata.model.RawBook;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * IndexingService with the real MongoDB classes on ONE shared client, as App wires them.
 * Skipped if there is no MongoDB (docker compose up -d), like the Mongo tests of common.
 */
class IndexingServiceMongoTest {

    private static final String URI = System.getenv().getOrDefault("MONGO_URI", "mongodb://localhost:27017");
    private static final String TEST_DB = "search_engine_test";

    @TempDir Path dataDir;

    private MongoClient client;
    private MongoDatabase db;
    private String booksCollection;
    private String indexCollection;
    private IndexingService service;
    private RangeBasedDatalake datalake;

    @BeforeEach
    void setUp() {
        client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(URI))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(1500, TimeUnit.MILLISECONDS))
                .build());
        assumeTrue(mongoAnswers(), "MongoDB no está disponible en " + URI + ": test saltado");

        db = client.getDatabase(TEST_DB);
        long suffix = System.nanoTime();
        booksCollection = "books_" + suffix;
        indexCollection = "index_" + suffix;
        datalake = new RangeBasedDatalake(dataDir.resolve("datalake/range"));
        service = new IndexingService(datalake,
                new MongoMetadataRepository(db, booksCollection),
                new MongoInvertedIndex(db, indexCollection),
                new MetadataParser(), Tokenizer.standard());
    }

    private boolean mongoAnswers() {
        try {
            client.getDatabase("admin").runCommand(new Document("ping", 1));
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    @AfterEach
    void tearDown() {
        if (db != null) {
            db.getCollection(booksCollection).drop();
            db.getCollection(indexCollection).drop();
        }
        client.close();
    }

    @Test
    void elLibroAcabaEnBooksYEnInvertedIndex() {
        datalake.save(new RawBook(1342, IndexingServiceTest.HEADER_1342, IndexingServiceTest.BODY_1342));

        UpdateResult result = service.update(1342);

        Document book = db.getCollection(booksCollection).find(new Document("_id", 1342)).first();
        assertNotNull(book, "falta el documento en books");
        assertEquals("Pride and Prejudice", book.getString("title"));
        assertEquals("en", book.getString("language"));

        long termDocs = db.getCollection(indexCollection).countDocuments();
        assertEquals(result.terms(), termDocs, "un documento por término");
        Document elizabeth = db.getCollection(indexCollection).find(new Document("term", "elizabeth")).first();
        assertNotNull(elizabeth);
        assertEquals(List.of(1342), elizabeth.getList("postings", Integer.class));
    }

    @Test
    void indexarDosVecesNoDuplicaNadaEnMongo() {
        datalake.save(new RawBook(1342, IndexingServiceTest.HEADER_1342, IndexingServiceTest.BODY_1342));

        UpdateResult first = service.update(1342);
        service.update(1342);

        assertEquals(1, db.getCollection(booksCollection).countDocuments());
        assertEquals(first.terms(), db.getCollection(indexCollection).countDocuments());
        Document elizabeth = db.getCollection(indexCollection).find(new Document("term", "elizabeth")).first();
        assertEquals(Set.of(1342), Set.copyOf(elizabeth.getList("postings", Integer.class)));
        assertEquals(1, elizabeth.getList("postings", Integer.class).size());
    }
}
