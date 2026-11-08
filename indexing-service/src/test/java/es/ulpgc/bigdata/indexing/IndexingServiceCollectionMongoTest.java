package es.ulpgc.bigdata.indexing;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.datamart.index.MongoInvertedIndex;
import es.ulpgc.bigdata.datamart.index.Tokenizer;
import es.ulpgc.bigdata.datamart.indexlog.MongoIndexLog;
import es.ulpgc.bigdata.datamart.metadata.MetadataParser;
import es.ulpgc.bigdata.datamart.metadata.MongoMetadataRepository;
import es.ulpgc.bigdata.model.RawBook;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * IndexingService wired as App wires it (one MongoInvertedIndex per collection on the shared
 * client), against a real MongoDB: the posting lists land in the collection CURRENT names. Its
 * own database, dropped at the end, because it writes CURRENT. Skipped without MongoDB.
 */
class IndexingServiceCollectionMongoTest {

    private static final String URI = System.getenv().getOrDefault("MONGO_URI", "mongodb://localhost:27017");

    @TempDir Path dataDir;

    private MongoClient client;
    private MongoDatabase db;
    private RangeBasedDatalake datalake;
    private MongoPublicationRepository publications;
    private IndexingService service;

    @BeforeEach
    void setUp() {
        client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(URI))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(1500, TimeUnit.MILLISECONDS))
                .build());
        assumeTrue(mongoAnswers(), "MongoDB no está disponible en " + URI + ": test saltado");

        db = client.getDatabase("search_engine_test_dr10_" + System.nanoTime());
        datalake = new RangeBasedDatalake(dataDir.resolve("datalake/range"));
        publications = new MongoPublicationRepository(db);
        service = new IndexingService(datalake, new MongoMetadataRepository(db),
                collection -> new MongoInvertedIndex(db, collection), MongoInvertedIndex.DEFAULT_COLLECTION,
                new MongoIndexLog(db), publications, new MetadataParser(), Tokenizer.standard(), Runnable::run);
        datalake.save(new RawBook(1342, IndexingServiceTest.HEADER_1342, IndexingServiceTest.BODY_1342));
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
            db.drop();
        }
        client.close();
    }

    private List<Integer> postings(String collection, String term) {
        Document doc = db.getCollection(collection).find(Filters.eq("term", term)).first();
        return doc == null ? List.of() : doc.getList("postings", Integer.class);
    }

    private boolean exists(String collection) {
        return db.listCollectionNames().into(new ArrayList<>()).contains(collection);
    }

    @Test
    void lasPostingListsVanALaColeccionDeCurrentYNoALaDeMongoCollection() {
        assertTrue(publications.switchCurrent(null,
                new Publication(1, 0, "inverted_index_r01", "2026-11-06T10:00:00Z")));

        service.update(1342);

        assertEquals(List.of(1342), postings("inverted_index_r01", "elizabeth"));
        assertFalse(exists(MongoInvertedIndex.DEFAULT_COLLECTION), "inverted_index ni se crea");
        assertTrue(service.status().indexSizeBytes() > 0, "status mide inverted_index_r01");
    }

    @Test
    void sinCurrentVanAMongoCollectionYIndexBatchIntoALaPedida() throws Exception {
        service.update(1342);
        service.indexBatchInto("inverted_index_r02", List.of(1342));

        assertEquals(List.of(1342), postings(MongoInvertedIndex.DEFAULT_COLLECTION, "elizabeth"));
        assertEquals(List.of(1342), postings("inverted_index_r02", "elizabeth"));
        assertEquals(1, db.getCollection("inverted_index_r02").listIndexes().into(new ArrayList<>())
                .stream().filter(i -> Boolean.TRUE.equals(i.get("unique"))).count(),
                "la colección nueva también tiene el índice único de term");
    }
}
