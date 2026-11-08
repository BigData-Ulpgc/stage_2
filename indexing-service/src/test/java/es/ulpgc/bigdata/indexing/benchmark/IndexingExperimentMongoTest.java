package es.ulpgc.bigdata.indexing.benchmark;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import es.ulpgc.bigdata.datamart.index.MongoInvertedIndex;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** The stats of DR-13 against a real MongoDB. Skipped without one. */
class IndexingExperimentMongoTest {

    private static final String URI = System.getenv().getOrDefault("MONGO_URI", "mongodb://localhost:27017");

    private MongoClient client;
    private MongoDatabase db;

    @BeforeEach
    void setUp() {
        client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(URI))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(1500, TimeUnit.MILLISECONDS))
                .build());
        assumeTrue(mongoAnswers(), "MongoDB no está disponible en " + URI + ": test saltado");
        db = client.getDatabase("search_engine_test_dr13_" + System.nanoTime());
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

    /** "the" in 6 books, "whale" in 4, "sea" in 3, "ship" in 2, "boat" and "oar" in 1. */
    private void index(String collection) {
        MongoInvertedIndex index = new MongoInvertedIndex(db, collection);
        index.addDocument(1, Set.of("the", "whale", "sea", "ship"));
        index.addDocument(2, Set.of("the", "whale", "sea"));
        index.addDocument(3, Set.of("the", "whale", "sea", "ship"));
        index.addDocument(4, Set.of("the", "whale"));
        index.addDocument(5, Set.of("the", "boat"));
        index.addDocument(6, Set.of("the", "oar"));
        index.flush();
    }

    /** metric|collection|term -> value */
    private static Map<String, String> byKey(List<String> rows) {
        assertEquals("metric,collection,term,value,unit", rows.get(0));
        Map<String, String> values = new LinkedHashMap<>();
        for (String row : rows.subList(1, rows.size())) {
            String[] f = row.split(",", -1);
            assertEquals(5, f.length, row);
            values.put(f[0] + "|" + f[1] + "|" + f[2], f[3]);
        }
        return values;
    }

    @Test
    void cuentaLosTerminosYDaLasListasMasLargasConSuTiempoDeLectura() {
        index("inverted_index");
        Map<String, String> v = byKey(IndexingExperiment.stats(db, "inverted_index"));

        assertEquals("6", v.get("terms|inverted_index|"));
        assertEquals("17", v.get("postings|inverted_index|"), "6 + 4 + 3 + 2 + 1 + 1");
        assertEquals("2.833", v.get("mean_postings|inverted_index|"));
        assertEquals("6", v.get("documents|inverted_index|"));
        assertTrue(Long.parseLong(v.get("index_size|inverted_index|")) > 0, "el índice único de term");

        List<String> longest = v.keySet().stream().filter(k -> k.startsWith("posting_length|")).toList();
        assertEquals(List.of("posting_length|inverted_index|the", "posting_length|inverted_index|whale",
                "posting_length|inverted_index|sea", "posting_length|inverted_index|ship",
                "posting_length|inverted_index|boat"), longest, "las 5 más largas, y a igual longitud por orden alfabético");
        assertEquals("6", v.get("posting_length|inverted_index|the"));
        assertTrue(Long.parseLong(v.get("posting_bytes|inverted_index|the"))
                > Long.parseLong(v.get("posting_bytes|inverted_index|boat")), "más ids, más bytes");
        assertTrue(Double.parseDouble(v.get("read_median|inverted_index|the")) > 0);
        assertTrue(Double.parseDouble(v.get("read_max|inverted_index|the"))
                >= Double.parseDouble(v.get("read_median|inverted_index|the")));

        assertEquals("0", v.get("documents|books|"), "una colección que no existe cuenta 0");
        assertEquals("0", v.get("storage_size|index_log|"));
    }

    @Test
    void laPruebaDeActualizacionMideAddToSetYPushYNoDejaNada() {
        List<String> rows = IndexingExperiment.probe(db, List.of(100, 1_000));

        assertEquals(List.of("operation,list_length,median_ms,max_ms"), rows.subList(0, 1));
        assertEquals(List.of("addToSet,100", "push,100", "addToSet,1000", "push,1000"),
                rows.stream().skip(1).map(r -> String.join(",", List.of(r.split(",")).subList(0, 2))).toList());
        for (String row : rows.subList(1, rows.size())) {
            String[] f = row.split(",");
            assertTrue(Double.parseDouble(f[2]) > 0 && Double.parseDouble(f[3]) >= Double.parseDouble(f[2]), row);
        }
        assertFalse(db.listCollectionNames().into(new java.util.ArrayList<>()).contains(IndexingExperiment.PROBE_COLLECTION),
                "borra su colección");
    }

    @Test
    void miraLaColeccionQueNombraCurrent() {
        index("inverted_index_r01");
        db.getCollection("index_meta").insertOne(new Document("_id", "CURRENT")
                .append("version", 3).append("collection", "inverted_index_r01"));

        Map<String, String> v = byKey(IndexingExperiment.stats(db, "inverted_index"));

        assertEquals("6", v.get("terms|inverted_index_r01|"));
        assertFalse(v.containsKey("documents|inverted_index|"), "la colección por defecto ni se mira");
    }
}
