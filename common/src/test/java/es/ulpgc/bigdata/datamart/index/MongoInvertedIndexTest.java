package es.ulpgc.bigdata.datamart.index;

import es.ulpgc.bigdata.testsupport.MongoTestSupport;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Needs a running MongoDB (see MongoTestSupport for the address). If there is none, these
 * tests are SKIPPED instead of failing, so that "mvn verify" works without Docker.
 */
class MongoInvertedIndexTest {

    private static final String URI = MongoTestSupport.URI;
    private static final String TEST_DB = MongoTestSupport.TEST_DB;

    private String collectionName;
    private MongoInvertedIndex index;
    private MongoClient rawClient;                       // to look at what is really in Mongo
    private MongoCollection<Document> raw;

    @BeforeEach
    void setUp() {
        assumeTrue(MongoTestSupport.available(), "MongoDB no está disponible en " + URI + ": test saltado");
        collectionName = MongoTestSupport.newCollectionName("test");
        index = open();
        rawClient = MongoClients.create(URI);
        raw = rawClient.getDatabase(TEST_DB).getCollection(collectionName);
    }

    @AfterEach
    void tearDown() {
        if (index != null) {
            index.clear();                               // do not leave test collections behind
            index.close();
        }
        if (rawClient != null) {
            rawClient.close();
        }
    }

    private MongoInvertedIndex open() {
        return new MongoInvertedIndex(URI, TEST_DB, collectionName);
    }

    private static void addPaperBooks(InvertedIndex index) {
        index.addDocument(10, Set.of("red", "boat", "sails"));
        index.addDocument(20, Set.of("blue", "boat", "sails", "fast"));
        index.addDocument(30, Set.of("red", "island"));
    }

    private List<Object> storedArray(String term) {
        Document doc = raw.find(new Document("term", term)).first();
        return doc == null ? null : doc.getList("postings", Object.class);
    }

    // --- Challenge criteria --------------------------------------------------

    @Test
    void indexarDosVecesNoDuplicaIds() {
        index.addDocument(10, Set.of("boat"));
        index.addDocument(10, Set.of("boat"));               // twice in the same batch
        index.flush();
        index.addDocument(10, Set.of("boat"));               // and once more in a later batch
        index.flush();

        assertEquals(List.of(10), index.postings("boat"));
        assertEquals(1, storedArray("boat").size());          // also in Mongo, not only when reading
        assertEquals(1, raw.countDocuments(new Document("term", "boat")));
    }

    @Test
    void cerrarYVolverAAbrirConservaElIndice() {
        addPaperBooks(index);
        index.flush();
        index.close();

        index = open();                                       // "another run of the program"

        assertEquals(List.of(10, 20), index.postings("boat"));
        assertEquals(List.of(10, 30), index.postings("red"));
        assertEquals(List.of(30), index.postings("island"));
    }

    @Test
    void clearLimpiaElBackendEntreRepeticiones() {
        addPaperBooks(index);
        index.flush();
        assertTrue(index.diskUsageBytes() > 0);

        index.clear();                                        // "next benchmark repetition"

        assertEquals(List.of(), index.postings("boat"));
        assertEquals(0, index.diskUsageBytes());
        assertEquals(0, raw.countDocuments());

        addPaperBooks(index);                                 // the second repetition works the same
        index.flush();
        assertEquals(List.of(10, 20), index.postings("boat"));
        assertTrue(hasUniqueIndexOnTerm(), "clear debe dejar el índice único recreado al volver a escribir");
    }

    // --- Structure in Mongo ---------------------------------------------------------

    private boolean hasUniqueIndexOnTerm() {
        for (Document idx : raw.listIndexes().into(new ArrayList<>())) {
            if (new Document("term", 1).equals(idx.get("key", Document.class))
                    && Boolean.TRUE.equals(idx.getBoolean("unique"))) {
                return true;
            }
        }
        return false;
    }

    @Test
    void existeUnIndiceUnicoSobreTerm() {
        assertTrue(hasUniqueIndexOnTerm());

        raw.insertOne(new Document("term", "boat").append("postings", List.of(1)));
        assertThrows(com.mongodb.MongoWriteException.class,
                () -> raw.insertOne(new Document("term", "boat").append("postings", List.of(2))));
    }

    @Test
    void unDocumentoPorTerminoConElFormatoDelSpec() {
        addPaperBooks(index);
        index.flush();

        assertEquals(6, raw.countDocuments());
        Document boat = raw.find(new Document("term", "boat")).first();
        assertNotNull(boat);
        assertEquals("boat", boat.getString("term"));
        assertEquals(Set.of(10, 20), Set.copyOf(boat.getList("postings", Integer.class)));
    }

    @Test
    void postingsSaleOrdenadaAunqueMongoGuardeEnOrdenDeLlegada() {
        index.addDocument(30, Set.of("red"));
        index.flush();
        index.addDocument(10, Set.of("red"));
        index.flush();

        assertEquals(List.of(30, 10), storedArray("red"));    // this is how it ends up in Mongo
        assertEquals(List.of(10, 30), index.postings("red"));  // this is how the index returns it
    }

    // --- addDocument accumulates until flush -------------------------------------

    @Test
    void loAnadidoSeVeAntesDelFlushPeroNoLlegaAMongoSinEl() {
        addPaperBooks(index);

        assertEquals(List.of(10, 20), index.postings("boat"));
        assertEquals(0, raw.countDocuments());
    }

    @Test
    void flushSinCambiosNoEscribeNada() {
        index.flush();
        assertEquals(0, raw.countDocuments());
    }

    @Test
    void terminoInexistenteDaListaVacia() {
        addPaperBooks(index);
        index.flush();
        assertEquals(List.of(), index.postings("whale"));
        assertEquals(List.of(), index.postings("Boat"));      // Mongo is case-sensitive
    }

    @Test
    void leeIdsGuardadosComoInt64PorOtroPrograma() {
        raw.insertOne(new Document("term", "boat").append("postings", List.of(20L, 10L, 20L)));

        assertEquals(List.of(10, 20), index.postings("boat"));
    }
}