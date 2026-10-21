package es.ulpgc.bigdata.datamart.metadata;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import es.ulpgc.bigdata.model.BookMetadata;
import es.ulpgc.bigdata.model.YearSource;
import es.ulpgc.bigdata.testsupport.MongoTestSupport;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Needs a running MongoDB (see MongoTestSupport). Without it these tests are SKIPPED.
 * Each test uses its own collection in search_engine_test and drops it afterwards.
 */
class MongoMetadataRepositoryTest {

    private MongoClient client;
    private MongoDatabase database;
    private String collectionName;
    private MetadataRepository repo;                     // used as an interface, not as a concrete class
    private MongoCollection<Document> raw;               // to look at what is really in Mongo

    @BeforeEach
    void setUp() {
        assumeTrue(MongoTestSupport.available(), "MongoDB no está disponible en " + MongoTestSupport.URI);
        client = MongoClients.create(MongoTestSupport.URI);
        database = client.getDatabase(MongoTestSupport.TEST_DB);
        collectionName = MongoTestSupport.newCollectionName("books");
        repo = new MongoMetadataRepository(database, collectionName);
        raw = database.getCollection(collectionName);
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            raw.drop();                                  // do not leave test collections behind
            client.close();
        }
    }

    private static BookMetadata book(int id, String title, String author) {
        return new BookMetadata(id, title, author, "en", 1998, YearSource.RELEASE_DATE, "June 1, 1998",
                Path.of("datalake/range/01000-01999/" + id + ".body.txt"),
                Path.of("datalake/range/01000-01999/" + id + ".header.txt"));
    }

    /** Book 67979 of the header survey: the year comes from "Original publication". */
    private static BookMetadata blueCastle() {
        return new BookMetadata(67979, "The Blue Castle: a novel", "L. M. Montgomery", "en",
                1926, YearSource.ORIGINAL_PUBLICATION, "May 3, 2022",
                Path.of("/data/datalake/range/67000-67999/67979.body.txt"),
                Path.of("/data/datalake/range/67000-67999/67979.header.txt"));
    }

    private static BookMetadata withoutData(int id) {
        return new BookMetadata(id, "Beowulf", null, null, null, null, null, null, null);
    }

    private static List<Integer> ids(List<BookMetadata> books) {
        return books.stream().map(BookMetadata::bookId).toList();
    }

    // --- findById -----------------------------------------------------------

    @Test
    void findByIdInexistenteDevuelveVacio() {
        assertTrue(repo.findById(999).isEmpty());
    }

    @Test
    void guardaYRecuperaTodosLosCampos() {
        repo.save(blueCastle());

        assertEquals(blueCastle(), repo.findById(67979).orElseThrow());
    }

    @Test
    void camposNullSeConservanComoNull() {
        repo.save(withoutData(16328));

        assertEquals(withoutData(16328), repo.findById(16328).orElseThrow());
    }

    // --- save: idempotent, replaces the whole document ---------------------

    @Test
    void guardarOtraVezElMismoIdLoReemplaza() {
        repo.save(book(1342, "Titulo provisional", "Anónimo"));
        repo.save(book(1342, "Pride and Prejudice", "Jane Austen"));

        assertEquals(1, repo.count());
        assertEquals("Pride and Prejudice", repo.findById(1342).orElseThrow().title());
    }

    @Test
    void reemplazarBorraLosCamposQueAhoraSonNull() {
        // With updateOne + $set only the given fields would change. replaceOne leaves the
        // document EXACTLY as the new BookMetadata, so the old author does not survive.
        repo.save(book(1342, "Pride and Prejudice", "Jane Austen"));
        repo.save(new BookMetadata(1342, "Pride and Prejudice", null, null, null, null, null, null, null));

        assertNull(repo.findById(1342).orElseThrow().author());
    }

    // --- saveAll: one ordered bulkWrite, not atomic, safe to repeat ---------

    @Test
    void saveAllGuardaTodoElLote() {
        repo.saveAll(List.of(book(1, "A", "X"), book(2, "B", "Y"), book(3, "C", "X")));

        assertEquals(3, repo.count());
    }

    @Test
    void repetirUnLoteNoDuplicaNada() {
        List<BookMetadata> batch = List.of(book(1, "A", "X"), book(2, "B", "Y"), blueCastle());

        repo.saveAll(batch);
        repo.saveAll(batch);                             // what Indexing does after a failure

        assertEquals(3, repo.count());
        assertEquals(batch, repo.findByIds(List.of(1, 2, 67979)));
    }

    @Test
    void siUnLoteTraeDosVecesElMismoIdGanaElUltimo() {
        repo.saveAll(List.of(book(1342, "Primero", "X"), book(84, "Frankenstein", "Mary Shelley"),
                book(1342, "Último", "X")));

        assertEquals(2, repo.count());
        assertEquals("Último", repo.findById(1342).orElseThrow().title());
    }

    @Test
    void unNullEnElLoteNoEscribeNada() {
        // The batch is checked before sending it, so books 1 and 2 are not saved either.
        List<BookMetadata> batch = Arrays.asList(book(1, "A", "X"), book(2, "B", "Y"), null);

        assertThrows(NullPointerException.class, () -> repo.saveAll(batch));
        assertEquals(0, repo.count());
    }

    @Test
    void saveAllVacioNoHaceNada() {
        repo.saveAll(List.of());

        assertEquals(0, repo.count());
    }

    // --- findByIds ----------------------------------------------------------

    @Test
    void findByIdsOrdenaSeSaltaLosDesconocidosYQuitaRepetidos() {
        repo.saveAll(List.of(book(1342, "Pride and Prejudice", "Jane Austen"),
                book(84, "Frankenstein", "Mary Shelley"), book(158, "Emma", "Jane Austen")));

        assertEquals(List.of(84, 158, 1342), ids(repo.findByIds(List.of(1342, 999, 84, 158, 84))));
    }

    @Test
    void findByIdsSinIdsDevuelveListaVacia() {
        repo.save(book(84, "Frankenstein", "Mary Shelley"));

        assertEquals(List.of(), repo.findByIds(List.of()));
    }

    @Test
    void findByIdsConMasIdsQueUnBloque() {
        // 2.5 blocks of IDS_PER_QUERY, saved in random order: the blocks must join in order.
        int n = 2 * MongoMetadataRepository.IDS_PER_QUERY + MongoMetadataRepository.IDS_PER_QUERY / 2;
        List<BookMetadata> books = new ArrayList<>(IntStream.rangeClosed(1, n)
                .mapToObj(id -> book(id, "Libro " + id, "Autor"))
                .toList());
        Collections.shuffle(books, new Random(42));
        repo.saveAll(books);

        List<Integer> asked = new ArrayList<>(IntStream.rangeClosed(1, n + 10).boxed().toList());   // 10 unknown
        Collections.shuffle(asked, new Random(7));

        assertEquals(IntStream.rangeClosed(1, n).boxed().toList(), ids(repo.findByIds(asked)));
    }

    // --- The document itself: the contract with Search (§6.3) --------------

    @Test
    void elDocumentoTieneLaFormaDelContrato() {
        repo.save(blueCastle());

        Document doc = raw.find().first();

        assertEquals(List.of("_id", "title", "author", "language", "year", "year_source",
                "release_date", "header_path", "body_path"), new ArrayList<>(doc.keySet()));
        assertEquals(67979, doc.get("_id"));                       // the book id IS the _id, as a number
        assertInstanceOf(Integer.class, doc.get("year"));
        assertEquals("original_publication", doc.get("year_source"));   // the contract text, not the enum name
        assertEquals("/data/datalake/range/67000-67999/67979.body.txt", doc.get("body_path"));
    }

    @Test
    void sinDatosElDocumentoTieneLosNueveCamposANull() {
        repo.save(withoutData(16328));

        Document doc = raw.find().first();

        assertEquals(9, doc.size());
        assertTrue(doc.containsKey("author"));                     // explicit null, not a missing field
        assertNull(doc.get("author"));
        assertNull(doc.get("year_source"));
    }

    @Test
    void leeDocumentosEscritosAMano() {
        // mongosh stores every number as a double: {_id: 7} is really {_id: 7.0}.
        raw.insertOne(new Document("_id", 7.0).append("title", "A mano").append("year", 1813.0)
                .append("year_source", "original_publication"));

        BookMetadata m = repo.findById(7).orElseThrow();

        assertEquals(7, m.bookId());
        assertEquals(1813, m.year());
        assertEquals(YearSource.ORIGINAL_PUBLICATION, m.yearSource());
        assertNull(m.author());                                    // missing field -> null as well
    }

    // --- count, clear and close ---------------------------------------------

    @Test
    void clearVaciaYElRepositorioSigueFuncionando() {
        repo.saveAll(List.of(book(1, "A", "X"), book(2, "B", "Y")));

        repo.clear();
        assertEquals(0, repo.count());
        assertTrue(repo.findById(1).isEmpty());

        repo.save(book(3, "C", "Z"));                              // Mongo creates the collection again
        assertEquals(1, repo.count());
    }

    @Test
    void cerrarElRepositorioNoCierraElCliente() {
        repo.close();

        // The shared client still works: another repository (or the index) can keep using it.
        MetadataRepository other = new MongoMetadataRepository(database, collectionName);
        other.save(book(84, "Frankenstein", "Mary Shelley"));
        assertEquals(1, other.count());
    }
}
