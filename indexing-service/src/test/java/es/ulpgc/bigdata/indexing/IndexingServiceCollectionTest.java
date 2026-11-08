package es.ulpgc.bigdata.indexing;

import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.datamart.index.InMemoryInvertedIndex;
import es.ulpgc.bigdata.datamart.index.InvertedIndex;
import es.ulpgc.bigdata.datamart.index.Tokenizer;
import es.ulpgc.bigdata.datamart.metadata.MetadataParser;
import es.ulpgc.bigdata.model.RawBook;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Which collection of the inverted index IndexingService writes into (D8, DR-10): the one
 * CURRENT names, read again before each write, and a given one for the rebuild. One in-memory
 * index per collection name, as App opens one MongoInvertedIndex per collection.
 */
class IndexingServiceCollectionTest {

    private static final String DEFAULT = "inverted_index";
    private static final String REBUILT = "inverted_index_r01";

    @TempDir Path dataDir;

    private RangeBasedDatalake datalake;
    private InMemoryMetadataRepository metadata;
    private InMemoryIndexLog indexLog;
    private InMemoryPublicationRepository publications;
    private final Map<String, SizedIndex> collections = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> opens = new ConcurrentHashMap<>();
    private IndexingService service;

    @BeforeEach
    void setUp() {
        datalake = new RangeBasedDatalake(dataDir.resolve("datalake/range"));
        metadata = new InMemoryMetadataRepository();
        indexLog = new InMemoryIndexLog();
        publications = new InMemoryPublicationRepository();
        service = new IndexingService(datalake, metadata, this::open, DEFAULT, indexLog, publications,
                new MetadataParser(), Tokenizer.standard(), Runnable::run);
        datalake.save(new RawBook(1342, IndexingServiceTest.HEADER_1342, IndexingServiceTest.BODY_1342));
        datalake.save(book(11, "Alice and the white rabbit, a fortune of tea."));
        datalake.save(book(84, "Victor builds a creature with a fortune."));
    }

    private InvertedIndex open(String collection) {
        opens.computeIfAbsent(collection, c -> new AtomicInteger()).incrementAndGet();
        return collections.computeIfAbsent(collection, c -> new SizedIndex(100 + collections.size()));
    }

    private List<Integer> postings(String collection, String term) {
        SizedIndex index = collections.get(collection);
        return index == null ? List.of() : index.postings(term);
    }

    private static RawBook book(int id, String body) {
        return new RawBook(id, "Title: Book " + id + "\n\nAuthor: Someone\n\nLanguage: English\n", body);
    }

    @Test
    void sinCurrentLasPostingListsVanALaColeccionPorDefecto() throws Exception {
        service.update(1342);
        service.indexBatch(List.of(11));

        assertEquals(List.of(11, 1342), postings(DEFAULT, "fortune"));
        assertEquals(Set.of(DEFAULT), collections.keySet(), "ninguna otra colección");
    }

    @Test
    void vanALaColeccionQueNombraCurrent() throws Exception {
        publications.setCurrent(new Publication(3, 0, REBUILT, "2026-11-06T10:00:00Z"));

        service.update(1342);
        service.indexBatch(List.of(11));

        assertEquals(List.of(11, 1342), postings(REBUILT, "fortune"));
        assertEquals(List.of(), postings(DEFAULT, "fortune"), "Search ya no lee la de por defecto");
    }

    @Test
    void siCurrentNoNombraColeccionVanALaPorDefecto() {
        publications.setCurrent(new Publication(3, 0));            // collection null

        service.update(1342);

        assertEquals(List.of(1342), postings(DEFAULT, "fortune"));
    }

    @Test
    void currentSeLeeAntesDeCadaEscrituraYCadaColeccionSeAbreUnaVez() throws Exception {
        service.update(1342);                                       // before any rebuild
        // Another replica rebuilt the index and switched CURRENT
        publications.setCurrent(new Publication(2, 1, REBUILT, "2026-11-06T10:00:00Z"));

        service.update(11);
        service.indexBatch(List.of(84));
        service.update(84);

        assertEquals(List.of(1342), postings(DEFAULT, "fortune"));
        assertEquals(List.of(11, 84), postings(REBUILT, "fortune"), "en cuanto cambia CURRENT, sin reiniciar");
        assertEquals(1, opens.get(DEFAULT).get());
        assertEquals(1, opens.get(REBUILT).get(), "el índice de una colección se abre una sola vez");
    }

    @Test
    void indexBatchIntoEscribeEnLaColeccionPedidaAunqueCurrentNombreOtra() throws Exception {
        publications.setCurrent(new Publication(5, 3, DEFAULT, "2026-11-06T10:00:00Z"));

        BatchResult result = service.indexBatchInto(REBUILT, List.of(1342, 11, 999));

        assertEquals(List.of(1342, 11), result.indexed().stream().map(UpdateResult::bookId).toList());
        assertTrue(result.rejected().containsKey(999), "un libro que no está se rechaza igual que en indexBatch");
        assertEquals(List.of(11, 1342), postings(REBUILT, "fortune"));
        assertFalse(collections.containsKey(DEFAULT), "la que lee Search no se toca");
        assertEquals(2, metadata.count(), "las fichas y el log son los de siempre");
        assertEquals(Set.of(11, 1342), indexLog.ids());
    }

    @Test
    void statusMideLaColeccionDeCurrent() {
        service.update(1342);
        assertEquals(collections.get(DEFAULT).size, service.status().indexSizeBytes());

        publications.setCurrent(new Publication(2, 1, REBUILT, "2026-11-06T10:00:00Z"));
        service.update(1342);

        assertEquals(collections.get(REBUILT).size, service.status().indexSizeBytes());
    }

    /** An in-memory index that says it takes {@code size} bytes, to tell collections apart in status. */
    private static final class SizedIndex extends InMemoryInvertedIndex {
        final long size;

        SizedIndex(long size) {
            this.size = size;
        }

        @Override
        public long diskUsageBytes() {
            return size;
        }
    }
}
