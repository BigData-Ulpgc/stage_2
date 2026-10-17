package es.ulpgc.bigdata.datamart.index;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests that any InvertedIndex must pass, however it is stored.
 * Each test runs once per implementation.
 */
class InvertedIndexContractTest {

    @TempDir Path tmp;

    /**
     * Implementations to test: expected name + recipe to create an empty one.
     * The recipe receives a temporary folder for the indexes that use disk.
     * Stage 2 keeps memory (reference, no external service) and mongo (the index Indexing
     * writes and Search reads). The mongo runs are skipped if MongoDB is not available.
     */
    static Stream<Arguments> indexes() {
        return Stream.of(
                Arguments.of("memory", (Function<Path, InvertedIndex>) dir -> new InMemoryInvertedIndex()),
                Arguments.of("mongo", (Function<Path, InvertedIndex>) dir -> openMongo()));
    }

    /** Mongo collections created by the current test, dropped after it. */
    private static final List<String> mongoCollections = new ArrayList<>();

    private static InvertedIndex openMongo() {
        assumeTrue(MongoTestSupport.available(), "MongoDB no está disponible en " + MongoTestSupport.URI);
        String collection = MongoTestSupport.newCollectionName("contract");
        mongoCollections.add(collection);
        return new MongoInvertedIndex(MongoTestSupport.URI, MongoTestSupport.TEST_DB, collection);
    }

    @AfterEach
    void dropMongoCollections() {
        for (String collection : mongoCollections) {
            try (MongoInvertedIndex index = new MongoInvertedIndex(
                    MongoTestSupport.URI, MongoTestSupport.TEST_DB, collection)) {
                index.clear();
            }
        }
        mongoCollections.clear();
    }

    private InvertedIndex create(Function<Path, InvertedIndex> factory) {
        return factory.apply(tmp);
    }

    /** The three books of the paper exercise, already tokenized. */
    private static void addPaperBooks(InvertedIndex index) {
        index.addDocument(10, Set.of("red", "boat", "sails"));
        index.addDocument(20, Set.of("blue", "boat", "sails", "fast"));
        index.addDocument(30, Set.of("red", "island"));
    }

    /**
     * A "search" that only knows the interface: it does not know whether the index is
     * memory, JSON or Mongo. The real search engine arrives in challenge 20.
     */
    private static List<Integer> searchAnd(InvertedIndex index, String a, String b) {
        List<Integer> second = index.postings(b);
        return index.postings(a).stream().filter(second::contains).toList();
    }

    // --- Identity -----------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexes")
    void tieneElNombreDeSuEstructura(String name, Function<Path, InvertedIndex> factory) {
        try (InvertedIndex index = create(factory)) {
            assertEquals(name, index.name());
        }
    }

    // --- addDocument and postings ------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexes")
    void postingsOrdenadasSinRepetir(String name, Function<Path, InvertedIndex> factory) {
        try (InvertedIndex index = create(factory)) {
            index.addDocument(30, Set.of("red"));
            index.addDocument(10, Set.of("red"));
            index.addDocument(10, Set.of("red"));            // the same book again

            assertEquals(List.of(10, 30), index.postings("red"));
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexes")
    void terminoInexistenteDaListaVacia(String name, Function<Path, InvertedIndex> factory) {
        try (InvertedIndex index = create(factory)) {
            assertEquals(List.of(), index.postings("whale"));
            addPaperBooks(index);
            assertEquals(List.of(), index.postings("whale"));
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexes")
    void postingsNoSePuedeModificarDesdeFuera(String name, Function<Path, InvertedIndex> factory) {
        try (InvertedIndex index = create(factory)) {
            addPaperBooks(index);
            assertThrows(UnsupportedOperationException.class, () -> index.postings("boat").add(99));
            assertEquals(List.of(10, 20), index.postings("boat"));
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexes")
    void seVeLoAnadidoAntesDelFlush(String name, Function<Path, InvertedIndex> factory) {
        try (InvertedIndex index = create(factory)) {
            addPaperBooks(index);                            // without flush
            assertEquals(List.of(10, 20), index.postings("boat"));
        }
    }

    // --- flush --------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexes")
    void flushNoCambiaLoQueSeConsulta(String name, Function<Path, InvertedIndex> factory) {
        try (InvertedIndex index = create(factory)) {
            addPaperBooks(index);
            index.flush();
            index.flush();                                   // twice in a row: no strange effects

            assertEquals(List.of(10, 20), index.postings("boat"));
            assertEquals(List.of(10, 30), index.postings("red"));
            assertEquals(List.of(30), index.postings("island"));
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexes")
    void anadirTrasUnFlushNoPierdeLoAnterior(String name, Function<Path, InvertedIndex> factory) {
        try (InvertedIndex index = create(factory)) {
            index.addDocument(10, Set.of("red", "boat"));
            index.flush();
            index.addDocument(20, Set.of("boat", "blue"));
            index.flush();

            assertEquals(List.of(10), index.postings("red"));
            assertEquals(List.of(10, 20), index.postings("boat"));
            assertEquals(List.of(20), index.postings("blue"));
        }
    }

    // --- clear --------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexes")
    void clearDejaElIndiceVacioYReutilizable(String name, Function<Path, InvertedIndex> factory) {
        try (InvertedIndex index = create(factory)) {
            addPaperBooks(index);
            index.flush();

            index.clear();
            assertEquals(List.of(), index.postings("boat"));
            assertEquals(0, index.diskUsageBytes());

            index.addDocument(99, Set.of("boat"));           // it can be used again
            assertEquals(List.of(99), index.postings("boat"));
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexes")
    void clearPermiteRepetirUnaMedidaConElMismoResultado(String name, Function<Path, InvertedIndex> factory) {
        try (InvertedIndex index = create(factory)) {
            addPaperBooks(index);
            index.flush();
            List<Integer> first = index.postings("boat");
            long firstSize = index.diskUsageBytes();

            index.clear();                                   // "next benchmark repetition"
            addPaperBooks(index);
            index.flush();

            assertEquals(first, index.postings("boat"));
            assertEquals(firstSize, index.diskUsageBytes());
        }
    }

    // --- diskUsageBytes -----------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexes")
    void diskUsageNuncaEsNegativo(String name, Function<Path, InvertedIndex> factory) {
        try (InvertedIndex index = create(factory)) {
            assertTrue(index.diskUsageBytes() >= 0);
            addPaperBooks(index);
            index.flush();
            assertTrue(index.diskUsageBytes() >= 0);
        }
    }

    // --- Search that only knows the interface -------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexes")
    void lasConsultasDelEjercicioFuncionanSoloConLaInterfaz(String name, Function<Path, InvertedIndex> factory) {
        try (InvertedIndex index = create(factory)) {
            addPaperBooks(index);
            index.flush();

            assertEquals(List.of(10, 20), searchAnd(index, "boat", "sails"));
            assertEquals(List.of(30), searchAnd(index, "red", "island"));
            assertEquals(List.of(10), searchAnd(index, "red", "boat"));
        }
    }
}