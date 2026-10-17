package es.ulpgc.bigdata.datalake;

import es.ulpgc.bigdata.model.BookLocation;
import es.ulpgc.bigdata.model.RawBook;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that any Datalake must pass, whatever its physical organisation.
 * Stage 1 ran each test for book, range and time; Stage 2 only keeps "range" (folders of
 * 1000 ids, so no directory grows with the corpus), but the tests stay parameterized.
 */
class DatalakeContractTest {

    @TempDir Path tmp;

    /** The implementations: expected name + recipe to create a new one. */
    static Stream<Arguments> structures() {
        return Stream.of(
                Arguments.of("range", (Function<Path, Datalake>) RangeBasedDatalake::new));
    }

    /** New, empty datalake in its own temporary subfolder. */
    private Datalake create(String name, Function<Path, Datalake> factory) {
        return factory.apply(tmp.resolve(name));
    }

    // ------------------------------------------------------------------
    // Your challenge 7 tests, now for the three structures
    // ------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("structures")
    void cumpleElContratoDatalake(String name, Function<Path, Datalake> factory) {
        // Type declared as Datalake, not as the concrete class:
        // the test does not know (or care) which implementation it is.
        Datalake datalake = create(name, factory);

        assertEquals(name, datalake.name());

        BookLocation location = datalake.save(new RawBook(10, "h", "b"));
        assertNotNull(location);

        Optional<BookLocation> found = datalake.locate(10);
        assertTrue(found.isPresent());

        assertEquals(List.of(10), datalake.listBookIds());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("structures")
    void listBookIdsIgnoraCarpetasSinLibroValido(String name, Function<Path, Datalake> factory)
            throws IOException {
        Datalake datalake = create(name, factory);
        datalake.save(new RawBook(10, "h", "b"));

        // garbage folder: it exists but has no book at all
        Files.createDirectories(tmp.resolve(name).resolve("basura"));

        assertEquals(List.of(10), datalake.listBookIds());
    }

    // ------------------------------------------------------------------
    // Contract of challenge 10
    // ------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("structures")
    void vacioNoFalla(String name, Function<Path, Datalake> factory) {
        Datalake d = create(name, factory);
        assertEquals(List.of(), d.listBookIds());
        assertTrue(d.locate(1342).isEmpty());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("structures")
    void idaYVueltaConservaElContenido(String name, Function<Path, Datalake> factory) throws IOException {
        Datalake d = create(name, factory);
        d.save(new RawBook(1342, "Title: Pride", "It is a truth… ñ"));

        BookLocation loc = d.locate(1342).orElseThrow();
        assertEquals("Title: Pride", Files.readString(loc.headerPath(), StandardCharsets.UTF_8));
        assertEquals("It is a truth… ñ", Files.readString(loc.bodyPath(), StandardCharsets.UTF_8));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("structures")
    void guardarDosVecesNoDuplicaEnElListado(String name, Function<Path, Datalake> factory) {
        Datalake d = create(name, factory);
        d.save(new RawBook(1342, "H", "B"));
        d.save(new RawBook(1342, "H2", "B2"));
        assertEquals(List.of(1342), d.listBookIds());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("structures")
    void libroSinHeaderDesaparece(String name, Function<Path, Datalake> factory) throws IOException {
        Datalake d = create(name, factory);
        d.save(new RawBook(1, "H", "B"));
        BookLocation loc = d.save(new RawBook(1342, "H", "B"));

        Files.delete(loc.headerPath());                 // simulates a half-done save

        assertTrue(d.locate(1342).isEmpty());
        assertEquals(List.of(1), d.listBookIds());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("structures")
    void ignoraBasura(String name, Function<Path, Datalake> factory) throws IOException {
        Datalake d = create(name, factory);
        BookLocation loc = d.save(new RawBook(1342, "H", "B"));

        // Next to the book: temporary and foreign files. They are placed based on the
        // paths returned by save, so it works for the three structures.
        Files.writeString(Path.of(loc.bodyPath() + ".tmp"), "x");
        Files.writeString(Path.of(loc.headerPath() + ".tmp"), "x");
        Files.writeString(loc.bodyPath().resolveSibling("notas.txt"), "x");

        // At the root: a loose file and a folder with an invalid name that imitates
        // the file names of the three structures.
        Path root = tmp.resolve(name);
        Files.writeString(root.resolve("suelto.body.txt"), "x");
        Path basura = Files.createDirectories(root.resolve("basura"));
        for (String f : List.of("header.txt", "body.txt", "99.header.txt", "99.body.txt")) {
            Files.writeString(basura.resolve(f), "x");
        }

        assertEquals(List.of(1342), d.listBookIds());
        assertTrue(d.locate(99).isEmpty());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("structures")
    void listadoYLocateSonCoherentes(String name, Function<Path, Datalake> factory) throws IOException {
        Datalake d = create(name, factory);
        for (int id = 0; id < 30; id++) {
            d.save(new RawBook(id * 137, "H", "B"));
        }
        Files.delete(d.locate(137).orElseThrow().bodyPath());

        List<Integer> listed = d.listBookIds();
        assertFalse(listed.contains(137));
        for (int id : listed) {
            assertTrue(d.locate(id).isPresent(), name + ": locate debería encontrar " + id);
        }
    }

    // ------------------------------------------------------------------
    // New tests after improving listBookIds
    // ------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("structures")
    void listadoEsInmodificable(String name, Function<Path, Datalake> factory) {
        Datalake d = create(name, factory);

        assertThrows(UnsupportedOperationException.class, () -> d.listBookIds().add(1));

        d.save(new RawBook(84, "H", "B"));
        assertThrows(UnsupportedOperationException.class, () -> d.listBookIds().add(1));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("structures")
    void listaOrdenadaConMuchosLibrosDesordenados(String name, Function<Path, Datalake> factory) {
        Datalake d = create(name, factory);
        List<Integer> ids = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            ids.add(i * 37);                        // spread across several ranges
        }
        Collections.shuffle(ids, new Random(42));   // fixed seed: always the same shuffle
        for (int id : ids) {
            d.save(new RawBook(id, "H", "B"));
        }

        List<Integer> esperado = new ArrayList<>(ids);
        Collections.sort(esperado);
        assertEquals(esperado, d.listBookIds());
    }

    // ------------------------------------------------------------------
    // Challenge 11: interruptions during save
    // ------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("structures")
    void interrupcionAntesDeMoverElBodyNoDejaLibroValido(String name, Function<Path, Datalake> factory)
            throws IOException {
        Datalake d = create(name, factory);
        d.save(new RawBook(1, "H", "B"));
        BookLocation loc = d.save(new RawBook(1342, "H", "B"));

        // Simulates that the process died after writing body.tmp but before moving it:
        // the header is in place and the body only exists as .tmp.
        Files.move(loc.bodyPath(), Path.of(loc.bodyPath() + ".tmp"));

        assertTrue(d.locate(1342).isEmpty());
        assertEquals(List.of(1), d.listBookIds());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("structures")
    void reintentarTrasLaInterrupcionRecuperaElLibro(String name, Function<Path, Datalake> factory)
            throws IOException {
        Datalake d = create(name, factory);
        BookLocation loc = d.save(new RawBook(1342, "H", "B"));
        Files.move(loc.bodyPath(), Path.of(loc.bodyPath() + ".tmp"));

        BookLocation again = d.save(new RawBook(1342, "H", "B definitivo"));

        assertEquals(List.of(1342), d.listBookIds());
        assertEquals("B definitivo",
                Files.readString(d.locate(1342).orElseThrow().bodyPath(), StandardCharsets.UTF_8));
        assertFalse(Files.exists(Path.of(again.bodyPath() + ".tmp")));
    }
}