package es.ulpgc.bigdata.datalake;

import es.ulpgc.bigdata.model.BookLocation;
import es.ulpgc.bigdata.model.RawBook;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RangeBasedDatalakeTest {

    @TempDir Path tmp;
    Datalake datalake;          // used as an interface, not as a concrete class (challenge 7)
    Path rangeRoot;

    @BeforeEach
    void setUp() {
        rangeRoot = tmp.resolve("range");
        datalake = new RangeBasedDatalake(rangeRoot);
    }

    @ParameterizedTest
    @CsvSource({
            "0,    00000-00999",
            "999,  00000-00999",
            "1000, 01000-01999",
            "1342, 01000-01999",
            "1999, 01000-01999",
            "2000, 02000-02999"
    })
    void carpetaDelRango(int id, String esperado) {
        assertEquals(esperado, RangeBasedDatalake.rangeFolder(id));
    }

    @Test
    void idNegativoSeRechaza() {
        assertThrows(IllegalArgumentException.class, () -> RangeBasedDatalake.rangeFolder(-5));
    }

    @Test
    void guardaEnLaCarpetaCorrecta() throws IOException {
        BookLocation loc = datalake.save(new RawBook(1342, "HEADER", "BODY ñ"));

        Path esperado = rangeRoot.resolve("01000-01999").resolve("1342.body.txt");
        assertEquals(esperado, loc.bodyPath());
        assertEquals("BODY ñ", Files.readString(esperado, StandardCharsets.UTF_8));
    }

    @Test
    void locateEncuentraGuardadoYNoInexistente() {
        datalake.save(new RawBook(1342, "H", "B"));
        assertTrue(datalake.locate(1342).isPresent());
        assertTrue(datalake.locate(1343).isEmpty());
    }

    @Test
    void locateSinHeaderNoCuentaComoGuardado() throws IOException {
        Path dir = Files.createDirectories(rangeRoot.resolve("01000-01999"));
        Files.writeString(dir.resolve("1342.body.txt"), "solo body");
        assertTrue(datalake.locate(1342).isEmpty());
        assertEquals(List.of(), datalake.listBookIds());
    }

    @Test
    void librosEnDistintosRangosOrdenados() {
        datalake.save(new RawBook(2000, "H", "B"));
        datalake.save(new RawBook(999, "H", "B"));
        datalake.save(new RawBook(1000, "H", "B"));
        datalake.save(new RawBook(84, "H", "B"));
        assertEquals(List.of(84, 999, 1000, 2000), datalake.listBookIds());
    }

    @Test
    void guardarDosVecesNoDuplica() {
        datalake.save(new RawBook(1342, "H", "B"));
        datalake.save(new RawBook(1342, "H2", "B2"));
        assertEquals(List.of(1342), datalake.listBookIds());
    }

    @Test
    void ignoraBasura() throws IOException {
        datalake.save(new RawBook(1342, "H", "B"));
        Path dir = rangeRoot.resolve("01000-01999");
        Files.writeString(dir.resolve("notas.txt"), "x");
        Files.writeString(dir.resolve("abc.body.txt"), "x");
        Files.writeString(dir.resolve("1600.body.txt.tmp"), "x");
        Files.writeString(dir.resolve("01342.body.txt"), "x");
        Files.writeString(dir.resolve("5000.body.txt"), "x");    // wrong range
        Files.writeString(dir.resolve("5000.header.txt"), "x");
        Files.createDirectories(rangeRoot.resolve("carpeta_rara"));
        Files.writeString(rangeRoot.resolve("suelto.body.txt"), "x");

        assertEquals(List.of(1342), datalake.listBookIds());
    }

    @Test
    void listadoYLocateSonCoherentes() {
        datalake.save(new RawBook(1, "H", "B"));
        datalake.save(new RawBook(1342, "H", "B"));
        for (int id : datalake.listBookIds()) {
            assertTrue(datalake.locate(id).isPresent(), "locate debería encontrar " + id);
        }
    }

    @Test
    void datalakeVacioNoFalla() {
        assertEquals(List.of(), datalake.listBookIds());
    }

    @Test
    void nombresNumericosInvalidosSeIgnoran() throws IOException {
        datalake.save(new RawBook(84, "H", "B"));
        Path dir = rangeRoot.resolve("00000-00999");
        for (String id : List.of("084", "99999999999")) {
            Files.writeString(dir.resolve(id + ".header.txt"), "x");
            Files.writeString(dir.resolve(id + ".body.txt"), "x");
        }

        assertEquals(List.of(84), datalake.listBookIds());
    }
}