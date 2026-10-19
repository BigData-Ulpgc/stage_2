package es.ulpgc.bigdata.datalake;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/** Tests of AbstractFileDatalake.writeAtomically, without any concrete datalake. */
class AtomicWriteTest {

    @TempDir Path tmp;

    private static List<String> namesIn(Path dir) throws IOException {
        try (Stream<Path> s = Files.list(dir)) {
            return s.map(p -> p.getFileName().toString()).sorted().toList();
        }
    }

    @Test
    void escribeElContenidoYCreaLasCarpetas() throws IOException {
        Path target = tmp.resolve("a/b/c/1342.body.txt");

        AbstractFileDatalake.writeAtomically(target, "It is a truth… ñ");

        assertEquals("It is a truth… ñ", Files.readString(target, StandardCharsets.UTF_8));
    }

    @Test
    void noDejaNingunTmpTrasEscribir() throws IOException {
        Path target = tmp.resolve("1342.body.txt");

        AbstractFileDatalake.writeAtomically(target, "texto");

        assertEquals(List.of("1342.body.txt"), namesIn(tmp));
    }

    @Test
    void sobrescribeElContenidoAnterior() throws IOException {
        Path target = tmp.resolve("1342.body.txt");
        AbstractFileDatalake.writeAtomically(target, "versión 1, bastante más larga");

        AbstractFileDatalake.writeAtomically(target, "v2");

        assertEquals("v2", Files.readString(target, StandardCharsets.UTF_8));
    }

    @Test
    void reemplazaUnTmpAbandonadoPorUnaEjecucionAnterior() throws IOException {
        Path target = tmp.resolve("1342.body.txt");
        Files.writeString(tmp.resolve("1342.body.txt.tmp"), "restos de un proceso que murió");

        AbstractFileDatalake.writeAtomically(target, "bueno");

        assertEquals("bueno", Files.readString(target, StandardCharsets.UTF_8));
        assertEquals(List.of("1342.body.txt"), namesIn(tmp));
    }

    @Test
    void siFallaElMovimientoNoDejaTmpNiTocaElDestino() throws IOException {
        // The target is a folder with content: the final move has to fail.
        Path target = tmp.resolve("1342.body.txt");
        Files.createDirectories(target);
        Files.writeString(target.resolve("dentro.txt"), "x");

        assertThrows(IOException.class,
                () -> AbstractFileDatalake.writeAtomically(target, "texto"));

        assertFalse(Files.exists(tmp.resolve("1342.body.txt.tmp")));
        assertTrue(Files.isRegularFile(target.resolve("dentro.txt")));
    }
}