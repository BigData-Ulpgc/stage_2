package es.ulpgc.bigdata.datalake;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    void unTmpAbandonadoPorUnaEjecucionAnteriorNoMolesta() throws IOException {
        // A process killed with kill -9 leaves its temporary file. Each write uses a new name,
        // so it is neither reused nor deleted: it stays, and the datalakes ignore any .tmp.
        Path target = tmp.resolve("1342.body.txt");
        Files.writeString(tmp.resolve("1342.body.txt.tmp"), "restos de un proceso que murió");

        AbstractFileDatalake.writeAtomically(target, "bueno");

        assertEquals("bueno", Files.readString(target, StandardCharsets.UTF_8));
        assertEquals(List.of("1342.body.txt", "1342.body.txt.tmp"), namesIn(tmp));
    }

    @Test
    void elTmpEsUnicoYConLosPermisosNormales() throws IOException {
        Path target = tmp.resolve("1342.body.txt");
        AbstractFileDatalake.writeAtomically(target, "uno");
        Set<PosixFilePermission> normal = Files.getPosixFilePermissions(target);

        AbstractFileDatalake.writeAtomically(target, "dos");

        // Not rw------- (Files.createTempFile): Indexing reads from another container.
        assertEquals(normal, Files.getPosixFilePermissions(target));
        assertTrue(normal.contains(PosixFilePermission.GROUP_READ) || normal.contains(PosixFilePermission.OTHERS_READ),
                "permisos " + PosixFilePermissions.toString(normal));
    }

    @Test
    void escriturasConcurrentesDelMismoFicheroNuncaDejanUnoAMedias() throws Exception {
        // Several writers of the same file, each with its own complete content, and a reader
        // that keeps checking it. With a fixed temporary name a writer could truncate the
        // temporary file another one is moving, and the target would be half-written.
        Path target = tmp.resolve("1342.body.txt");
        int writers = 8;
        int rounds = 40;
        List<String> contents = new ArrayList<>();
        for (int w = 0; w < writers; w++) {
            contents.add(String.valueOf((char) ('a' + w)).repeat(200_000));
        }
        AbstractFileDatalake.writeAtomically(target, contents.get(0));

        ExecutorService pool = Executors.newFixedThreadPool(writers + 1);
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean writing = new AtomicBoolean(true);
        List<Future<?>> futures = new ArrayList<>();
        for (int w = 0; w < writers; w++) {
            String content = contents.get(w);
            futures.add(pool.submit(() -> {
                start.await();
                for (int r = 0; r < rounds; r++) {
                    AbstractFileDatalake.writeAtomically(target, content);
                }
                return null;
            }));
        }
        Future<Integer> reader = pool.submit(() -> {
            start.await();
            int reads = 0;
            while (writing.get()) {
                String seen = Files.readString(target, StandardCharsets.UTF_8);
                assertTrue(contents.contains(seen), "contenido a medias: " + seen.length() + " caracteres");
                reads++;
            }
            return reads;
        });
        start.countDown();
        try {
            for (Future<?> f : futures) {
                f.get(60, TimeUnit.SECONDS);               // rethrows any failure of a writer
            }
        } finally {
            writing.set(false);
            pool.shutdown();
        }
        assertTrue(reader.get(10, TimeUnit.SECONDS) > 0);
        assertTrue(contents.contains(Files.readString(target, StandardCharsets.UTF_8)));
        assertEquals(List.of("1342.body.txt"), namesIn(tmp), "ningún .tmp tras escrituras que acabaron bien");
    }

    @Test
    void siFallaElMovimientoNoDejaTmpNiTocaElDestino() throws IOException {
        // The target is a folder with content: the final move has to fail.
        Path target = tmp.resolve("1342.body.txt");
        Files.createDirectories(target);
        Files.writeString(target.resolve("dentro.txt"), "x");

        assertThrows(IOException.class,
                () -> AbstractFileDatalake.writeAtomically(target, "texto"));

        assertEquals(List.of("1342.body.txt"), namesIn(tmp), "ningún .tmp: se borra si el move falla");
        assertTrue(Files.isRegularFile(target.resolve("dentro.txt")));
    }
}