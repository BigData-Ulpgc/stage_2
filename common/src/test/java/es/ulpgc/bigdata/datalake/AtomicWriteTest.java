package es.ulpgc.bigdata.datalake;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.CopyOption;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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
        String leftover = "1342.body.txt.3f2b9c1e-8a4d-4f6e-9b7a-2c5d8e1f0a3b.tmp";
        Files.writeString(tmp.resolve(leftover), "restos de un proceso que murió");

        AbstractFileDatalake.writeAtomically(target, "bueno");

        assertEquals("bueno", Files.readString(target, StandardCharsets.UTF_8));
        assertEquals(List.of("1342.body.txt", leftover), namesIn(tmp));
    }

    @Test
    void elFicheroEscritoLoPuedenLeerOtrosUsuarios() throws IOException {
        // Windows (NTFS) has no POSIX permissions: there is nothing to check.
        assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"),
                "sin permisos POSIX en este sistema de ficheros");
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

    // ------------------------------------------------------------------
    // moveReplacing: what Windows does is simulated with a fake Mover,
    // because on Linux a rename never gets AccessDeniedException for that reason.
    // ------------------------------------------------------------------

    /** Denies the access the first `denials` times (-1: always), then really moves. */
    private static AbstractFileDatalake.Mover denyingMover(int denials, AtomicInteger calls) {
        return (source, target, options) -> {
            int call = calls.incrementAndGet();
            if (denials < 0 || call <= denials) {
                throw new AccessDeniedException(target.toString());
            }
            Files.move(source, target, options);
        };
    }

    @Test
    void siWindowsDeniegaElAccesoUnMomentoReintentaYMueve() throws IOException {
        Path source = Files.writeString(tmp.resolve("1342.body.txt.x.tmp"), "nuevo");
        Path target = Files.writeString(tmp.resolve("1342.body.txt"), "viejo");
        AtomicInteger calls = new AtomicInteger();

        AbstractFileDatalake.moveReplacing(source, target, denyingMover(2, calls));

        assertEquals(3, calls.get());
        assertEquals("nuevo", Files.readString(target, StandardCharsets.UTF_8));
        assertEquals(List.of("1342.body.txt"), namesIn(tmp));
    }

    @Test
    void siElAccesoSigueDenegadoSeRindeTrasElMaximoDeIntentosEsperandoEntreEllos() throws IOException {
        Path source = Files.writeString(tmp.resolve("1342.body.txt.x.tmp"), "nuevo");
        Path target = Files.writeString(tmp.resolve("1342.body.txt"), "viejo");
        AtomicInteger calls = new AtomicInteger();
        long start = System.nanoTime();

        assertThrows(AccessDeniedException.class,
                () -> AbstractFileDatalake.moveReplacing(source, target, denyingMover(-1, calls)));

        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertEquals(AbstractFileDatalake.MAX_MOVE_ATTEMPTS, calls.get());
        assertTrue(elapsedMs >= 1_000, "pausas de 1+2+…+49 ms entre intentos, esperado ≥ 1 s: " + elapsedMs + " ms");
        assertEquals("viejo", Files.readString(target, StandardCharsets.UTF_8));
    }

    @Test
    void writeAtomicallyReintentaElMovimientoSiWindowsDeniegaElAcceso() throws IOException {
        Path target = Files.writeString(tmp.resolve("1342.body.txt"), "viejo");
        AtomicInteger calls = new AtomicInteger();

        AbstractFileDatalake.writeAtomically(target, "nuevo", denyingMover(2, calls));

        assertEquals(3, calls.get());
        assertEquals("nuevo", Files.readString(target, StandardCharsets.UTF_8));
        assertEquals(List.of("1342.body.txt"), namesIn(tmp));
    }

    @Test
    void siTrasLosReintentosSigueDenegadoNoDejaTmpNiTocaElDestino() throws IOException {
        Path target = Files.writeString(tmp.resolve("1342.body.txt"), "viejo");

        assertThrows(AccessDeniedException.class,
                () -> AbstractFileDatalake.writeAtomically(target, "nuevo", denyingMover(-1, new AtomicInteger())));

        assertEquals("viejo", Files.readString(target, StandardCharsets.UTF_8));
        assertEquals(List.of("1342.body.txt"), namesIn(tmp), "ningún .tmp: se borra si el move falla");
    }

    @Test
    void siElDestinoEsUnaCarpetaNoReintenta() throws IOException {
        // On Windows, moving onto a folder gives AccessDeniedException too: waiting will not fix it.
        Path source = Files.writeString(tmp.resolve("x.tmp"), "nuevo");
        Path target = Files.createDirectories(tmp.resolve("1342.body.txt"));
        AtomicInteger calls = new AtomicInteger();

        assertThrows(AccessDeniedException.class,
                () -> AbstractFileDatalake.moveReplacing(source, target, denyingMover(-1, calls)));

        assertEquals(1, calls.get());
    }

    @Test
    void otroErrorDeEntradaSalidaNoSeReintenta() throws IOException {
        Path target = tmp.resolve("1342.body.txt");
        AtomicInteger calls = new AtomicInteger();
        AbstractFileDatalake.Mover failing = (source, t, options) -> {
            calls.incrementAndGet();
            throw new NoSuchFileException(source.toString());
        };

        assertThrows(NoSuchFileException.class,
                () -> AbstractFileDatalake.moveReplacing(tmp.resolve("x.tmp"), target, failing));

        assertEquals(1, calls.get());
    }

    @Test
    void sinMovimientoAtomicoMueveIgualmenteSinEsaOpcion() throws IOException {
        Path source = Files.writeString(tmp.resolve("x.tmp"), "nuevo");
        Path target = Files.writeString(tmp.resolve("1342.body.txt"), "viejo");
        List<Set<CopyOption>> calls = new ArrayList<>();
        AbstractFileDatalake.Mover noAtomic = (s, t, options) -> {
            calls.add(Set.of(options));
            if (calls.get(calls.size() - 1).contains(StandardCopyOption.ATOMIC_MOVE)) {
                throw new AtomicMoveNotSupportedException(s.toString(), t.toString(), "sin soporte");
            }
            Files.move(s, t, options);
        };

        AbstractFileDatalake.moveReplacing(source, target, noAtomic);

        assertEquals(List.of(Set.of(StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING),
                             Set.of(StandardCopyOption.REPLACE_EXISTING)), calls);
        assertEquals("nuevo", Files.readString(target, StandardCharsets.UTF_8));
    }
}