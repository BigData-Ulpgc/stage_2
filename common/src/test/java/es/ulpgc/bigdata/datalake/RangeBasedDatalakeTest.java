package es.ulpgc.bigdata.datalake;

import es.ulpgc.bigdata.model.BookIds;
import es.ulpgc.bigdata.model.BookLocation;
import es.ulpgc.bigdata.model.RawBook;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class RangeBasedDatalakeTest {

    @TempDir Path tmp;
    Datalake datalake;          // used as an interface, not as a concrete class (challenge 7)
    Path rangeRoot;

    /** 10:15:00.789: the marker keeps the second only. */
    static final Instant T1 = Instant.parse("2026-11-04T10:15:00.789Z");
    static final Instant T2 = Instant.parse("2026-11-05T08:00:00Z");

    @BeforeEach
    void setUp() {
        rangeRoot = tmp.resolve("range");
        datalake = new RangeBasedDatalake(rangeRoot, Clock.fixed(T1, ZoneOffset.UTC));
    }

    private Path ready(int id) {
        return rangeRoot.resolve(RangeBasedDatalake.rangeFolder(id)).resolve(id + "._READY");
    }

    /** Header and body without marker: a save that died before the marker, or a pre-DR-06 datalake. */
    private void writeWithoutMarker(int id, String header, String body) throws IOException {
        Path dir = Files.createDirectories(rangeRoot.resolve(RangeBasedDatalake.rangeFolder(id)));
        Files.writeString(dir.resolve(id + ".header.txt"), header);
        Files.writeString(dir.resolve(id + ".body.txt"), body);
    }

    private List<String> filesIn(String rangeFolder) throws IOException {
        try (Stream<Path> s = Files.list(rangeRoot.resolve(rangeFolder))) {
            return s.map(p -> p.getFileName().toString()).sorted().toList();
        }
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

    // ------------------------------------------------------------------
    // The ids are those of BookIds (1 to 999,999,999), as in the APIs
    // ------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(ints = {-5, 0, BookIds.MAX + 1, Integer.MAX_VALUE})
    void saveRechazaLosIdsFueraDeBookIdsSinEscribirNada(int id) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> datalake.save(new RawBook(id, "H", "B")));

        assertTrue(e.getMessage().contains(String.valueOf(id)), e.getMessage());
        assertFalse(Files.exists(rangeRoot), "no crea ni la carpeta");
    }

    @Test
    void elMayorIdValidoSeGuardaYSeLista() {
        datalake.save(new RawBook(BookIds.MAX, "H", "B"));

        assertTrue(datalake.locate(BookIds.MAX).isPresent());
        assertEquals(List.of(BookIds.MAX), datalake.listBookIds());
    }

    @Test
    void elIdUnoSeGuardaYSeLista() {
        datalake.save(new RawBook(1, "H", "B"));

        assertEquals(List.of(1), datalake.listBookIds());
    }

    @ParameterizedTest
    @ValueSource(ints = {-5, 0, BookIds.MAX + 1})
    void locateDeUnIdFueraDeBookIdsNoEncuentraNada(int id) {
        assertTrue(datalake.locate(id).isEmpty());
    }

    @Test
    void unLibroConId0PuestoAManoNoSeLista() throws IOException {
        // A datalake written before this change could have one: it is not a valid book.
        datalake.save(new RawBook(84, "H", "B"));
        Path dir = rangeRoot.resolve("00000-00999");
        Files.writeString(dir.resolve("0.header.txt"), "x");
        Files.writeString(dir.resolve("0.body.txt"), "x");
        Files.writeString(dir.resolve("0._READY"), "x");

        assertEquals(List.of(84), datalake.listBookIds());
        assertTrue(datalake.locate(0).isEmpty());
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
        Files.writeString(dir.resolve("abc._READY"), "x");
        Files.writeString(dir.resolve("1600.body.txt.tmp"), "x");
        Files.writeString(dir.resolve("1600._READY.tmp"), "x");
        Files.writeString(dir.resolve("1600._READY.3f2b9c1e-8a4d-4f6e-9b7a-2c5d8e1f0a3b.tmp"), "x");
        Files.writeString(dir.resolve("1342._READY.3f2b9c1e-8a4d-4f6e-9b7a-2c5d8e1f0a3b.tmp"), "x");
        Files.writeString(dir.resolve("01342.body.txt"), "x");
        Files.writeString(dir.resolve("01342._READY"), "x");
        Files.writeString(dir.resolve("5000.body.txt"), "x");    // wrong range, even with its marker
        Files.writeString(dir.resolve("5000.header.txt"), "x");
        Files.writeString(dir.resolve("5000._READY"), "x");
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
            Files.writeString(dir.resolve(id + "._READY"), "x");
        }

        assertEquals(List.of(84), datalake.listBookIds());
    }

    // ------------------------------------------------------------------
    // The ready marker (contract §6.2, DR-06)
    // ------------------------------------------------------------------

    @Test
    void saveEscribeElMarcadorConLaFechaUtcAlSegundo() throws IOException {
        datalake.save(new RawBook(1342, "H", "B"));

        assertEquals(List.of("1342._READY", "1342.body.txt", "1342.header.txt"), filesIn("01000-01999"));
        assertEquals("2026-11-04T10:15:00Z\n", Files.readString(ready(1342), StandardCharsets.UTF_8));
    }

    @Test
    void sinMarcadorElLibroNoExiste() throws IOException {
        // A save that died after the body, or a datalake written before DR-06.
        writeWithoutMarker(1342, "H", "B");

        assertTrue(datalake.locate(1342).isEmpty());
        assertEquals(List.of(), datalake.listBookIds());
    }

    @Test
    void unLibroSinMarcadorSeVuelveAGuardarEntero() throws IOException {
        writeWithoutMarker(1342, "H vieja", "B a medias");

        datalake.save(new RawBook(1342, "H", "B"));

        BookLocation loc = datalake.locate(1342).orElseThrow();
        assertEquals("H", Files.readString(loc.headerPath(), StandardCharsets.UTF_8));
        assertEquals("B", Files.readString(loc.bodyPath(), StandardCharsets.UTF_8));
        assertEquals(List.of(1342), datalake.listBookIds());
    }

    @Test
    void unMarcadorSinCuerpoNoEsUnLibroYSaveLoRepara() throws IOException {
        BookLocation loc = datalake.save(new RawBook(1342, "H", "B"));
        Files.delete(loc.bodyPath());

        assertTrue(datalake.locate(1342).isEmpty());
        assertEquals(List.of(), datalake.listBookIds());

        datalake.save(new RawBook(1342, "H", "B"));
        assertEquals("B", Files.readString(datalake.locate(1342).orElseThrow().bodyPath(), StandardCharsets.UTF_8));
    }

    /** Runs before each file of a book is written. */
    private interface FileHook {
        void beforeWriting(Path target) throws IOException;
    }

    /** A datalake on the same folder that calls hook before writing each file. */
    private Datalake hooked(FileHook hook) {
        return new RangeBasedDatalake(rangeRoot, Clock.fixed(T2, ZoneOffset.UTC)) {
            @Override
            protected void writeFile(Path target, String content) throws IOException {
                hook.beforeWriting(target);
                super.writeFile(target, content);
            }
        };
    }

    @Test
    void alRepararUnLibroDanadoBorraElMarcadorViejoAntesDeEscribirNada() throws IOException {
        BookLocation loc = datalake.save(new RawBook(1342, "H", "B"));
        Files.delete(loc.headerPath());                       // damaged: marker and body, no header
        List<String> seen = new ArrayList<>();
        Datalake watching = hooked(target -> seen.add(
                target.getFileName() + (Files.exists(ready(1342)) ? " con marcador" : " sin marcador")));

        watching.save(new RawBook(1342, "H2", "B2"));

        assertEquals(List.of("1342.header.txt sin marcador",
                             "1342.body.txt sin marcador",
                             "1342._READY sin marcador"), seen,
                "mientras se reescribe, el libro no puede contar como completo");
        BookLocation repaired = datalake.locate(1342).orElseThrow();
        assertEquals("H2", Files.readString(repaired.headerPath(), StandardCharsets.UTF_8));
        assertEquals("B2", Files.readString(repaired.bodyPath(), StandardCharsets.UTF_8));
        assertEquals("2026-11-05T08:00:00Z\n", Files.readString(ready(1342), StandardCharsets.UTF_8));
    }

    @Test
    void siLaReparacionSeCortaAMitadElLibroNoQuedaMezclado() throws IOException {
        // Review of #24 by Carlos: with the old marker in place, the new header plus the old
        // body would count as a complete book, and no later save would repair it.
        BookLocation loc = datalake.save(new RawBook(1342, "H", "B"));
        Files.delete(loc.headerPath());
        Datalake dying = hooked(target -> {
            if (target.getFileName().toString().endsWith(".body.txt")) {
                throw new IOException("el proceso muere aquí");
            }
        });

        assertThrows(UncheckedIOException.class, () -> dying.save(new RawBook(1342, "H2", "B2")));

        assertTrue(datalake.locate(1342).isEmpty(), "cabecera nueva y cuerpo viejo no son un libro");
        assertEquals(List.of(), datalake.listBookIds());

        datalake.save(new RawBook(1342, "H2", "B2"));         // the retry repairs it
        BookLocation repaired = datalake.locate(1342).orElseThrow();
        assertEquals("H2", Files.readString(repaired.headerPath(), StandardCharsets.UTF_8));
        assertEquals("B2", Files.readString(repaired.bodyPath(), StandardCharsets.UTF_8));
    }

    @Test
    void unMarcadorSoloNoEsUnLibro() throws IOException {
        Path dir = Files.createDirectories(rangeRoot.resolve("01000-01999"));
        Files.writeString(dir.resolve("1342._READY"), "2026-11-04T10:15:00Z\n");

        assertTrue(datalake.locate(1342).isEmpty());
        assertEquals(List.of(), datalake.listBookIds());
    }

    @Test
    void unLibroGuardadoNoSeModifica() throws IOException {
        BookLocation first = datalake.save(new RawBook(1342, "H", "B"));
        Datalake later = new RangeBasedDatalake(rangeRoot, Clock.fixed(T2, ZoneOffset.UTC));

        BookLocation second = later.save(new RawBook(1342, "H2", "B2"));

        assertEquals(first, second, "devuelve dónde ya estaba");
        assertEquals("H", Files.readString(first.headerPath(), StandardCharsets.UTF_8));
        assertEquals("B", Files.readString(first.bodyPath(), StandardCharsets.UTF_8));
        assertEquals("2026-11-04T10:15:00Z\n", Files.readString(ready(1342), StandardCharsets.UTF_8),
                "ni siquiera la fecha del marcador");
    }

    @Test
    void siFallaElCuerpoNoSeEscribeElMarcador() throws IOException {
        // The body path is a folder with content: moving the body there fails.
        Path body = Files.createDirectories(rangeRoot.resolve("01000-01999/1342.body.txt"));
        Files.writeString(body.resolve("dentro.txt"), "x");

        assertThrows(UncheckedIOException.class, () -> datalake.save(new RawBook(1342, "H", "B")));

        assertFalse(Files.exists(ready(1342)), "el marcador va el último");
        assertTrue(datalake.locate(1342).isEmpty());
    }

    @Test
    void siFallaLaCabeceraNoSeEscribeNadaMas() throws IOException {
        Path header = Files.createDirectories(rangeRoot.resolve("01000-01999/1342.header.txt"));
        Files.writeString(header.resolve("dentro.txt"), "x");

        assertThrows(UncheckedIOException.class, () -> datalake.save(new RawBook(1342, "H", "B")));

        assertFalse(Files.exists(rangeRoot.resolve("01000-01999/1342.body.txt")));
        assertFalse(Files.exists(ready(1342)));
    }

    @Test
    void variosSaveDelMismoLibroALaVezDejanUnLibroEntero() throws Exception {
        // A manual POST /ingest and a worker, or a worker that lost its lease and the next one:
        // the same book saved from several threads at once.
        int threads = 8;
        String body = "It is a truth universally acknowledged. ".repeat(20_000);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<BookLocation>> saves = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            saves.add(pool.submit(() -> {
                start.await();
                return datalake.save(new RawBook(1342, "Title: Pride", body));
            }));
        }
        start.countDown();
        try {
            for (Future<BookLocation> f : saves) {
                assertEquals(rangeRoot.resolve("01000-01999/1342.body.txt"),
                        f.get(30, TimeUnit.SECONDS).bodyPath());          // none fails
            }
        } finally {
            pool.shutdown();
        }

        BookLocation loc = datalake.locate(1342).orElseThrow();
        assertEquals(body, Files.readString(loc.bodyPath(), StandardCharsets.UTF_8));
        assertEquals(List.of(1342), datalake.listBookIds());
        assertEquals(List.of("1342._READY", "1342.body.txt", "1342.header.txt"), filesIn("01000-01999"),
                "ningún .tmp");
    }
}
