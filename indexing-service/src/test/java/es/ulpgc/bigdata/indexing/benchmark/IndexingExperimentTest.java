package es.ulpgc.bigdata.indexing.benchmark;

import com.hazelcast.core.HazelcastInstance;
import es.ulpgc.bigdata.config.Env;
import es.ulpgc.bigdata.coordination.BookState;
import es.ulpgc.bigdata.coordination.Claim;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.Stage;
import es.ulpgc.bigdata.testsupport.HazelcastTestCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The driver of DR-13 against the embedded Hazelcast cluster of common's tests. */
class IndexingExperimentTest {

    private static final Duration POLL = Duration.ofMillis(10);

    @TempDir Path dir;

    private static HazelcastInstance client;
    private Coordination coordination;

    @BeforeAll
    static void connect() {
        client = HazelcastTestCluster.settings().openClient();
    }

    @AfterAll
    static void disconnect() {
        client.shutdown();
    }

    @BeforeEach
    void setUp() {
        HazelcastTestCluster.reset();
        coordination = Coordination.on(client, HazelcastTestCluster.settings(), Clock.systemUTC());
    }

    @Test
    void seedPoneLosIdsConsecutivosEnIngestQueue() {
        assertEquals(5, IndexingExperiment.seed(coordination, 1_000_001, 5));
        assertEquals(5, coordination.queueSize(Stage.INGESTION));
        assertEquals(5, coordination.count(BookState.PENDING));
        assertTrue(coordination.task(1_000_005).isPresent());
        assertFalse(coordination.task(1_000_006).isPresent(), "ni uno más");
        assertEquals(0, IndexingExperiment.seed(coordination, 1_000_001, 5), "sembrar otra vez no duplica");
    }

    @Test
    void waitTerminaCuandoHayBastantesEnEseEstado() throws Exception {
        IndexingExperiment.seed(coordination, 1_000_001, 3);
        Thread ingestion = new Thread(() -> {
            try {
                for (int i = 0; i < 3; i++) {
                    Claim claim = coordination.claim(Stage.INGESTION, "ingestion-test/w0", Duration.ofSeconds(5)).orElseThrow();
                    Thread.sleep(50);
                    coordination.complete(claim);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        ingestion.start();
        assertTrue(IndexingExperiment.waitFor(coordination, List.of(BookState.DOWNLOADED), 3, Duration.ofSeconds(10), POLL));
        assertEquals(3, coordination.count(BookState.DOWNLOADED));
        ingestion.join();
    }

    @Test
    void waitSeRindeAlAcabarseElTiempo() throws Exception {
        IndexingExperiment.seed(coordination, 1_000_001, 3);
        assertFalse(IndexingExperiment.waitFor(coordination, List.of(BookState.DOWNLOADED), 3, Duration.ofMillis(100), POLL));
    }

    @Test
    void waitSumaVariosEstados() throws Exception {
        IndexingExperiment.seed(coordination, 1_000_001, 3);
        Claim claim = coordination.claim(Stage.INGESTION, "ingestion-test/w0", Duration.ofSeconds(5)).orElseThrow();
        coordination.complete(claim);
        assertEquals(List.of(BookState.PENDING, BookState.DOWNLOADED), IndexingExperiment.states("PENDING, DOWNLOADED"));
        assertTrue(IndexingExperiment.waitFor(coordination, IndexingExperiment.states("PENDING,DOWNLOADED"), 3,
                Duration.ofMillis(500), POLL), "2 PENDING + 1 DOWNLOADED");
        assertFalse(IndexingExperiment.waitFor(coordination, IndexingExperiment.states("DOWNLOADED"), 3,
                Duration.ofMillis(100), POLL));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> IndexingExperiment.states("INDEXD"));
        assertTrue(e.getMessage().contains("INDEXD"));
    }

    @Test
    void measureEmpiezaACronometrarAntesDeArrancarYParaAlLlegar() throws Exception {
        AtomicLong indexed = new AtomicLong();
        AtomicBoolean started = new AtomicBoolean();
        Thread writer = new Thread(() -> {
            try {
                while (!started.get()) {
                    Thread.sleep(1);
                }
                for (int i = 0; i < 10; i++) {
                    Thread.sleep(20);
                    indexed.addAndGet(10);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        writer.start();

        // The start takes 150 ms, as docker compose up does: it counts in the time of the run
        IndexingExperiment.Measurement m = IndexingExperiment.measure(indexed::get, () -> 0, 100,
                Duration.ofSeconds(10), POLL, () -> {
                    try {
                        Thread.sleep(150);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    started.set(true);
                }, sample -> { });
        writer.join();

        assertTrue(m.complete());
        assertEquals(100, m.indexed());
        assertTrue(m.progress().samples().get(0).seconds() >= 0.15, "el arranque ya cuenta en la primera muestra");
        assertTrue(m.progress().secondsToReach(1).getAsDouble() >= 0.17, "arranque y el primer lote");
        assertTrue(m.totalSeconds() >= 0.35, "150 ms de arranque y 10 lotes de 20 ms");
        String[] summary = m.summary().split(",", -1);
        assertEquals(IndexingExperiment.SUMMARY_HEADER.split(",").length, summary.length);
        assertEquals("100", summary[0]);
        assertEquals("0", summary[1]);
        assertFalse(summary[5].isEmpty(), "hay ritmo estable");
    }

    @Test
    void elResumenDaElArranqueElTotalYElRitmoEstable() {
        Progress p = new Progress();
        for (int s = 0; s <= 5; s++) {
            p.add(s, 0);                                // 5 s of start-up
        }
        for (int s = 6; s <= 15; s++) {
            p.add(s, (s - 5) * 10L);                    // then 10 books/s
        }
        assertEquals("100,0,6.000,15.000,6.667,10.000",
                new IndexingExperiment.Measurement(100, p, 0, true).summary(),
                "libros, fallidos, primer libro, total, ritmo total (con el arranque) y ritmo estable (10-90 %)");
    }

    @Test
    void measureCuentaLosFallidosParaNoEsperarParaSiempre() throws Exception {
        IndexingExperiment.Measurement m = IndexingExperiment.measure(() -> 98, () -> 2, 100,
                Duration.ofSeconds(10), POLL, () -> { }, sample -> { });
        assertTrue(m.complete());
        assertEquals(2, m.failed());
        assertTrue(m.summary().startsWith("98,2,"));
    }

    @Test
    void measureSeRindeAlAcabarseElTiempo() throws Exception {
        IndexingExperiment.Measurement m = IndexingExperiment.measure(() -> 40, () -> 0, 100,
                Duration.ofMillis(100), POLL, () -> { }, sample -> { });
        assertFalse(m.complete());
        String[] summary = m.summary().split(",", -1);
        assertEquals("40", summary[0]);
        assertEquals("", summary[5], "sin llegar al 90 % no hay ritmo estable");
    }

    @Test
    void cadaMuestraSeEscribeEnCuantoSeToma() throws Exception {
        Path out = dir.resolve("timeline.csv");
        try (IndexingExperiment.Timeline timeline = new IndexingExperiment.Timeline(out)) {
            IndexingExperiment.measure(new AtomicLong(0)::getAndIncrement, () -> 0, 3, Duration.ofSeconds(10), POLL,
                    () -> { }, sample -> {
                        timeline.accept(sample);
                        try {
                            assertEquals(sample.books() + 2, Files.readAllLines(out).size(),
                                    "ya está en el fichero antes de la siguiente");
                        } catch (java.io.IOException e) {
                            throw new java.io.UncheckedIOException(e);
                        }
                    });
        }
        List<String> lines = Files.readAllLines(out);
        assertEquals("seconds,books_indexed", lines.get(0));
        assertEquals(List.of("0", "1", "2", "3"), lines.stream().skip(1).map(l -> l.split(",")[1]).toList());
    }

    @Test
    void laMedianaDeLosTiemposDeLectura() {
        assertEquals(2.0, IndexingExperiment.median(new double[] {3, 1, 2}));
        assertEquals(2.5, IndexingExperiment.median(new double[] {4, 1, 2, 3}));
    }

    /** Without any cluster: a bad argument fails before connecting, never waits for Hazelcast. */
    @Test
    @Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)  // fails even if it hangs
    void losArgumentosMalosSeRechazanSinConectar() {
        Env env = Env.of(Map.of());
        PrintStream out = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
        assertThrows(IllegalArgumentException.class, () -> IndexingExperiment.run(new String[0], env, out));
        assertThrows(IllegalArgumentException.class, () -> IndexingExperiment.run(new String[] {"nada"}, env, out));
        assertThrows(IllegalArgumentException.class, () -> IndexingExperiment.run(new String[] {"seed", "1"}, env, out));
        assertThrows(IllegalArgumentException.class, () -> IndexingExperiment.run(new String[] {"seed", "1", "-3"}, env, out));
        assertThrows(IllegalArgumentException.class,
                () -> IndexingExperiment.run(new String[] {"wait", "INDEXED", "0", "60"}, env, out));
        assertThrows(IllegalArgumentException.class,
                () -> IndexingExperiment.run(new String[] {"wait", "INDEXED", "10", "x"}, env, out));
        assertThrows(IllegalArgumentException.class,
                () -> IndexingExperiment.run(new String[] {"measure", "-1", "60", "out.csv", "--", "true"}, env, out));
        assertThrows(IllegalArgumentException.class,
                () -> IndexingExperiment.run(new String[] {"measure", "10", "60", "out.csv"}, env, out), "sin comando");
        assertThrows(IllegalArgumentException.class,
                () -> IndexingExperiment.run(new String[] {"measure", "10", "60", "out.csv", "--"}, env, out), "comando vacío");
    }
}
