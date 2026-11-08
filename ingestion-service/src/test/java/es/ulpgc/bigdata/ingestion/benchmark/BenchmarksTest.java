package es.ulpgc.bigdata.ingestion.benchmark;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * One invocation of each benchmark, outside JMH, on a small dataset with the format of DR-16:
 * the setup, the measured method and the checks after it work, and nothing is left on disk.
 * The JMH run itself is benchmarks/ingestion/run-jmh.sh --smoke.
 */
class BenchmarksTest {

    private static final int FIRST_ID = 1_000_001;
    private static final int BOOKS = BenchmarkBooks.BATCH + 20;

    @TempDir
    Path tmp;
    private Path books;
    private Path work;

    @BeforeEach
    void dataset() throws IOException {
        books = Files.createDirectory(tmp.resolve("books"));
        work = tmp.resolve("work");
        for (int i = 0; i < BOOKS; i++) {
            int id = FIRST_ID + i;
            String body = ("word" + i + " ").repeat(1 + i * 10);
            Files.writeString(books.resolve("pg" + id + ".txt"), "Title: Book " + id + "\r\n\r\n"
                    + "*** START OF THE PROJECT GUTENBERG EBOOK BOOK " + id + " ***\r\n"
                    + body + "\r\n*** END OF THE PROJECT GUTENBERG EBOOK BOOK " + id + " ***\r\nfooter\r\n",
                    StandardCharsets.UTF_8);
        }
        Files.writeString(books.resolve("manifest.json"), "{}");     // not a book: ignored
    }

    @Test
    void theBooksAreChosenByLengthAndTheRoundIsTheFirstHundred() {
        List<BenchmarkBooks.RawFile> all = BenchmarkBooks.load(books);
        assertEquals(BOOKS, all.size());
        // The lengths grow with the id: percentile p is the book at index floor(p * (n - 1))
        assertEquals(FIRST_ID + 11, BenchmarkBooks.byLength(all, "short").id());
        assertEquals(FIRST_ID + 59, BenchmarkBooks.byLength(all, "median").id());
        assertEquals(FIRST_ID + 107, BenchmarkBooks.byLength(all, "long").id());
        List<BenchmarkBooks.RawFile> round = BenchmarkBooks.batch(all);
        assertEquals(BenchmarkBooks.BATCH, round.size());
        assertEquals(FIRST_ID, round.get(0).id());
        assertThrows(IllegalArgumentException.class, () -> BenchmarkBooks.byLength(all, "huge"));
    }

    @Test
    void withoutTheDatasetItSaysHowToGetIt() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> BenchmarkBooks.load(tmp.resolve("missing")));
        assertTrue(e.getMessage().contains("run-jmh.sh"), e.getMessage());
    }

    @Test
    void theSplitterBenchmarkSplitsItsBook() {
        SplitterBenchmark benchmark = new SplitterBenchmark();
        benchmark.book = "median";
        benchmark.setUp(books);
        assertEquals(FIRST_ID + 59, benchmark.split().orElseThrow().id());
    }

    @Test
    void theWriteBenchmarkSavesTheRoundAndDeletesIt() throws Exception {
        DatalakeWriteBenchmark benchmark = new DatalakeWriteBenchmark();
        benchmark.threads = 4;
        benchmark.setUp(books, work);
        try {
            for (int invocation = 0; invocation < 2; invocation++) {
                benchmark.emptyDatalake();
                assertEquals(BenchmarkBooks.BATCH, benchmark.saveRound());
                benchmark.checkAndDelete();
            }
            assertEmpty(work);
        } finally {
            benchmark.tearDown();
        }
    }

    @Test
    void theWriteBenchmarkFailsIfTheRoundWasNotSaved() throws Exception {
        DatalakeWriteBenchmark benchmark = new DatalakeWriteBenchmark();
        benchmark.threads = 1;
        benchmark.setUp(books, work);
        try {
            benchmark.emptyDatalake();
            assertThrows(IllegalStateException.class, benchmark::checkAndDelete);
            assertEmpty(work);                          // deleted all the same
        } finally {
            benchmark.tearDown();
        }
    }

    @Test
    void theIngestBenchmarkDownloadsTheRoundAndDeletesIt() throws Exception {
        IngestBenchmark benchmark = new IngestBenchmark();
        benchmark.threads = 2;
        benchmark.setUp(books, work);
        try {
            for (int invocation = 0; invocation < 2; invocation++) {
                benchmark.emptyDatalake();
                assertEquals(BenchmarkBooks.BATCH, benchmark.ingestRound());
                benchmark.checkAndDelete();
            }
            assertEmpty(work);
        } finally {
            benchmark.tearDown();
        }
    }

    @Test
    void theCoordinationBenchmarkCompletesEveryBookOnce() throws Exception {
        CoordinationBenchmark benchmark = new CoordinationBenchmark();
        benchmark.threads = 4;
        benchmark.setUp();
        try {
            for (int invocation = 0; invocation < 2; invocation++) {
                benchmark.seedRound();
                assertEquals(BenchmarkBooks.BATCH, benchmark.claimAndComplete());
                benchmark.check();
            }
        } finally {
            benchmark.tearDown();
        }
    }

    private static void assertEmpty(Path dir) throws IOException {
        try (Stream<Path> left = Files.list(dir)) {
            assertEquals(List.of(), left.toList());
        }
    }
}
