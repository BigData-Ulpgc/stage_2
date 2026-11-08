package es.ulpgc.bigdata.ingestion.benchmark;

import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.model.RawBook;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * RangeBasedDatalake.save of the 100 books of a round, already split, into an empty datalake,
 * with 1, 2, 4 and 8 threads: what the K workers of a replica (INGESTION_WORKERS) write. Each
 * save is three files (header, body, then the _READY marker), each through a temporary file
 * and an atomic move. The ids are consecutive, so the 100 books go to the same range folder,
 * as they do in the service. The score is the mean time per book.
 *
 * Every invocation writes into a new folder and deletes it afterwards (not measured), so a
 * save never finds its book already there, which would write nothing. Nothing is fsync'ed (the
 * datalake does not, datalake-readiness.md §5): this is the cost up to the page cache of the OS,
 * not of the disk itself.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
@Threads(1)
@State(Scope.Benchmark)
public class DatalakeWriteBenchmark {

    @Param({"1", "2", "4", "8"})
    public int threads;

    private List<RawBook> books;
    private Path work;
    private ExecutorService pool;
    private Path dir;
    private RangeBasedDatalake datalake;
    private int saved;

    @Setup(Level.Trial)
    public void setUp() {
        setUp(BenchmarkBooks.booksDir(), BenchmarkBooks.workDir());
    }

    void setUp(Path booksDir, Path workDir) {
        books = BenchmarkBooks.batch(BenchmarkBooks.load(booksDir)).stream().map(BenchmarkBooks::split).toList();
        work = workDir;
        pool = Executors.newFixedThreadPool(threads);
    }

    @Setup(Level.Invocation)
    public void emptyDatalake() throws IOException {
        dir = BenchmarkBooks.newDatalakeDir(work);
        datalake = new RangeBasedDatalake(dir);
    }

    @Benchmark
    @OperationsPerInvocation(BenchmarkBooks.BATCH)
    public int saveRound() throws Exception {
        saved = BenchmarkBooks.onPool(pool, books, book -> datalake.save(book) != null);
        return saved;
    }

    /** Every book of the round must be READY: otherwise the time measured something else. */
    @TearDown(Level.Invocation)
    public void checkAndDelete() throws IOException {
        try {
            int ready = datalake.listBookIds().size();
            if (saved != books.size() || ready != books.size()) {
                throw new IllegalStateException("saved " + saved + " and " + ready + " READY of " + books.size());
            }
        } finally {
            BenchmarkBooks.delete(dir);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        pool.shutdownNow();
    }
}
