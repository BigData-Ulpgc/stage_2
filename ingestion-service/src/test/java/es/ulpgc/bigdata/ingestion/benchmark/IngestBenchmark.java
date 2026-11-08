package es.ulpgc.bigdata.ingestion.benchmark;

import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.ingestion.BookSplitter;
import es.ulpgc.bigdata.ingestion.IngestionService;
import es.ulpgc.bigdata.ingestion.source.LocalFileSource;
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
 * IngestionService.ingest of the 100 books of a round, with 1, 2, 4 and 8 threads: the work of
 * a worker on each book it claims (CF-08), from the local source into an empty datalake. That is
 * read the raw file, split it, check that the book is not there yet, and save it. The score is
 * the mean time per book, so 1e6 / score is books per second of one replica without the network
 * of Gutenberg and without Hazelcast (CoordinationBenchmark measures that part).
 *
 * The raw files are read from the page cache (the same 100 books every time), as in the
 * Indexing benchmarks. Every invocation writes into a new datalake and deletes it afterwards
 * (not measured), so every ingest is a DOWNLOADED, never an ALREADY_AVAILABLE.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
@Threads(1)
@State(Scope.Benchmark)
public class IngestBenchmark {

    @Param({"1", "2", "4", "8"})
    public int threads;

    private List<Integer> ids;
    private LocalFileSource source;
    private Path work;
    private ExecutorService pool;
    private Path dir;
    private IngestionService service;
    private int downloaded;

    @Setup(Level.Trial)
    public void setUp() {
        setUp(BenchmarkBooks.booksDir(), BenchmarkBooks.workDir());
    }

    void setUp(Path booksDir, Path workDir) {
        List<BenchmarkBooks.RawFile> batch = BenchmarkBooks.batch(BenchmarkBooks.load(booksDir));
        batch.forEach(BenchmarkBooks::split);           // every one has its markers
        ids = batch.stream().map(BenchmarkBooks.RawFile::id).toList();
        source = new LocalFileSource(booksDir);
        work = workDir;
        pool = Executors.newFixedThreadPool(threads);
    }

    @Setup(Level.Invocation)
    public void emptyDatalake() throws IOException {
        dir = BenchmarkBooks.newDatalakeDir(work);
        service = new IngestionService(source, new BookSplitter(), new RangeBasedDatalake(dir));
    }

    @Benchmark
    @OperationsPerInvocation(BenchmarkBooks.BATCH)
    public int ingestRound() throws Exception {
        downloaded = BenchmarkBooks.onPool(pool, ids,
                id -> service.ingest(id).outcome() == IngestionService.Outcome.DOWNLOADED);
        return downloaded;
    }

    /** Every book must have been downloaded now and be in the datalake. */
    @TearDown(Level.Invocation)
    public void checkAndDelete() throws IOException {
        try {
            if (downloaded != ids.size() || !service.list().equals(ids)) {
                throw new IllegalStateException("downloaded " + downloaded + " of " + ids.size()
                        + ", in the datalake " + service.list().size());
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
