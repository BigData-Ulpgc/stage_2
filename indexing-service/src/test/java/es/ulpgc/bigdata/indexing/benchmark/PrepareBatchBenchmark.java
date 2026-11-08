package es.ulpgc.bigdata.indexing.benchmark;

import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.datamart.index.InMemoryInvertedIndex;
import es.ulpgc.bigdata.datamart.index.Tokenizer;
import es.ulpgc.bigdata.datamart.metadata.MetadataParser;
import es.ulpgc.bigdata.indexing.BatchResult;
import es.ulpgc.bigdata.indexing.InMemoryIndexLog;
import es.ulpgc.bigdata.indexing.InMemoryMetadataRepository;
import es.ulpgc.bigdata.indexing.InMemoryPublicationRepository;
import es.ulpgc.bigdata.indexing.IndexingService;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * IndexingService.indexBatch of the 100 books of a batch (DR-09), with INDEXING_THREADS = 1, 2,
 * 4 and 8 to prepare them: read from the datalake, parse the header, tokenize the body. The
 * write goes to in-memory stores, new for every batch, so it is the part of the write that runs
 * in the JVM (the pending map, as MongoInvertedIndex fills it before its flush) and none of
 * MongoDB. The score is the mean time per book.
 *
 * The files are read from the page cache (the same 100 books every time): this measures the
 * CPU of preparing a book, not the disk. One benchmark thread calls indexBatch, as the
 * index-queue writer does; the parser threads are the pool of the service.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
@Threads(1)
@State(Scope.Benchmark)
public class PrepareBatchBenchmark {

    @Param({"1", "2", "4", "8"})
    public int threads;

    private final MetadataParser parser = new MetadataParser();
    private final Tokenizer tokenizer = Tokenizer.standard();
    private Path dir;
    private RangeBasedDatalake datalake;
    private List<Integer> ids;
    private ExecutorService parsers;
    private IndexingService service;

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        List<RawBook> books = BenchmarkBooks.batch(BenchmarkBooks.load());
        dir = Files.createTempDirectory("dr12-batch");
        datalake = BenchmarkBooks.datalake(dir, books);
        ids = books.stream().map(RawBook::id).toList();
        parsers = Executors.newFixedThreadPool(threads, runnable -> {
            Thread thread = new Thread(runnable, "bench-parser");
            thread.setDaemon(true);
            return thread;
        });
        newService();
        BatchResult check = service.indexBatch(ids);
        if (check.indexed().size() != ids.size()) {
            throw new IllegalStateException("el lote no se indexa entero: " + check.rejected());
        }
    }

    /** New, empty stores before each batch, as the pending map is empty before each flush. */
    @Setup(Level.Invocation)
    public void newService() {
        InMemoryIndexLog log = new InMemoryIndexLog();
        service = new IndexingService(datalake, new InMemoryMetadataRepository(), new InMemoryInvertedIndex(),
                log, new InMemoryPublicationRepository(log), parser, tokenizer, parsers);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        parsers.shutdownNow();
        BenchmarkBooks.delete(dir);
    }

    @Benchmark
    @OperationsPerInvocation(BenchmarkBooks.BATCH)
    public BatchResult indexBatch() throws InterruptedException {
        return service.indexBatch(ids);
    }
}
