package es.ulpgc.bigdata.indexing.benchmark;

import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.datamart.metadata.MetadataParser;
import es.ulpgc.bigdata.model.BookLocation;
import es.ulpgc.bigdata.model.RawBook;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * MetadataParser.parse on the headers of all the books: the mix of DR-16 is the one of the 200
 * real headers (4.5 % with "Original publication", 1.5 % without "Author", 10 % not in English),
 * so the score is the mean time per header over that mix. The header does not grow with the
 * book, so there is no short or long one here.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
@Threads(1)
@State(Scope.Benchmark)
public class MetadataParserBenchmark {

    /** The books of the run-jmh.sh dataset; parseAll divides its time by this. */
    static final int BOOKS = 200;

    private final MetadataParser parser = new MetadataParser();
    private final List<BookLocation> locations = new ArrayList<>();
    private final List<String> headers = new ArrayList<>();
    private Path dir;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
        List<RawBook> books = BenchmarkBooks.load();
        if (books.size() != BOOKS) {
            throw new IllegalStateException("este benchmark espera " + BOOKS + " libros, hay " + books.size());
        }
        dir = Files.createTempDirectory("dr12-parser");
        RangeBasedDatalake datalake = BenchmarkBooks.datalake(dir, books);
        for (RawBook book : books) {
            locations.add(datalake.locate(book.id()).orElseThrow());
            headers.add(book.header());
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        BenchmarkBooks.delete(dir);
    }

    @Benchmark
    @OperationsPerInvocation(BOOKS)
    public void parseAll(Blackhole blackhole) {
        for (int i = 0; i < BOOKS; i++) {
            blackhole.consume(parser.parse(locations.get(i), headers.get(i)));
        }
    }
}
