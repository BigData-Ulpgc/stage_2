package es.ulpgc.bigdata.ingestion.benchmark;

import es.ulpgc.bigdata.ingestion.BookSplitter;
import es.ulpgc.bigdata.model.RawBook;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * BookSplitter.split of one raw book: normalize the line ends, find the START and END markers,
 * cut out header and body. The book is the "short", "median" or "long" one of the dataset
 * (percentiles 10, 50 and 90 of the raw length, see books.csv). The score is the time per book.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
@Threads(1)
@State(Scope.Benchmark)
public class SplitterBenchmark {

    @Param({"short", "median", "long"})
    public String book;

    private final BookSplitter splitter = new BookSplitter();
    private BenchmarkBooks.RawFile file;

    @Setup
    public void setUp() {
        setUp(BenchmarkBooks.booksDir());
    }

    void setUp(Path books) {
        file = BenchmarkBooks.byLength(BenchmarkBooks.load(books), book);
        // The same split Ingestion stores: a book without markers would measure the early exit
        BenchmarkBooks.split(file);
    }

    @Benchmark
    public Optional<RawBook> split() {
        return splitter.split(file.id(), file.text());
    }
}
