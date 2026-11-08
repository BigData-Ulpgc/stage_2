package es.ulpgc.bigdata.indexing.benchmark;

import es.ulpgc.bigdata.datamart.index.Tokenizer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tokenizer.uniqueTerms on one body: what IndexingService.prepare does with every book after
 * reading it. Does its cost follow the length of the book? books.csv of the run has the size of
 * each one, to give the result in MB/s.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
@Threads(1)
@State(Scope.Benchmark)
public class TokenizerBenchmark {

    @Param({"short", "median", "long"})
    public String book;

    private final Tokenizer tokenizer = Tokenizer.standard();
    private String body;

    @Setup(Level.Trial)
    public void setUp() {
        body = BenchmarkBooks.byLength(BenchmarkBooks.load(), book).body();
    }

    @Benchmark
    public Set<String> uniqueTerms() {
        return tokenizer.uniqueTerms(body);
    }
}
