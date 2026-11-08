package es.ulpgc.bigdata.search.benchmark;

import es.ulpgc.bigdata.search.query.SearchEngine;
import es.ulpgc.bigdata.search.query.SearchQuery;
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

import java.util.concurrent.TimeUnit;

/**
 * AND of the posting lists: does the cost follow the shortest list, as SearchEngine intends?
 * No filters and always the first page, so the difference between scenarios is the intersection.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
@Threads(1)
@State(Scope.Benchmark)
public class IntersectionBenchmark {

    @Param({"200", "10000", "100000"})
    public int books;

    @Param({"common", "common_frequent", "common_rare", "common_frequent_medium"})
    public String terms;

    private SyntheticIndex index;
    private SearchQuery query;

    @Setup(Level.Trial)
    public void setUp() {
        index = new SyntheticIndex(books);
        query = SyntheticIndex.query(SyntheticIndex.terms(terms), null, null, null, 0, SearchQuery.DEFAULT_LIMIT);
        index.verify(query);
    }

    @Benchmark
    public SearchEngine.Result search() {
        return index.engine.search(index.snapshot, query);
    }
}
