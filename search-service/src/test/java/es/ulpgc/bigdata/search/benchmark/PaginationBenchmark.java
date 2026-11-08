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

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Asking for a page near the start or near the end of the results, of two sizes. The matches are
 * always the same (the term "common"): only the page requested changes.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
@Threads(1)
@State(Scope.Benchmark)
public class PaginationBenchmark {

    @Param({"200", "10000", "100000"})
    public int books;

    /** Where the page starts, as a percentage of the results. */
    @Param({"0", "50", "99"})
    public int position;

    @Param({"20", "100"})
    public int limit;

    private SyntheticIndex index;
    private SearchQuery query;

    @Setup(Level.Trial)
    public void setUp() {
        index = new SyntheticIndex(books);
        Set<String> terms = Set.of("common");
        int total = index.expectedTotal(SyntheticIndex.query(terms, null, null, null, 0, limit));
        int offset = total * position / 100;
        query = SyntheticIndex.query(terms, null, null, null, offset, limit);
        index.verify(query);
    }

    @Benchmark
    public SearchEngine.Result search() {
        return index.engine.search(index.snapshot, query);
    }
}
