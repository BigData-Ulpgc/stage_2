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
 * Filters on the snapshot metadata. The term is always "common" (half of the books), so every
 * scenario examines the same candidates: what changes is the cost of checking each one.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
@Threads(1)
@State(Scope.Benchmark)
public class FilterBenchmark {

    @Param({"200", "10000", "100000"})
    public int books;

    @Param({"none", "language", "year", "author", "all"})
    public String filter;

    private SyntheticIndex index;
    private SearchQuery query;

    @Setup(Level.Trial)
    public void setUp() {
        index = new SyntheticIndex(books);
        String author = filter.equals("author") || filter.equals("all") ? "author 0042" : null;
        String language = filter.equals("language") || filter.equals("all") ? "en" : null;
        Integer year = filter.equals("year") || filter.equals("all") ? Integer.valueOf(1900) : null;
        query = SyntheticIndex.query(Set.of("common"), author, language, year, 0, SearchQuery.DEFAULT_LIMIT);
        index.verify(query);
    }

    @Benchmark
    public SearchEngine.Result search() {
        return index.engine.search(index.snapshot, query);
    }
}
