package es.ulpgc.bigdata.indexing.benchmark;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import es.ulpgc.bigdata.datamart.index.InMemoryInvertedIndex;
import es.ulpgc.bigdata.datamart.index.InvertedIndex;
import es.ulpgc.bigdata.datamart.index.MongoInvertedIndex;
import es.ulpgc.bigdata.datamart.index.Tokenizer;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * InvertedIndex.addDocument (the "Indexer.add" of the plan): the terms of the 100 books of a
 * batch go into an empty index, as they go into the pending map of MongoInvertedIndex before its
 * flush. The score is the mean time per book.
 *
 *  - memory: InMemoryInvertedIndex, the index of the Stage 1 and of the tests;
 *  - mongo:  MongoInvertedIndex without its flush, so without MongoDB: addDocument only touches
 *            its pending map. Its client points to a port where nothing answers, and it is never
 *            used. The code is the same as memory, so both should cost the same.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
// The unused client keeps trying to connect in the background: its log would fill the output
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx4g", "-Dorg.slf4j.simpleLogger.log.org.mongodb.driver=off"})
@Threads(1)
@State(Scope.Benchmark)
public class AddDocumentBenchmark {

    @Param({"memory", "mongo"})
    public String index;

    private final List<Integer> ids = new ArrayList<>();
    private final List<Set<String>> terms = new ArrayList<>();
    private MongoClient unused;
    private InvertedIndex target;

    @Setup(Level.Trial)
    public void setUp() {
        Tokenizer tokenizer = Tokenizer.standard();
        for (RawBook book : BenchmarkBooks.batch(BenchmarkBooks.load())) {
            ids.add(book.id());
            terms.add(tokenizer.uniqueTerms(book.body()));
        }
        if (index.equals("mongo")) {
            unused = MongoClients.create("mongodb://127.0.0.1:9/?serverSelectionTimeoutMS=1");
        } else if (!index.equals("memory")) {
            throw new IllegalArgumentException("índice desconocido: " + index);
        }
    }

    /** A new, empty index before each batch: its cost is not measured. */
    @Setup(Level.Invocation)
    public void emptyIndex() {
        target = unused == null
                ? new InMemoryInvertedIndex()
                : new MongoInvertedIndex(unused.getDatabase("unused"));
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (unused != null) {
            unused.close();
        }
    }

    @Benchmark
    @OperationsPerInvocation(BenchmarkBooks.BATCH)
    public InvertedIndex addBatch() {
        for (int i = 0; i < ids.size(); i++) {
            target.addDocument(ids.get(i), terms.get(i));
        }
        return target;
    }
}
