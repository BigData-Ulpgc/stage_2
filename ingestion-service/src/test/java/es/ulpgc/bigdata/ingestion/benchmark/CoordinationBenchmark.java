package es.ulpgc.bigdata.ingestion.benchmark;

import com.hazelcast.core.HazelcastInstance;
import es.ulpgc.bigdata.coordination.BookState;
import es.ulpgc.bigdata.coordination.Claim;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.CoordinationSettings;
import es.ulpgc.bigdata.coordination.Stage;
import es.ulpgc.bigdata.testsupport.HazelcastTestCluster;
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

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.IntStream;

/**
 * What coordinating a book through Hazelcast costs a worker of Ingestion: claim (T2, poll of
 * ingest-queue and the claim in book-state, one transaction) and complete (T3, DOWNLOADED and
 * the offer to index-queue, another transaction), with 1, 2, 4 and 8 threads sharing one client,
 * as the workers of a replica do. Nothing is downloaded or written: compare the score with
 * IngestBenchmark to see what part of the time of a book is coordination.
 *
 * The cluster is the one of the tests (HazelcastTestCluster, common): two members in this JVM,
 * with the backup and the index of hazelcast/hazelcast.yaml, and a client with the settings of
 * the services. Before every invocation (not measured) it is emptied and the 100 ids of a round
 * are seeded (T1, the Control's part); the threads then claim and complete until ingest-queue
 * is empty. The score is the mean time per book.
 *
 * Limits: client and members talk over loopback, so there is no real network; and the members
 * use the CPU of this same JVM, which the threads of the benchmark also need.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms4g", "-Xmx4g"})
@Threads(1)
@State(Scope.Benchmark)
public class CoordinationBenchmark {

    // Held here: java.util.logging keeps loggers by weak reference, and the level that
    // HazelcastTestCluster sets would be lost with its logger, filling the JMH output with INFO
    private static final Logger HAZELCAST_LOG = Logger.getLogger("com.hazelcast");

    @Param({"1", "2", "4", "8"})
    public int threads;

    private final List<Integer> ids = IntStream.rangeClosed(1, BenchmarkBooks.BATCH).boxed().toList();
    private List<Integer> workers;
    private HazelcastInstance client;
    private Coordination coordination;
    private ExecutorService pool;
    private int completed;

    @Setup(Level.Trial)
    public void setUp() {
        System.setProperty("hazelcast.logging.type", "jdk");
        HAZELCAST_LOG.setLevel(java.util.logging.Level.WARNING);
        CoordinationSettings settings = HazelcastTestCluster.settings();     // starts the two members
        client = settings.openClient();
        coordination = Coordination.on(client, settings, Clock.systemUTC());
        workers = IntStream.range(0, threads).boxed().toList();
        pool = Executors.newFixedThreadPool(threads);
    }

    @Setup(Level.Invocation)
    public void seedRound() {
        HazelcastTestCluster.reset();
        if (coordination.seed(ids) != ids.size()) {
            throw new IllegalStateException("the round was not seeded whole");
        }
    }

    /** Each thread is a worker: it claims and completes until ingest-queue is empty. */
    @Benchmark
    @OperationsPerInvocation(BenchmarkBooks.BATCH)
    public int claimAndComplete() throws Exception {
        completed = 0;
        int[] perWorker = new int[threads];
        BenchmarkBooks.onPool(pool, workers, worker -> {
            String owner = Coordination.owner("ingestion-bench", worker);
            Optional<Claim> claim;
            while ((claim = coordination.claim(Stage.INGESTION, owner, Duration.ZERO)).isPresent()) {
                if (!coordination.complete(claim.get())) {
                    throw new IllegalStateException("lost the claim of " + claim.get().bookId());
                }
                perWorker[worker]++;
            }
            return true;
        });
        for (int count : perWorker) {
            completed += count;
        }
        return completed;
    }

    /** Every book must be DOWNLOADED and waiting in index-queue, once. */
    @TearDown(Level.Invocation)
    public void check() {
        long downloaded = coordination.count(BookState.DOWNLOADED);
        int queued = coordination.queueSize(Stage.INDEXING);
        if (completed != ids.size() || downloaded != ids.size() || queued != ids.size()) {
            throw new IllegalStateException("completed " + completed + ", DOWNLOADED " + downloaded
                    + ", in index-queue " + queued + " of " + ids.size());
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        pool.shutdownNow();
        client.shutdown();
    }
}
