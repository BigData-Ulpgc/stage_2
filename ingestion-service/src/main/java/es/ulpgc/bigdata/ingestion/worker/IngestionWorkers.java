package es.ulpgc.bigdata.ingestion.worker;

import es.ulpgc.bigdata.config.Env;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.ingestion.IngestionService;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * The K workers of one Ingestion replica, sharing its Coordination client, its source and its
 * Throttle:
 *
 * <pre>
 *   INGESTION_WORKERS         4   worker threads of the replica (0: HTTP API only, no Hazelcast)
 *   INGESTION_FETCH_RETRIES   2   retries of a transient source failure inside one attempt
 * </pre>
 *
 * Several replicas (docker compose --scale ingestion=N) share the work through ingest-queue:
 * Hazelcast gives each id to one claim, so nobody needs to know the other replicas.
 *
 * close() asks every worker to stop after its current book and waits up to {@link #GRACE};
 * a worker still downloading after that is interrupted, and its book is left INGESTING until
 * its lease expires and the Control requeues it (coordination §7).
 */
public final class IngestionWorkers implements AutoCloseable {

    public static final String WORKERS = "INGESTION_WORKERS";
    public static final String FETCH_RETRIES = "INGESTION_FETCH_RETRIES";
    public static final int DEFAULT_WORKERS = 4;
    public static final int DEFAULT_FETCH_RETRIES = 2;
    static final Duration GRACE = Duration.ofSeconds(10);

    private final List<IngestionWorker> workers = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();
    private final WorkerStats stats = new WorkerStats();
    private final Throttle throttle;
    private final Coordination coordination;

    private IngestionWorkers(Coordination coordination, Throttle throttle) {
        this.coordination = coordination;
        this.throttle = throttle;
    }

    /** INGESTION_WORKERS from the environment: 0 or more. */
    public static int count(Env env) {
        return env.integerAtLeast(WORKERS, DEFAULT_WORKERS, 0);
    }

    /** INGESTION_FETCH_RETRIES from the environment: 0 or more. */
    public static int fetchRetries(Env env) {
        return env.integerAtLeast(FETCH_RETRIES, DEFAULT_FETCH_RETRIES, 0);
    }

    /** Starts {@code count} worker threads named ingestion-worker-0, -1... */
    public static IngestionWorkers start(Coordination coordination, IngestionService ingestion,
                                         int count, int fetchRetries, Clock clock) {
        Objects.requireNonNull(coordination, "coordination");
        IngestionWorkers pool = new IngestionWorkers(coordination, new Throttle(clock));
        for (int i = 0; i < count; i++) {
            IngestionWorker worker = new IngestionWorker(coordination, ingestion, pool.throttle, pool.stats,
                    Coordination.owner("ingestion", i), fetchRetries, d -> Thread.sleep(d.toMillis()));
            Thread thread = new Thread(worker, "ingestion-worker-" + i);
            pool.workers.add(worker);
            pool.threads.add(thread);
            thread.start();
        }
        return pool;
    }

    /** For GET /status: what the workers did, the pauses and the dropped ids. */
    public Map<String, Object> status() {
        Map<String, Object> status = new LinkedHashMap<>(stats.asMap());
        status.put("workers", workers.size());
        status.put("throttle_pauses", throttle.pauses());
        status.put("paused_seconds", throttle.remaining().toSeconds());
        status.put("invalid_ids_dropped", coordination.invalidIdsDropped());
        status.put("ids_skipped", coordination.idsSkipped());
        return status;
    }

    @Override
    public void close() {
        workers.forEach(IngestionWorker::stop);
        long deadline = System.nanoTime() + GRACE.toNanos();
        for (Thread thread : threads) {
            try {
                thread.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        threads.stream().filter(Thread::isAlive).forEach(Thread::interrupt);
    }
}
