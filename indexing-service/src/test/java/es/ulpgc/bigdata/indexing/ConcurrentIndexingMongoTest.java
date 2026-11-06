package es.ulpgc.bigdata.indexing;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.datamart.index.MongoInvertedIndex;
import es.ulpgc.bigdata.datamart.index.Tokenizer;
import es.ulpgc.bigdata.datamart.indexlog.MongoIndexLog;
import es.ulpgc.bigdata.datamart.metadata.MetadataParser;
import es.ulpgc.bigdata.datamart.metadata.MongoMetadataRepository;
import es.ulpgc.bigdata.model.RawBook;
import es.ulpgc.bigdata.testsupport.MongoTestSupport;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * DR-11: indexing under real concurrency, against a real MongoDB. Each test has a database of
 * its own (search_engine_test_dr11_...), dropped at the end, with the collection names of the
 * service.
 *
 * The same book is indexed at the same time from several places in three real situations:
 *  - a writer slower than its lease (a long pause, a lost cluster): the Control's lease check
 *    puts the book back in index-queue, and the replica that holds index-writer now indexes it
 *    while the old one still finishes it (DR-06b);
 *  - POST /index/update/{id} while the index-queue writer indexes that book;
 *  - two threads of one replica (POST /index/update is served by many Jetty threads).
 * Inside one replica the writeLock orders the writes to the inverted index, but between
 * replicas nothing does: two upserts of the same NEW term can meet, and with the unique index
 * on "term" one of them could fail with E11000 duplicate key. MongoDB (4.2 or later) retries
 * such an upsert itself when its filter is an equality on the fields of the unique index, as
 * eq("term") is, and as _id is in books and index_log. These tests pin that guarantee.
 */
class ConcurrentIndexingMongoTest {

    private static final int EPISODES = 10;
    private static final int BOOKS_PER_EPISODE = 10;
    private static final int EPISODE_IDS = 100;             // the ids of episode e are e*100+1...
    private static final int THREADS_PER_REPLICA = 4;
    private static final int VOCABULARY = 300;

    @TempDir Path dataDir;

    private final List<MongoClient> clients = new ArrayList<>();
    private MongoDatabase db;
    private RangeBasedDatalake datalake;

    @BeforeEach
    void setUp() {
        assumeTrue(MongoTestSupport.available(), "MongoDB no está disponible en " + MongoTestSupport.URI + ": test saltado");
        db = newClient().getDatabase("search_engine_test_dr11_" + System.nanoTime());
        datalake = new RangeBasedDatalake(dataDir.resolve("datalake/range"));
    }

    @AfterEach
    void tearDown() {
        if (db != null) {
            db.drop();
        }
        clients.forEach(MongoClient::close);
    }

    /** A client of its own, as each replica has: its own connection pool. */
    private MongoClient newClient() {
        MongoClient client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MongoTestSupport.URI))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(1500, TimeUnit.MILLISECONDS))
                .build());
        clients.add(client);
        return client;
    }

    /** One replica: its own client and its own stores, over the same database. */
    private IndexingService replica() {
        MongoDatabase database = newClient().getDatabase(db.getName());
        return new IndexingService(datalake,
                new MongoMetadataRepository(database),
                new MongoInvertedIndex(database),
                new MongoIndexLog(database),
                new MongoPublicationRepository(database),
                new MetadataParser(), Tokenizer.standard());
    }

    /**
     * Book id: half of the shared vocabulary of its episode (so most terms are in many books,
     * and every thread upserts them) and one word of its own. Each episode has new terms.
     */
    private static String body(int id) {
        int episode = id / EPISODE_IDS;
        StringBuilder body = new StringBuilder();
        for (int j = 0; j < VOCABULARY / 2; j++) {
            body.append('e').append(episode).append("word").append((j * 7 + id * 13) % VOCABULARY).append(' ');
        }
        return body.append("only").append(id).append(".\n").toString();
    }

    /**
     * Waits for every thread and fails with the real failure (E11000, say), not with what the
     * threads stopped because of it got: a BrokenBarrierException, or a TimeoutException if
     * they reached the barrier after it was reset.
     */
    private static void assertNoThreadFailed(List<Future<?>> threads) throws InterruptedException {
        Throwable real = null;
        Throwable knockOn = null;
        for (Future<?> thread : threads) {
            try {
                thread.get(120, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                boolean stopped = cause instanceof BrokenBarrierException || cause instanceof TimeoutException;
                if (!stopped && real == null) {
                    real = cause;
                } else if (stopped && knockOn == null) {
                    knockOn = cause;
                }
            } catch (TimeoutException e) {
                throw new AssertionError("un hilo no terminó en 120 s", e);
            }
        }
        Throwable failure = real != null ? real : knockOn;
        if (failure != null) {
            throw new AssertionError("un hilo falló: " + failure, failure);
        }
    }

    /**
     * EPISODES times, every thread of both replicas indexes the same NEW books at once: their
     * terms do not exist yet, so the first flushes of the two replicas upsert the same new terms
     * at the same time. A race is only possible on the first write of a term, which is why it is
     * repeated with new terms. Then each thread repeats every book in another order, in batches
     * (the queue writer) and one by one (POST /index/update).
     */
    @Test
    void dosReplicasIndexandoLosMismosLibrosALaVezDejanElIndiceComoUnaSola() throws Exception {
        Map<String, Set<Integer>> expected = new HashMap<>();
        List<List<Integer>> episodes = new ArrayList<>();
        for (int e = 1; e <= EPISODES; e++) {
            List<Integer> ids = new ArrayList<>();
            for (int id = e * EPISODE_IDS + 1; id <= e * EPISODE_IDS + BOOKS_PER_EPISODE; id++) {
                datalake.save(new RawBook(id, IndexingServiceTest.HEADER_1342, body(id)));
                ids.add(id);
                for (String term : Tokenizer.standard().uniqueTerms(body(id))) {
                    expected.computeIfAbsent(term, t -> new TreeSet<>()).add(id);
                }
            }
            episodes.add(ids);
        }
        List<Integer> all = episodes.stream().flatMap(List::stream).toList();
        List<IndexingService> replicas = List.of(replica(), replica());

        int threadCount = replicas.size() * THREADS_PER_REPLICA;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CyclicBarrier together = new CyclicBarrier(threadCount);
        List<Future<?>> threads = new ArrayList<>();
        for (int r = 0; r < replicas.size(); r++) {
            for (int t = 0; t < THREADS_PER_REPLICA; t++) {
                IndexingService service = replicas.get(r);
                Random random = new Random(r * 100L + t);
                threads.add(pool.submit(() -> {
                    try {
                        for (List<Integer> ids : episodes) {
                            together.await(30, TimeUnit.SECONDS);
                            service.indexBatch(ids);
                        }
                        List<Integer> order = new ArrayList<>(all);
                        Collections.shuffle(order, random);
                        for (int i = 0; i < order.size(); i += 5) {
                            service.indexBatch(order.subList(i, Math.min(i + 5, order.size())));
                        }
                        for (int id : order.subList(0, BOOKS_PER_EPISODE)) {
                            service.update(id);
                        }
                        return null;
                    } catch (Exception | Error e) {
                        together.reset();               // the others stop at once instead of waiting
                        throw e;
                    }
                }));
            }
        }
        try {
            assertNoThreadFailed(threads);
        } finally {
            pool.shutdownNow();
        }

        List<Document> terms = db.getCollection("inverted_index").find().into(new ArrayList<>());
        assertEquals(expected.size(), terms.size(), "un documento por término, ni más ni menos");
        Set<String> seen = new HashSet<>();
        for (Document term : terms) {
            String name = term.getString("term");
            List<Integer> postings = term.getList("postings", Integer.class);
            assertEquals(Set.copyOf(postings).size(), postings.size(), "un id repetido en '" + name + "'");
            assertEquals(expected.get(name), new TreeSet<>(postings), "la posting list de '" + name + "'");
            seen.add(name);
        }
        assertEquals(expected.keySet(), seen);
        assertEquals(all.size(), db.getCollection("books").countDocuments(), "un documento por libro en books");
        assertEquals(all.size(), db.getCollection("index_log").countDocuments(), "una entrada por libro en index_log");
    }
}
