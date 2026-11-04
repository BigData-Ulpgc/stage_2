package es.ulpgc.bigdata.indexing;

import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.datamart.index.InMemoryInvertedIndex;
import es.ulpgc.bigdata.datamart.index.InvertedIndex;
import es.ulpgc.bigdata.datamart.index.Tokenizer;
import es.ulpgc.bigdata.datamart.metadata.MetadataParser;
import es.ulpgc.bigdata.datamart.metadata.MetadataRepositoryException;
import es.ulpgc.bigdata.model.BookIds;
import es.ulpgc.bigdata.model.BookMetadata;
import es.ulpgc.bigdata.model.RawBook;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * IndexingService without MongoDB or Docker: a real datalake in a temporary folder, the
 * in-memory inverted index and a fake metadata repository.
 */
class IndexingServiceTest {

    /** Header with the format of a real Project Gutenberg one (data/headers/1342.header.txt). */
    static final String HEADER_1342 = """
            The Project Gutenberg eBook of Pride and Prejudice

            Title: Pride and Prejudice

            Author: Jane Austen

            Release date: June 1, 1998 [eBook #1342]
                            Most recently updated: September 1, 2026

            Language: English
            """;

    static final String BODY_1342 = """
            It is a truth universally acknowledged, that a single man in possession
            of a good fortune, must be in want of a wife. Elizabeth Bennet laughed.
            """;

    @TempDir Path dataDir;

    private RangeBasedDatalake datalake;
    private InMemoryMetadataRepository metadata;
    private SpyIndex index;
    private InMemoryPublicationRepository publications;
    private IndexingService service;
    private final Tokenizer tokenizer = Tokenizer.standard();

    @BeforeEach
    void setUp() {
        datalake = new RangeBasedDatalake(dataDir.resolve("datalake/range"));
        metadata = new InMemoryMetadataRepository();
        index = new SpyIndex();
        publications = new InMemoryPublicationRepository();
        service = new IndexingService(datalake, metadata, index, publications, new MetadataParser(), tokenizer);
    }

    // --- What update writes --------------------------------------------------------

    @Test
    void indexaUnLibroCompletoYDevuelveSusTerminos() {
        datalake.save(new RawBook(1342, HEADER_1342, BODY_1342));

        UpdateResult result = service.update(1342);

        assertEquals(1342, result.bookId());
        assertEquals(tokenizer.uniqueTerms(BODY_1342).size(), result.terms());
        assertEquals(List.of(1342), index.postings("elizabeth"));
        assertEquals(List.of(1342), index.postings("fortune"));
        assertEquals(List.of(), index.postings("the"), "las stopwords no se indexan");

        BookMetadata book = metadata.findById(1342).orElseThrow();
        assertEquals("Pride and Prejudice", book.title());
        assertEquals("Jane Austen", book.author());
        assertEquals("en", book.language());
        assertEquals(1998, book.year());
    }

    @Test
    void elUpdateEsSincronoElIndiceYaHizoFlush() {
        datalake.save(new RawBook(1342, HEADER_1342, BODY_1342));

        service.update(1342);

        assertEquals(1, index.flushes.get(), "un 200 significa escrito: el flush ya se hizo");
    }

    @Test
    void indexarDosVecesDaElMismoResultado() {
        datalake.save(new RawBook(1342, HEADER_1342, BODY_1342));

        UpdateResult first = service.update(1342);
        UpdateResult second = service.update(1342);

        assertEquals(first, second);
        assertEquals(List.of(1342), index.postings("elizabeth"));
        assertEquals(1, metadata.count());
    }

    // --- Books that are not ready --------------------------------------------------

    @Test
    void unLibroQueNoEstaEnElDatalakeDaNotFoundYNoEscribeNada() {
        BookNotFoundException e = assertThrows(BookNotFoundException.class, () -> service.update(999999));

        assertEquals(999999, e.bookId());
        assertEquals(0, metadata.count());
        assertEquals(0, index.flushes.get());
    }

    @ParameterizedTest
    @ValueSource(ints = {Integer.MIN_VALUE, -1, 0, BookIds.MAX + 1, Integer.MAX_VALUE})
    void unIdFueraDeRangoSeRechazaSinTocarNada(int bookId) {
        assertThrows(IllegalArgumentException.class, () -> service.update(bookId));
        assertEquals(0, metadata.count());
        assertEquals(0, index.flushes.get());
    }

    // --- Failures --------------------------------------------------------------------

    @Test
    void siFallanLosMetadatosNoSeTocaElIndice() {
        datalake.save(new RawBook(1342, HEADER_1342, BODY_1342));
        metadata.failing(true);

        assertThrows(MetadataRepositoryException.class, () -> service.update(1342));

        assertEquals(List.of(), index.postings("elizabeth"), "metadatos primero: el índice no se tocó");
    }

    @Test
    void siFallaElFlushElLockSeSueltaYOtroHiloPuedeIndexar() throws Exception {
        datalake.save(new RawBook(1342, HEADER_1342, BODY_1342));
        datalake.save(new RawBook(84, HEADER_1342, "Frankenstein monster"));
        index.failNextFlushWith = new IllegalStateException("flush falla (simulado)");

        assertThrows(IllegalStateException.class, () -> service.update(1342));

        // ReentrantLock lets the SAME thread lock again even if it was never unlocked, so
        // the check must come from ANOTHER thread: it would wait forever without the finally.
        ExecutorService other = Executors.newSingleThreadExecutor();
        try {
            Future<UpdateResult> result = other.submit(() -> service.update(84));
            assertEquals(84, result.get(5, TimeUnit.SECONDS).bookId());
        } finally {
            other.shutdownNow();
        }
    }

    // --- Concurrency (D5) ------------------------------------------------------------

    @Test
    void variosHilosALaVezNuncaEscribenEnElIndiceALaVez() throws Exception {
        int threads = 8;
        int booksPerThread = 10;
        for (int id = 1; id <= threads * booksPerThread; id++) {
            datalake.save(new RawBook(id, HEADER_1342, "shared words plus unique" + id));
        }

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);       // every thread starts at the same time
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int first = t * booksPerThread + 1;
            futures.add(pool.submit(() -> {
                start.await();
                for (int id = first; id < first + booksPerThread; id++) {
                    service.update(id);
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);                    // rethrows any failure of the thread
        }
        pool.shutdown();

        assertEquals(1, index.maxWritersAtOnce.get(), "dos hilos escribieron en el índice a la vez");
        assertEquals(threads * booksPerThread, index.postings("shared").size());
        assertEquals(threads * booksPerThread, metadata.count());
    }

    // --- status (GET /index/status) ----------------------------------------------------

    @Test
    void statusSinNadaIndexadoNiPublicado() {
        IndexStatus status = service.status();

        assertEquals(0, status.booksIndexed());
        assertNull(status.lastUpdate());
        assertEquals(0, status.indexSizeBytes());
        assertNull(status.publishedVersion());
        assertEquals(0, status.booksPublished());
    }

    @Test
    void statusTrasUnUpdateCuentaElLibroYApuntaLaHora() {
        datalake.save(new RawBook(1342, HEADER_1342, BODY_1342));
        Instant before = Instant.now();

        service.update(1342);
        IndexStatus status = service.status();

        assertEquals(1, status.booksIndexed());
        assertNotNull(status.lastUpdate());
        assertFalse(status.lastUpdate().isBefore(before));
        assertFalse(status.lastUpdate().isAfter(Instant.now()));
    }

    @Test
    void unUpdateFallidoNoCambiaLastUpdate() {
        datalake.save(new RawBook(1342, HEADER_1342, BODY_1342));
        index.failNextFlushWith = new IllegalStateException("flush falla (simulado)");

        assertThrows(IllegalStateException.class, () -> service.update(1342));

        assertNull(service.status().lastUpdate(), "solo cuenta un update que terminó bien");
    }

    @Test
    void statusLeeLaVersionPublicadaDeCurrent() {
        publications.setCurrent(new Publication(42, 187));

        IndexStatus status = service.status();

        assertEquals(42, status.publishedVersion());
        assertEquals(187, status.booksPublished());
    }

    @Test
    void statusNoEsperaAUnUpdateQueTieneElLock() throws Exception {
        datalake.save(new RawBook(1342, HEADER_1342, BODY_1342));
        index.flushGate = new CountDownLatch(1);               // the update will stop inside flush
        ExecutorService writer = Executors.newSingleThreadExecutor();
        ExecutorService reader = Executors.newSingleThreadExecutor();
        try {
            Future<UpdateResult> update = writer.submit(() -> service.update(1342));
            assertTrue(index.flushEntered.await(5, TimeUnit.SECONDS), "el update no llegó al flush");

            // The update holds the lock now. status must answer anyway.
            Future<IndexStatus> status = reader.submit(service::status);
            assertEquals(1, status.get(2, TimeUnit.SECONDS).booksIndexed(),
                    "status no debe esperar al lock del índice");

            index.flushGate.countDown();                       // let the update finish
            assertEquals(1342, update.get(5, TimeUnit.SECONDS).bookId());
        } finally {
            index.flushGate.countDown();
            writer.shutdownNow();
            reader.shutdownNow();
        }
    }

    /**
     * InMemoryInvertedIndex plus a spy: counts flushes, can fail the next flush, and records
     * how many threads were inside addDocument/flush at the same time. The short sleep widens
     * the window, so without the lock two threads would almost surely meet inside.
     */
    static final class SpyIndex implements InvertedIndex {

        private final InMemoryInvertedIndex delegate = new InMemoryInvertedIndex();
        private final AtomicInteger writersNow = new AtomicInteger();
        final AtomicInteger maxWritersAtOnce = new AtomicInteger();
        final AtomicInteger flushes = new AtomicInteger();
        /** If not null, the next flush throws it (once). */
        volatile RuntimeException failNextFlushWith = null;
        /** If not null, flush counts down flushEntered and then waits until the test opens the gate. */
        volatile CountDownLatch flushGate = null;
        final CountDownLatch flushEntered = new CountDownLatch(1);

        private void enter() {
            int now = writersNow.incrementAndGet();
            maxWritersAtOnce.accumulateAndGet(now, Math::max);
        }

        @Override
        public void addDocument(int bookId, Set<String> terms) {
            enter();
            try {
                delegate.addDocument(bookId, terms);
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                writersNow.decrementAndGet();
            }
        }

        @Override
        public void flush() {
            enter();
            try {
                RuntimeException failure = failNextFlushWith;
                if (failure != null) {
                    failNextFlushWith = null;
                    throw failure;
                }
                CountDownLatch gate = flushGate;
                if (gate != null) {
                    flushEntered.countDown();
                    gate.await();
                }
                delegate.flush();
                flushes.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                writersNow.decrementAndGet();
            }
        }

        @Override public String name() { return delegate.name(); }
        @Override public List<Integer> postings(String term) { return delegate.postings(term); }
        @Override public void clear() { delegate.clear(); }
        @Override public long diskUsageBytes() { return delegate.diskUsageBytes(); }
        @Override public void close() { delegate.close(); }
    }
}
