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
import com.mongodb.MongoException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
 * in-memory inverted index and fakes of the metadata repository and the index log.
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
    private InMemoryIndexLog indexLog;
    private InMemoryPublicationRepository publications;
    private IndexingService service;
    private final Tokenizer tokenizer = Tokenizer.standard();

    @BeforeEach
    void setUp() {
        datalake = new RangeBasedDatalake(dataDir.resolve("datalake/range"));
        metadata = new InMemoryMetadataRepository();
        index = new SpyIndex();
        indexLog = new InMemoryIndexLog();
        publications = new InMemoryPublicationRepository();
        service = newService();
    }

    /** A service on the same stores: calling it again is what a restart of the process sees. */
    private IndexingService newService() {
        return new IndexingService(datalake, metadata, index, indexLog, publications, new MetadataParser(), tokenizer);
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

        assertTrue(indexLog.contains(1342), "y, lo último, la prueba de que está entero en el índice");
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
        assertEquals(1, indexLog.count(), "un libro reindexado sigue siendo un libro");
    }

    // --- Books that are not ready --------------------------------------------------

    @Test
    void unLibroQueNoEstaEnElDatalakeDaNotFoundYNoEscribeNada() {
        BookNotFoundException e = assertThrows(BookNotFoundException.class, () -> service.update(999999));

        assertEquals(999999, e.bookId());
        assertEquals(0, metadata.count());
        assertEquals(0, index.flushes.get());
        assertEquals(0, indexLog.count());
    }

    @Test
    void unLibroSinMarcadorReadyDaNotFoundYNoEscribeNada() throws Exception {
        // Header and body on disk, but no <id>._READY: Ingestion died before writing it (§6.2).
        Path range = Files.createDirectories(dataDir.resolve("datalake/range/01000-01999"));
        Files.writeString(range.resolve("1342.header.txt"), HEADER_1342);
        Files.writeString(range.resolve("1342.body.txt"), BODY_1342);

        assertThrows(BookNotFoundException.class, () -> service.update(1342));

        assertEquals(0, metadata.count());
        assertEquals(0, index.flushes.get());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",                                             // cuerpo vacío
            "   \n\n\t  ",                                 // solo espacios
            "The and of, to: a! In? IS... it -- \"the\".",  // solo stopwords y puntuación
            "Α β γ δ. Война и мир. 紅樓夢",                   // solo caracteres que el tokenizer no guarda
            "a b c d e 1 2 3"                               // solo tokens de longitud 1
    })
    void unLibroSinTerminosDaEmptyBookYNoEscribeNada(String body) {
        datalake.save(new RawBook(1342, HEADER_1342, body));

        EmptyBookException e = assertThrows(EmptyBookException.class, () -> service.update(1342));

        assertEquals(1342, e.bookId());
        assertEquals(0, metadata.count(), "ni la ficha en books");
        assertEquals(0, index.flushes.get(), "ni las posting lists");
        assertEquals(0, indexLog.count(), "ni index_log: DR-08 no lo publicará");
        assertNull(service.status().lastUpdate(), "no cuenta como update terminado");
    }

    @Test
    void unSoloTerminoYaBastaParaIndexar() {
        datalake.save(new RawBook(1342, HEADER_1342, "The whale."));

        UpdateResult result = service.update(1342);

        assertEquals(1, result.terms());
        assertEquals(1, metadata.count());
        assertEquals(1, index.flushes.get());
    }

    @ParameterizedTest
    @ValueSource(ints = {Integer.MIN_VALUE, -1, 0, BookIds.MAX + 1, Integer.MAX_VALUE})
    void unIdFueraDeRangoSeRechazaSinTocarNada(int bookId) {
        assertThrows(IllegalArgumentException.class, () -> service.update(bookId));
        assertEquals(0, metadata.count());
        assertEquals(0, index.flushes.get());
        assertEquals(0, indexLog.count());
    }

    // --- Failures --------------------------------------------------------------------

    @Test
    void siFallanLosMetadatosNoSeTocaElIndice() {
        datalake.save(new RawBook(1342, HEADER_1342, BODY_1342));
        metadata.failing(true);

        assertThrows(MetadataRepositoryException.class, () -> service.update(1342));

        assertEquals(List.of(), index.postings("elizabeth"), "metadatos primero: el índice no se tocó");
        assertEquals(0, indexLog.count());
    }

    @Test
    void siFallaElFlushElLibroNoEntraEnElLog() {
        datalake.save(new RawBook(1342, HEADER_1342, BODY_1342));
        index.failNextFlushWith = new MongoException("flush falla (simulado)");

        assertThrows(MongoException.class, () -> service.update(1342));

        assertTrue(metadata.findById(1342).isPresent(), "la ficha ya se guardó");
        assertFalse(indexLog.contains(1342),
                "sin posting lists no hay prueba: el log va DESPUÉS del flush, y DR-08 no lo publicará");
    }

    @Test
    void siFallaElLogElUpdateFallaYElReintentoLoCompleta() {
        datalake.save(new RawBook(1342, HEADER_1342, BODY_1342));
        indexLog.failing(true);

        assertThrows(MongoException.class, () -> service.update(1342), "la API responde 503");

        // The book is in the index, but without evidence it does not count and is not published.
        assertEquals(List.of(1342), index.postings("elizabeth"));
        indexLog.failing(false);
        assertEquals(0, service.status().booksIndexed());

        service.update(1342);                               // the retry (idempotent)

        assertEquals(1, service.status().booksIndexed());
        assertEquals(List.of(1342), index.postings("elizabeth"), "sin duplicados en el índice");
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
        assertEquals(threads * booksPerThread, indexLog.count());
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
        // The log keeps milliseconds, as a MongoDB date.
        assertFalse(status.lastUpdate().isBefore(before.truncatedTo(ChronoUnit.MILLIS)));
        assertFalse(status.lastUpdate().isAfter(Instant.now()));
    }

    @Test
    void unUpdateFallidoNoCambiaLastUpdate() {
        datalake.save(new RawBook(1342, HEADER_1342, BODY_1342));
        index.failNextFlushWith = new IllegalStateException("flush falla (simulado)");

        assertThrows(IllegalStateException.class, () -> service.update(1342));

        IndexStatus status = service.status();
        assertNull(status.lastUpdate(), "solo cuenta un update que terminó bien");
        assertEquals(0, status.booksIndexed());
    }

    @Test
    void statusCuentaLosLibrosDelLogNoLasFichas() {
        datalake.save(new RawBook(1342, HEADER_1342, BODY_1342));
        service.update(1342);
        // A book whose update stopped after saving its metadata: it is in books, not in the index.
        metadata.save(new BookMetadata(84, "Frankenstein", "Mary Shelley", "en", null, null, null, null, null));

        assertEquals(2, metadata.count());
        assertEquals(1, service.status().booksIndexed(), "solo los libros enteros en el índice");
    }

    @Test
    void statusSobreviveAUnReinicio() {
        datalake.save(new RawBook(1342, HEADER_1342, BODY_1342));
        service.update(1342);
        IndexStatus before = service.status();

        IndexStatus after = newService().status();          // another process on the same stores

        assertEquals(1, after.booksIndexed());
        assertNotNull(after.lastUpdate(), "antes de DR-07 volvía a null al reiniciar");
        assertEquals(before.lastUpdate(), after.lastUpdate());
    }

    @Test
    void reindexarUnLibroAdelantaLastUpdate() throws InterruptedException {
        datalake.save(new RawBook(1342, HEADER_1342, BODY_1342));
        service.update(1342);
        Instant first = service.status().lastUpdate();
        Thread.sleep(5);                                    // the log keeps milliseconds

        service.update(1342);

        assertTrue(service.status().lastUpdate().isAfter(first), "reindexar cuenta como indexar");
        assertEquals(1, service.status().booksIndexed());
    }

    @Test
    void statusConMongoCaidoFallaConMongoException() {
        indexLog.failing(true);

        assertThrows(MongoException.class, service::status, "la API responde 503");
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

            // The update holds the lock now. status must answer anyway, and the book does not
            // count yet: its posting lists are not written, so it is not in the log.
            Future<IndexStatus> status = reader.submit(service::status);
            assertEquals(0, status.get(2, TimeUnit.SECONDS).booksIndexed(),
                    "status no debe esperar al lock del índice, ni contar un libro a medias");

            index.flushGate.countDown();                       // let the update finish
            assertEquals(1342, update.get(5, TimeUnit.SECONDS).bookId());
            assertEquals(1, service.status().booksIndexed());
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
