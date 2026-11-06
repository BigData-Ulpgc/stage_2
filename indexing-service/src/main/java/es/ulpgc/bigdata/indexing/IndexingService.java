package es.ulpgc.bigdata.indexing;

import es.ulpgc.bigdata.datalake.Datalake;
import es.ulpgc.bigdata.datamart.index.InvertedIndex;
import es.ulpgc.bigdata.datamart.index.MongoInvertedIndex;
import es.ulpgc.bigdata.datamart.index.Tokenizer;
import es.ulpgc.bigdata.datamart.indexlog.IndexLog;
import es.ulpgc.bigdata.datamart.metadata.MetadataParser;
import es.ulpgc.bigdata.datamart.metadata.MetadataRepository;
import es.ulpgc.bigdata.model.BookIds;
import es.ulpgc.bigdata.model.BookLocation;
import es.ulpgc.bigdata.model.BookMetadata;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Core of the Indexing Service, without anything of HTTP (DR-05, D8): IndexingApi calls it
 * for POST /index/update/{id}, and the index-queue consumer will call the same method
 * (DR-06), so both paths index exactly the same way.
 *
 * Two ways in: update(id), one book (POST /index/update), and indexBatch(ids), a batch (the
 * index-queue writer, DR-09). Both read, parse and tokenize each book first (prepare) and then
 * write the books that are ready in one go (write), so they index exactly the same way.
 *
 * Batches (DR-09): indexBatch prepares the books in parallel on the parsers pool
 * (INDEXING_THREADS) and writes them together: one saveAll of the metadata, one flush of the
 * inverted index and one entry per book in the log. The flush sends one upsert per DISTINCT
 * term of the batch, so a common word is written once for the whole batch instead of once per
 * book: the writes to MongoDB are what limits indexing. A book that cannot be prepared (not
 * complete in the datalake, no terms, unreadable) is left out and reported; it never stops the
 * others. A write that fails fails the whole batch: nothing is in the log, the retry repeats it.
 *
 * Thread safety (D5): Jetty serves every request in its own thread. Reading, parsing and
 * tokenizing run in parallel; the metadata repository is thread-safe (a MongoCollection and
 * no state of its own). Only the inverted index is not (its pending map), so addDocument +
 * flush run under a lock: one writer at a time in this process, whether it writes one book
 * or a batch. Between replicas, Hazelcast's index-writer lock lets only one consume the queue
 * (DR-06b).
 *
 * Write order of an update (DR-07): metadata, posting lists, and LAST the index log. The log
 * is the evidence that the book is complete in the index (publish, INDEXED and the recovery
 * of the Control rely on it), so it is written only when everything else is.
 *
 * Which collection (D8, DR-10): the posting lists go to the collection of the inverted index
 * that CURRENT names, the one Search reads; MONGO_COLLECTION while there is no CURRENT or it
 * names none. A rebuild publishes a new collection, so CURRENT is read again before each write,
 * not once at start: after a rebuild in any replica, the next write already goes to the new
 * collection. A write that read CURRENT just before a rebuild switched it still lands in the
 * old one, and nothing is lost: a rebuild only starts with the writers stopped, and the book of
 * an update already running is in the datalake, so the rebuild indexes it too.
 *
 * Errors are exceptions with a meaning; IndexingApi turns them into HTTP codes:
 *  - BookNotFoundException:                    the book is not complete in the datalake (404);
 *  - EmptyBookException:                       the book gives no term, nothing written (422);
 *  - MongoException, MetadataRepositoryException: MongoDB does not answer (503), also when only
 *                                              the index log failed: the retry completes it;
 *  - UncheckedIOException:                     the book files could not be read (500);
 *  - IllegalStateException:                    CURRENT does not follow the contract (500).
 */
public final class IndexingService {

    private final Datalake datalake;
    private final MetadataRepository metadata;
    /** Opens the inverted index of a collection; each one is opened once and kept in indexes. */
    private final Function<String, InvertedIndex> openIndex;
    /** MONGO_COLLECTION: where the posting lists go while CURRENT names no collection. */
    private final String defaultCollection;
    private final Map<String, InvertedIndex> indexes = new ConcurrentHashMap<>();
    private final IndexLog indexLog;
    private final PublicationRepository publications;
    private final MetadataParser parser;
    private final Tokenizer tokenizer;

    /** Where indexBatch prepares the books (INDEXING_THREADS); App owns it and shuts it down. */
    private final Executor parsers;

    /** Only one thread at a time touches the inverted index. */
    private final ReentrantLock writeLock = new ReentrantLock();

    /**
     * One inverted index whatever collection CURRENT names (tests and benchmarks of a single
     * collection). The books of a batch are prepared one after another, in the calling thread.
     */
    public IndexingService(Datalake datalake, MetadataRepository metadata, InvertedIndex index,
                           IndexLog indexLog, PublicationRepository publications,
                           MetadataParser parser, Tokenizer tokenizer) {
        this(datalake, metadata, index, indexLog, publications, parser, tokenizer, Runnable::run);
    }

    /**
     * One inverted index whatever collection CURRENT names (tests and benchmarks of a single
     * collection).
     *
     * @param parsers where indexBatch prepares the books: a pool of INDEXING_THREADS in the service
     */
    public IndexingService(Datalake datalake, MetadataRepository metadata, InvertedIndex index,
                           IndexLog indexLog, PublicationRepository publications,
                           MetadataParser parser, Tokenizer tokenizer, Executor parsers) {
        this(datalake, metadata, oneIndex(index), MongoInvertedIndex.DEFAULT_COLLECTION,
                indexLog, publications, parser, tokenizer, parsers);
    }

    /**
     * The service: the posting lists go to the collection CURRENT names (D8, DR-10).
     *
     * @param openIndex         opens the inverted index of a collection; called once per collection
     * @param defaultCollection MONGO_COLLECTION, while CURRENT names no collection
     * @param parsers           where indexBatch prepares the books: a pool of INDEXING_THREADS
     */
    public IndexingService(Datalake datalake, MetadataRepository metadata,
                           Function<String, InvertedIndex> openIndex, String defaultCollection,
                           IndexLog indexLog, PublicationRepository publications,
                           MetadataParser parser, Tokenizer tokenizer, Executor parsers) {
        this.datalake = Objects.requireNonNull(datalake, "datalake");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.openIndex = Objects.requireNonNull(openIndex, "openIndex");
        this.defaultCollection = Objects.requireNonNull(defaultCollection, "defaultCollection");
        this.indexLog = Objects.requireNonNull(indexLog, "indexLog");
        this.publications = Objects.requireNonNull(publications, "publications");
        this.parser = Objects.requireNonNull(parser, "parser");
        this.tokenizer = Objects.requireNonNull(tokenizer, "tokenizer");
        this.parsers = Objects.requireNonNull(parsers, "parsers");
    }

    private static Function<String, InvertedIndex> oneIndex(InvertedIndex index) {
        Objects.requireNonNull(index, "index");
        return collection -> index;
    }

    /**
     * Indexes one complete book of the datalake: its metadata into the metadata store, its
     * terms into the inverted index and, last, the book into the index log. When it returns,
     * all three are written (synchronous, D2), but the book is not visible to Search until the
     * next publish.
     *
     * Idempotent: metadata are replaced by book_id, posting lists never repeat an id and the
     * log has one entry per book, so repeating the call (after a timeout or a 503) is always
     * safe. If it fails halfway, the book is not in the log yet: publish ignores it, it is not
     * INDEXED, and the retry completes it. That is why the log goes last.
     *
     * @throws BookNotFoundException    if the datalake does not have the book complete
     * @throws EmptyBookException       if the body gives no term; nothing is written
     * @throws IllegalArgumentException if bookId is not valid (BookIds): a bug of the caller,
     *                                  the API already answers 400 before calling
     */
    public UpdateResult update(int bookId) {
        Prepared book = prepare(bookId);
        write(List.of(book), currentCollection());
        return book.result();
    }

    /**
     * Indexes a batch of books (DR-09): prepares them in parallel on the parsers pool, then
     * writes the ones that are ready in one go, as update does for one. A repeated id counts
     * once. When it returns, every indexed book is complete in the index and in the log.
     *
     * @return the indexed books, in the order of bookIds, and the ones that could not be
     *         prepared with the exception update would have thrown for them
     *         (BookNotFoundException, EmptyBookException, UncheckedIOException,
     *         IllegalArgumentException...): nothing of those books was written
     * @throws InterruptedException if the calling thread is interrupted while the books are
     *                              prepared; nothing was written
     * @throws RuntimeException     what the write throws (MongoException,
     *                              MetadataRepositoryException...): none of the batch is in the
     *                              log, so none of it is complete, and repeating it is safe
     */
    public BatchResult indexBatch(List<Integer> bookIds) throws InterruptedException {
        return indexBatch(bookIds, this::currentCollection);
    }

    /**
     * indexBatch, but the posting lists go to {@code collection} whatever CURRENT says: the
     * rebuild (DR-10) fills a new collection that Search does not read yet. The metadata and the
     * log are the same stores as always.
     */
    public BatchResult indexBatchInto(String collection, List<Integer> bookIds) throws InterruptedException {
        Objects.requireNonNull(collection, "collection");
        return indexBatch(bookIds, () -> collection);
    }

    /** @param collection read just before writing, once the books are prepared */
    private BatchResult indexBatch(List<Integer> bookIds, Supplier<String> collection) throws InterruptedException {
        List<Integer> ids = List.copyOf(new LinkedHashSet<>(Objects.requireNonNull(bookIds, "bookIds")));
        List<CompletableFuture<Prepared>> parsing = new ArrayList<>(ids.size());
        for (int id : ids) {
            parsing.add(CompletableFuture.supplyAsync(() -> prepare(id), parsers));
        }
        List<Prepared> ready = new ArrayList<>(ids.size());
        Map<Integer, RuntimeException> rejected = new LinkedHashMap<>();
        for (int i = 0; i < ids.size(); i++) {
            try {
                ready.add(parsing.get(i).get());
            } catch (ExecutionException e) {
                if (e.getCause() instanceof RuntimeException failure) {
                    rejected.put(ids.get(i), failure);
                } else if (e.getCause() instanceof Error error) {
                    throw error;
                } else {
                    throw new IllegalStateException(e.getCause());
                }
            } catch (InterruptedException e) {
                parsing.forEach(f -> f.cancel(true));       // the ones not started yet never run
                throw e;
            }
        }
        if (!ready.isEmpty()) {
            write(ready, collection.get());
        }
        return new BatchResult(ready.stream().map(Prepared::result).toList(), rejected);
    }

    /** A book read, parsed and tokenized, ready to be written. */
    private record Prepared(BookMetadata metadata, Set<String> terms) {
        UpdateResult result() {
            return new UpdateResult(metadata.bookId(), terms.size());
        }
    }

    /**
     * Reads, parses and tokenizes one book: the expensive part, which runs in parallel
     * (between requests, and on the parsers pool for a batch). Writes nothing.
     */
    private Prepared prepare(int bookId) {
        if (!BookIds.isValid(bookId)) {
            throw new IllegalArgumentException("book_id fuera de rango (1.." + BookIds.MAX + "): " + bookId);
        }
        // Readiness is decided ONLY by the datalake (§6.2): the book exists when its
        // <id>._READY marker is there, written after its header and body (DR-06).
        BookLocation location = datalake.locate(bookId)
                .orElseThrow(() -> new BookNotFoundException(bookId));
        String header = read(location.headerPath());
        String body = read(location.bodyPath());
        BookMetadata book = parser.parse(location, header);
        Set<String> terms = tokenizer.uniqueTerms(body);
        // A book without terms could never be found: reject it BEFORE writing anything, so it
        // leaves no metadata in books that a later publish could make visible.
        if (terms.isEmpty()) {
            throw new EmptyBookException(bookId);
        }
        return new Prepared(book, terms);
    }

    /**
     * Writes prepared books in the order of DR-07: metadata, posting lists and LAST the log,
     * the evidence that they are complete. One call to each store, whatever the number of books.
     *
     * @param collection the collection of the inverted index the posting lists go to
     */
    private void write(List<Prepared> books, String collection) {
        InvertedIndex index = indexOf(collection);
        // 1. Metadata first, as in the plan; the repository is thread-safe.
        metadata.saveAll(books.stream().map(Prepared::metadata).toList());

        // 2. Posting lists: the inverted index is not thread-safe. One flush for all of them.
        writeLock.lock();
        try {
            for (Prepared book : books) {
                index.addDocument(book.metadata().bookId(), book.terms());
            }
            index.flush();                      // synchronous: written when this returns
        } finally {
            writeLock.unlock();                 // always, also when flush fails
        }

        // 3. Only now are the books complete in the index: record them. Outside the lock (the
        //    order is guaranteed because it is the same thread), so the lock lasts as little.
        indexLog.markIndexed(books.stream().map(b -> b.metadata().bookId()).toList(), Instant.now());
    }

    /**
     * Statistics for GET /index/status, read from the stores. Without the lock: it only reads
     * and never touches the pending map, so a status request never waits behind an update.
     * The price: the numbers are read at slightly different moments (an update may land in
     * between), which is fine for monitoring.
     */
    public IndexStatus status() {
        Optional<Publication> current = publications.current();
        long booksIndexed = indexLog.count();                 // complete books only (DR-07)
        Instant lastUpdate = indexLog.lastIndexedAt().orElse(null);
        // The collection Search reads; no fsync on the shared client
        long indexSizeBytes = indexOf(collectionOf(current)).diskUsageBytes();
        return new IndexStatus(
                booksIndexed,
                lastUpdate,
                indexSizeBytes,
                current.map(Publication::version).orElse(null),
                current.map(Publication::books).orElse(0L));
    }

    /** The collection of CURRENT, where the next write goes: one read of index_meta. */
    private String currentCollection() {
        return collectionOf(publications.current());
    }

    /** D8: the one CURRENT names; MONGO_COLLECTION if there is no CURRENT or it names none. */
    private String collectionOf(Optional<Publication> current) {
        return current.map(Publication::collection).orElse(defaultCollection);
    }

    private InvertedIndex indexOf(String collection) {
        return indexes.computeIfAbsent(collection,
                name -> Objects.requireNonNull(openIndex.apply(name), "openIndex devolvió null para " + name));
    }

    private static String read(Path file) {
        try {
            return Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("No se pudo leer " + file, e);
        }
    }
}
