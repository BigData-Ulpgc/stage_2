package es.ulpgc.bigdata.indexing;

import es.ulpgc.bigdata.datalake.Datalake;
import es.ulpgc.bigdata.datamart.index.InvertedIndex;
import es.ulpgc.bigdata.datamart.index.Tokenizer;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Core of the Indexing Service, without anything of HTTP (DR-05, D8): IndexingApi calls it
 * for POST /index/update/{id}, and the index-queue consumer will call the same method
 * (DR-06), so both paths index exactly the same way.
 *
 * Thread safety (D5): Jetty serves every request in its own thread. Reading, parsing and
 * tokenizing run in parallel; the metadata repository is thread-safe (a MongoCollection and
 * no state of its own). Only the inverted index is not (its pending map), so addDocument +
 * flush run under a lock: one writer at a time in this process. DR-09 replaces it with a
 * writer thread with batches plus Hazelcast's index-writer lock, which also coordinates
 * different replicas.
 *
 * Errors are exceptions with a meaning; IndexingApi turns them into HTTP codes:
 *  - BookNotFoundException:                    the book is not complete in the datalake (404);
 *  - MongoException, MetadataRepositoryException: MongoDB does not answer (503);
 *  - UncheckedIOException:                     the book files could not be read (500);
 *  - IllegalStateException:                    CURRENT does not follow the contract (500).
 */
public final class IndexingService {

    private final Datalake datalake;
    private final MetadataRepository metadata;
    private final InvertedIndex index;
    private final PublicationRepository publications;
    private final MetadataParser parser;
    private final Tokenizer tokenizer;

    /** Only one thread at a time touches the inverted index. */
    private final ReentrantLock writeLock = new ReentrantLock();

    /**
     * End of the last successful update of THIS process; null if none. Several request threads
     * write it, hence atomic. Provisional until DR-07, where index_log keeps it across restarts.
     */
    private final AtomicReference<Instant> lastUpdate = new AtomicReference<>();

    public IndexingService(Datalake datalake, MetadataRepository metadata, InvertedIndex index,
                           PublicationRepository publications, MetadataParser parser, Tokenizer tokenizer) {
        this.datalake = Objects.requireNonNull(datalake, "datalake");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.index = Objects.requireNonNull(index, "index");
        this.publications = Objects.requireNonNull(publications, "publications");
        this.parser = Objects.requireNonNull(parser, "parser");
        this.tokenizer = Objects.requireNonNull(tokenizer, "tokenizer");
    }

    /**
     * Indexes one complete book of the datalake: its metadata into the metadata store and its
     * terms into the inverted index. When it returns, both are written (synchronous, D2), but
     * the book is not visible to Search until the next publish.
     *
     * Idempotent: metadata are replaced by book_id and posting lists never repeat an id, so
     * repeating the call (after a timeout or a 503) is always safe. The write order does not
     * matter for consistency: if it fails halfway (metadata saved, postings not), nothing is
     * visible because only publish makes books visible, and the retry completes it.
     *
     * @throws BookNotFoundException    if the datalake does not have the book complete
     * @throws IllegalArgumentException if bookId is not valid (BookIds): a bug of the caller,
     *                                  the API already answers 400 before calling
     */
    public UpdateResult update(int bookId) {
        if (!BookIds.isValid(bookId)) {
            throw new IllegalArgumentException("book_id fuera de rango (1.." + BookIds.MAX + "): " + bookId);
        }
        // 1. Readiness is decided ONLY by the datalake (§6.2): header + body today, also the
        //    <id>._READY marker after DR-06, without changing this method.
        BookLocation location = datalake.locate(bookId)
                .orElseThrow(() -> new BookNotFoundException(bookId));

        // 2. Outside the lock: the expensive part, in parallel between requests.
        String header = read(location.headerPath());
        String body = read(location.bodyPath());
        BookMetadata book = parser.parse(location, header);
        Set<String> terms = tokenizer.uniqueTerms(body);

        // 3. Metadata first, as in the plan; the repository is thread-safe.
        metadata.save(book);

        // 4. Posting lists: the inverted index is not thread-safe.
        writeLock.lock();
        try {
            index.addDocument(bookId, terms);
            index.flush();                      // synchronous: written when update returns
        } finally {
            writeLock.unlock();                 // always, also when flush fails
        }
        lastUpdate.set(Instant.now());          // only reached if everything was written
        return new UpdateResult(bookId, terms.size());
    }

    /**
     * Statistics for GET /index/status, read from the stores. Without the lock: it only reads
     * and never touches the pending map, so a status request never waits behind an update.
     * The price: the numbers are read at slightly different moments (an update may land in
     * between), which is fine for monitoring.
     */
    public IndexStatus status() {
        long booksIndexed = metadata.count();                 // provisional until DR-07 (index_log)
        long indexSizeBytes = index.diskUsageBytes();         // no fsync on the shared client
        Optional<Publication> current = publications.current();
        return new IndexStatus(
                booksIndexed,
                lastUpdate.get(),
                indexSizeBytes,
                current.map(Publication::version).orElse(null),
                current.map(Publication::books).orElse(0L));
    }

    private static String read(Path file) {
        try {
            return Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("No se pudo leer " + file, e);
        }
    }
}
