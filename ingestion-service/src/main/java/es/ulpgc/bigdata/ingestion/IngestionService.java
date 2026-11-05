package es.ulpgc.bigdata.ingestion;

import es.ulpgc.bigdata.datalake.Datalake;
import es.ulpgc.bigdata.ingestion.source.BookNotFoundException;
import es.ulpgc.bigdata.ingestion.source.BookSource;
import es.ulpgc.bigdata.ingestion.source.SourceUnavailableException;
import es.ulpgc.bigdata.model.BookIds;
import es.ulpgc.bigdata.model.BookLocation;
import es.ulpgc.bigdata.model.RawBook;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * What the Ingestion Service does, without HTTP: fetch a book from the source, split it and save
 * it in the datalake. POST /ingest/{id} (IngestionApi) calls it, and so will the workers of
 * ingest-queue (CF-08): both paths run the same code.
 *
 * "In the datalake" means READY (api/contracts/datalake-readiness.md): its {@code <id>._READY}
 * marker exists together with its header and body; RangeBasedDatalake writes the marker last.
 * A book written halfway (a crash before the marker, or a file lost) is not in the datalake,
 * and the next ingestion downloads and writes it again, whole.
 *
 * A ready book is not downloaded again (ALREADY_AVAILABLE) and its files are not touched, so
 * repeating an ingestion is safe. Two concurrent ingestions of the same id may both download;
 * every file goes through its own temporary file and an atomic move, so the result is one
 * complete copy either way.
 *
 * Thread-safe if the source and the datalake are (GutenbergClient, LocalFileSource and
 * RangeBasedDatalake are).
 */
public final class IngestionService {

    public enum Outcome {
        /** Fetched from the source and saved now. */
        DOWNLOADED,
        /** Already in the datalake: nothing was fetched or written. */
        ALREADY_AVAILABLE
    }

    public record IngestResult(int bookId, Outcome outcome, BookLocation location) {
    }

    private final BookSource source;
    private final BookSplitter splitter;
    private final Datalake datalake;

    public IngestionService(BookSource source, BookSplitter splitter, Datalake datalake) {
        this.source = Objects.requireNonNull(source, "source");
        this.splitter = Objects.requireNonNull(splitter, "splitter");
        this.datalake = Objects.requireNonNull(datalake, "datalake");
    }

    /**
     * Synchronous: when it returns, the book is complete in the datalake.
     *
     * @throws BookNotFoundException      the source does not have the book (permanent)
     * @throws InvalidBookException       the text has no START/END markers (permanent)
     * @throws SourceUnavailableException the source could not be read now (transient)
     * @throws IllegalArgumentException   {@code bookId} is not a valid book id (a bug of the
     *                                    caller: the API answers 400 before calling)
     */
    public IngestResult ingest(int bookId)
            throws BookNotFoundException, InvalidBookException, SourceUnavailableException, InterruptedException {
        requireValid(bookId);
        Optional<BookLocation> existing = datalake.locate(bookId);
        if (existing.isPresent()) {
            return new IngestResult(bookId, Outcome.ALREADY_AVAILABLE, existing.get());
        }
        String raw = source.fetch(bookId);
        RawBook book = splitter.split(bookId, raw).orElseThrow(() -> new InvalidBookException(bookId));
        return new IngestResult(bookId, Outcome.DOWNLOADED, datalake.save(book));
    }

    /** Where the book is, if it is complete in the datalake. */
    public Optional<BookLocation> locate(int bookId) {
        requireValid(bookId);
        return datalake.locate(bookId);
    }

    /** Every complete book of the datalake, in ascending id order. */
    public List<Integer> list() {
        List<Integer> ids = new ArrayList<>(datalake.listBookIds());
        Collections.sort(ids);
        return ids;
    }

    /** The source in use, for GET /status. */
    public String sourceDescription() {
        return source.describe();
    }

    private static void requireValid(int bookId) {
        if (!BookIds.isValid(bookId)) {
            throw new IllegalArgumentException("invalid book id: " + bookId);
        }
    }
}
