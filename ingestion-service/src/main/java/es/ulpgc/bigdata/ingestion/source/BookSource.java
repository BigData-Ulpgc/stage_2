package es.ulpgc.bigdata.ingestion.source;

/**
 * Where the raw text of a book comes from: Project Gutenberg (GutenbergClient) or a local
 * folder (LocalFileSource), chosen with INGESTION_SOURCE (BookSources).
 *
 * The two kinds of failure are different exceptions because the coordination contract treats
 * them differently (api/contracts/coordination.md §4.3): a book the source does not have is a
 * PERMANENT error (NOT_FOUND, no retry), anything else is TRANSIENT (DOWNLOAD_FAILED, the book
 * is retried). In Stage 1 both were an empty Optional and could not be told apart.
 */
@FunctionalInterface
public interface BookSource {

    /**
     * @return the raw text, untouched (the split comes later, BookSplitter)
     * @throws BookNotFoundException       the source does not have this book (HTTP 404, no file)
     * @throws SourceUnavailableException  it could not be read now: timeout, network, HTTP 5xx, I/O
     */
    String fetch(int bookId) throws BookNotFoundException, SourceUnavailableException, InterruptedException;

    /** One line for the startup log and GET /status, e.g. "gutenberg https://www.gutenberg.org/cache/epub". */
    default String describe() {
        return getClass().getSimpleName();
    }
}
