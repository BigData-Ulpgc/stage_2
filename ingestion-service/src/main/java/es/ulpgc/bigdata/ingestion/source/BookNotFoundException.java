package es.ulpgc.bigdata.ingestion.source;

/** The source does not have the book: a permanent error, retrying cannot help (NOT_FOUND). */
public final class BookNotFoundException extends Exception {

    private final int bookId;

    public BookNotFoundException(int bookId, String detail) {
        super("book " + bookId + " not found: " + detail);
        this.bookId = bookId;
    }

    public int bookId() {
        return bookId;
    }
}
