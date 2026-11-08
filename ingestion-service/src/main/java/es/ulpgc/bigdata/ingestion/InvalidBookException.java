package es.ulpgc.bigdata.ingestion;

/**
 * The source served the book, but its text has no "*** START OF" / "*** END OF" markers, so it
 * cannot be split into header and body. Permanent: downloading it again gives the same text
 * (422 INVALID_BOOK in the API, INVALID_BOOK in the coordination contract). Nothing is written.
 */
public final class InvalidBookException extends Exception {

    private final int bookId;

    public InvalidBookException(int bookId) {
        super("book " + bookId + " has no Project Gutenberg START/END markers");
        this.bookId = bookId;
    }

    public int bookId() {
        return bookId;
    }
}
