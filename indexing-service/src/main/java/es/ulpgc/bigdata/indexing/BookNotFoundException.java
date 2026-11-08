package es.ulpgc.bigdata.indexing;

/**
 * The book is not complete in the datalake: it was never ingested, or it is still being
 * written. The API answers 404 NOT_FOUND; whoever called can try again once Ingestion has
 * finished the book.
 */
public class BookNotFoundException extends RuntimeException {

    private final int bookId;

    public BookNotFoundException(int bookId) {
        super("El libro " + bookId + " no está completo en el datalake");
        this.bookId = bookId;
    }

    public int bookId() {
        return bookId;
    }
}
