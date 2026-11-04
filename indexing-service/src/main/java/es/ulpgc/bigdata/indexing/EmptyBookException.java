package es.ulpgc.bigdata.indexing;

/**
 * The book is complete in the datalake but its body gives no term at all: it is empty, has only
 * stopwords and punctuation, or is written only in characters the tokenizer does not keep
 * (it keeps ASCII letters and digits). No search could ever find it, so it is not indexed and
 * nothing is written. The API answers 422 EMPTY_BOOK; repeating the call gives the same answer
 * until the book changes in the datalake.
 */
public class EmptyBookException extends RuntimeException {

    private final int bookId;

    public EmptyBookException(int bookId) {
        super("El libro " + bookId + " no tiene ningún término indexable");
        this.bookId = bookId;
    }

    public int bookId() {
        return bookId;
    }
}
