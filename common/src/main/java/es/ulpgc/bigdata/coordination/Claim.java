package es.ulpgc.bigdata.coordination;

/**
 * A book a worker has claimed: what {@link Coordination#claim} returns and what the worker
 * gives back to {@link Coordination#complete} or {@link Coordination#fail}.
 *
 * {@code task} is the exact value the claim wrote. Completing compares the entry with it: if
 * the Control has requeued the book in the meantime (expired lease), the comparison fails and
 * the worker knows it has lost the book.
 */
public record Claim(Stage stage, BookTask task) {

    public int bookId() {
        return task.bookId();
    }
}
