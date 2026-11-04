package es.ulpgc.bigdata.coordination;

import java.util.Objects;

/**
 * The value of book-state for one book (contract §3).
 *
 * Stored with Compact serialization: Hazelcast serializes a record field by field, and the
 * members (official image, without our classes) can store, compare and query it. Two rules
 * follow from that:
 * <ul>
 *   <li>Every service must use THIS class: the Compact type name is the fully qualified class
 *       name, so a copy in another package would be a different type.</li>
 *   <li>Adding, removing or renaming a field changes the format: empty the cluster after such
 *       a change (contract header).</li>
 * </ul>
 *
 * Immutable: a transition builds a new value with one of the methods below, and Coordination
 * writes it with replace(id, expected, next), which succeeds only if nobody changed the entry
 * since it was read. The methods are package-private: only Coordination makes transitions.
 *
 * @param bookId      the key, repeated so that a value is self-describing
 * @param state       a {@link BookState} name
 * @param owner       worker that holds the lease ({@code <service>-<hostname>/w<n>}); null
 *                    outside INGESTING and INDEXING
 * @param leaseUntil  epoch ms until which the owner may work on the book; 0 without owner
 * @param attempts    failed attempts in the current stage; back to 0 when a stage completes
 * @param failedStage a {@link Stage} name when state is FAILED, else null
 * @param lastError   {@code <CODE>: <message>} of the last failure, kept for diagnosis
 * @param enqueuedAt  epoch ms when the id entered its current queue
 * @param startedAt   epoch ms of the claim of the current or last stage
 * @param finishedAt  epoch ms when the last stage completed
 */
public record BookTask(int bookId, String state, String owner, long leaseUntil, int attempts,
                       String failedStage, String lastError, long enqueuedAt, long startedAt,
                       long finishedAt) {

    /** Longest message kept in lastError: it travels with every read of the entry. */
    static final int MAX_ERROR_LENGTH = 300;

    public BookTask {
        Objects.requireNonNull(state, "state");
        BookState.valueOf(state);                       // fails on a name that is not a state
        if (failedStage != null) {
            Stage.valueOf(failedStage);
        }
    }

    /** The state as an enum. */
    public BookState bookState() {
        return BookState.valueOf(state);
    }

    /** T1: a seeded book, just offered to ingest-queue. */
    static BookTask pending(int bookId, long now) {
        return new BookTask(bookId, BookState.PENDING.name(), null, 0, 0, null, null, now, 0, 0);
    }

    /** T2, T6: claimed by {@code owner} until {@code leaseUntil}. */
    BookTask claimed(Stage stage, String owner, long leaseUntil, long now) {
        return new BookTask(bookId, stage.working().name(), owner, leaseUntil, attempts, null,
                lastError, enqueuedAt, now, finishedAt);
    }

    /** The same claim with a later lease. */
    BookTask renewed(long newLeaseUntil) {
        return new BookTask(bookId, state, owner, newLeaseUntil, attempts, failedStage, lastError,
                enqueuedAt, startedAt, finishedAt);
    }

    /** T3, T7: the stage is complete; {@code enqueuedNow} when the id goes on to the next queue. */
    BookTask completed(Stage stage, long now, boolean enqueuedNow) {
        return new BookTask(bookId, stage.done().name(), null, 0, 0, null, lastError,
                enqueuedNow ? now : enqueuedAt, startedAt, now);
    }

    /** T4, T8: back to the queue of the stage after a failed attempt. */
    BookTask requeued(Stage stage, int newAttempts, String error, long now) {
        return new BookTask(bookId, stage.waiting().name(), null, 0, newAttempts, null, error, now,
                startedAt, finishedAt);
    }

    /** T5, T9: no more attempts. */
    BookTask failed(Stage stage, int newAttempts, String error) {
        return new BookTask(bookId, BookState.FAILED.name(), null, 0, newAttempts, stage.name(),
                error, enqueuedAt, startedAt, finishedAt);
    }

    /** T10: visible to Search. */
    BookTask published() {
        return new BookTask(bookId, BookState.PUBLISHED.name(), null, 0, 0, null, lastError,
                enqueuedAt, startedAt, finishedAt);
    }

    /** T11: back to the start of the stage that failed, with its attempts reset (lastError kept). */
    BookTask retried(long now) {
        Stage stage = Stage.valueOf(failedStage);
        return new BookTask(bookId, stage.waiting().name(), null, 0, 0, null, lastError, now,
                startedAt, finishedAt);
    }

    /** {@code CODE: message}, cut to {@link #MAX_ERROR_LENGTH} characters. */
    static String error(ErrorCode code, String message) {
        String text = message == null || message.isBlank() ? code.name() : code.name() + ": " + message.strip();
        return text.length() <= MAX_ERROR_LENGTH ? text : text.substring(0, MAX_ERROR_LENGTH);
    }
}
