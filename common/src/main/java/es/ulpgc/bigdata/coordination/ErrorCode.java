package es.ulpgc.bigdata.coordination;

/**
 * Why a stage failed (contract §4.3). The kind decides what {@link Coordination#fail} does:
 *
 * <ul>
 *   <li>PERMANENT: retrying cannot help, the book goes to FAILED at once.</li>
 *   <li>TRANSIENT: one failed attempt; the book is requeued until MAX_ATTEMPTS.</li>
 *   <li>OUTAGE: not the book's fault (MongoDB down). Requeued WITHOUT counting an attempt,
 *       otherwise one outage would send every book in flight to FAILED.</li>
 * </ul>
 */
public enum ErrorCode {
    /** Ingestion: Gutenberg answers 404. */
    NOT_FOUND(Kind.PERMANENT),
    /** Ingestion: no START / END markers. */
    INVALID_BOOK(Kind.PERMANENT),
    /** Indexing: no indexable term. */
    EMPTY_BOOK(Kind.PERMANENT),
    /** Ingestion: timeout, connection error or 5xx of Gutenberg, after its own retries. */
    DOWNLOAD_FAILED(Kind.TRANSIENT),
    /** Indexing: the book has no {@code <id>._READY}. */
    NOT_READY(Kind.TRANSIENT),
    /** Control: the owner did not finish before its lease expired. */
    LEASE_EXPIRED(Kind.TRANSIENT),
    /** Anyone: unexpected exception. */
    INTERNAL_ERROR(Kind.TRANSIENT),
    /** Indexing: MongoDB does not answer. */
    STORAGE_UNAVAILABLE(Kind.OUTAGE);

    public enum Kind { PERMANENT, TRANSIENT, OUTAGE }

    private final Kind kind;

    ErrorCode(Kind kind) {
        this.kind = kind;
    }

    public Kind kind() {
        return kind;
    }
}
