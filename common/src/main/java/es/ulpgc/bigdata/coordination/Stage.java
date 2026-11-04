package es.ulpgc.bigdata.coordination;

/**
 * The two stages a worker claims books for. Each one takes ids from its queue in the
 * {@code waiting} state, holds them {@code working} with a lease, and leaves them {@code done}.
 *
 * <pre>
 *   INGESTION   ingest-queue   PENDING    -> INGESTING -> DOWNLOADED (then offered to index-queue)
 *   INDEXING    index-queue    DOWNLOADED -> INDEXING  -> INDEXED
 * </pre>
 */
public enum Stage {
    INGESTION(CoordinationNames.INGEST_QUEUE, BookState.PENDING, BookState.INGESTING, BookState.DOWNLOADED),
    INDEXING(CoordinationNames.INDEX_QUEUE, BookState.DOWNLOADED, BookState.INDEXING, BookState.INDEXED);

    private final String queue;
    private final BookState waiting;
    private final BookState working;
    private final BookState done;

    Stage(String queue, BookState waiting, BookState working, BookState done) {
        this.queue = queue;
        this.waiting = waiting;
        this.working = working;
        this.done = done;
    }

    /** The queue this stage takes its ids from (and puts them back to on a retry). */
    public String queue() {
        return queue;
    }

    /** State of a book waiting in {@link #queue()}. */
    public BookState waiting() {
        return waiting;
    }

    /** State while a worker holds the lease. */
    public BookState working() {
        return working;
    }

    /** State when the stage is complete. */
    public BookState done() {
        return done;
    }

    /** The stage whose worker holds a book in this state (INGESTING or INDEXING). */
    static Stage ofWorking(BookState state) {
        for (Stage stage : values()) {
            if (stage.working == state) {
                return stage;
            }
        }
        throw new IllegalArgumentException("no stage works on " + state);
    }
}
