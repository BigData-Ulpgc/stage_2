package es.ulpgc.bigdata.coordination;

/** What {@link Coordination#fail} did with the book. */
public enum FailOutcome {
    /** Back in the queue of its stage for another attempt (T4, T8). */
    REQUEUED,
    /** In FAILED: permanent error or no attempts left (T5, T9). */
    FAILED,
    /**
     * Nothing: the entry was no longer the worker's claim (the Control requeued it after the
     * lease expired, or a recovery rebuilt it). The worker just drops the book.
     */
    LEASE_LOST
}
