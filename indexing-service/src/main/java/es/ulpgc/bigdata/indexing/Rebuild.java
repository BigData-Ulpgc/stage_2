package es.ulpgc.bigdata.indexing;

import java.time.Instant;
import java.util.Objects;

/**
 * The last rebuild of the index (DR-10), as index_meta REBUILD keeps it. It is in MongoDB, not
 * in the memory of the replica that runs it, because indexing:7002 sends each request to any
 * replica: GET /index/status shows the same progress in all of them.
 *
 * @param state       running, done or failed
 * @param startedAt   when POST /index/rebuild started it
 * @param finishedAt  when it ended; null while it runs
 * @param processed   books indexed so far into the new collection
 * @param total       complete books found in the datalake when it started
 * @param error       why it failed, in English (it goes in the response); null unless failed
 * @param collection  the collection it fills (inverted_index_rNN); never reused, also if it failed
 * @param pendingDrop the collection a finished rebuild left to drop, Search may still read it;
 *                    null if none
 * @param dropAfter   when pendingDrop may be dropped; null if none
 */
public record Rebuild(State state, Instant startedAt, Instant finishedAt, int processed, int total, String error,
                      String collection, String pendingDrop, Instant dropAfter) {

    public enum State { RUNNING, DONE, FAILED }

    public Rebuild {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(startedAt, "startedAt");
    }

    Rebuild withProgress(int processed, int total, String collection) {
        return new Rebuild(state, startedAt, finishedAt, processed, total, error, collection, pendingDrop, dropAfter);
    }

    Rebuild done(Instant at, String previous, Instant dropAfter) {
        return new Rebuild(State.DONE, startedAt, at, processed, total, null, collection, previous, dropAfter);
    }

    Rebuild failed(Instant at, String why) {
        return new Rebuild(State.FAILED, startedAt, at, processed, total, why, collection, pendingDrop, dropAfter);
    }

    /** The collection left to drop, if it still has to wait at {@code now}. */
    boolean keepsPendingDrop(Instant now) {
        return pendingDrop != null && dropAfter != null && now.isBefore(dropAfter);
    }
}
