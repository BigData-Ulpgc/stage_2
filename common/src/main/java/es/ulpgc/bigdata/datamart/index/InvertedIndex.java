package es.ulpgc.bigdata.datamart.index;

import java.util.List;
import java.util.Set;

/**
 * Inverted index: term -> ids of the books that contain it.
 *
 * The LOGICAL index is always the same; each implementation decides how
 * to store it (memory, one JSON, one file per term, MongoDB). Whoever searches
 * only uses addDocument and postings, and neither knows nor cares where the data lives.
 *
 * What flush means in each backend:
 *  - memory:       nothing. There is nothing to persist; the data is lost on close.
 *  - monolithic:   rewrites the WHOLE JSON (temp file + move), with every term.
 *  - hierarchical: writes only the files of the terms modified since the last flush.
 *  - mongo:        sends the pending updates to the database, if they were accumulated in memory.
 * In all of them: after flush, closing and reopening the index keeps everything that was added.
 *
 * Rules every implementation follows:
 *  - postings returns ids sorted from lowest to highest, without repetitions; empty list if the term does not exist.
 *  - postings reflects what was added even if no flush has been done yet.
 *  - addDocument is idempotent: adding the same book again changes nothing.
 *  - clear leaves the index empty, on disk too.
 */
public interface InvertedIndex extends AutoCloseable {

    /** Name of the structure, as in the SPEC and the benchmark CSV: "memory", "monolithic"... */
    String name();

    /** Adds the book to the posting list of each term (already tokenized). */
    void addDocument(int bookId, Set<String> terms);

    /** Ids of the books that contain the term: sorted, without repetitions, unmodifiable. */
    List<Integer> postings(String term);

    /** Makes everything added so far persistent. See the table in the interface comment. */
    void flush();

    /** Empties the index completely, in memory and on disk. */
    void clear();

    /** Bytes the index takes on disk right now (0 if it does not use disk). For the benchmark. */
    long diskUsageBytes();

    /** Releases resources (connections, open files). Does not imply flush. */
    @Override
    void close();
}