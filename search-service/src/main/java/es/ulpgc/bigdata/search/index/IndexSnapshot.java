package es.ulpgc.bigdata.search.index;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;

/**
 * One published version of the index, loaded in memory and never modified afterwards.
 *
 * Immutability is what makes concurrent searches safe without locks: every request takes
 * a reference to a snapshot and uses only that one, so a response never mixes two index
 * versions. A new version is loaded into a NEW snapshot and swapped in (DP-06).
 */
public final class IndexSnapshot {

    private final String version;
    private final Map<String, int[]> postings;     // term -> ids, ascending, no duplicates
    private final Map<Integer, BookRecord> books;  // every book id that appears in the index
    private final SortedSet<Integer> bookIds;

    IndexSnapshot(String version, Map<String, int[]> postings,
                  Map<Integer, BookRecord> books, SortedSet<Integer> bookIds) {
        this.version = version;
        this.postings = Collections.unmodifiableMap(postings);
        this.books = Collections.unmodifiableMap(books);
        this.bookIds = Collections.unmodifiableSortedSet(bookIds);
    }

    /** Version name as written in CURRENT, e.g. "v000001". */
    public String version() {
        return version;
    }

    /** Ids of the books containing the term, ascending; empty if the term is unknown. */
    public List<Integer> postings(String term) {
        int[] ids = postings.get(term);
        return ids == null ? List.of() : Arrays.stream(ids).boxed().toList();
    }

    /** Metadata of a book of this version; empty if the book is not in the published index. */
    public Optional<BookRecord> book(int bookId) {
        return Optional.ofNullable(books.get(bookId));
    }

    /** All book ids of this version, ascending. */
    public SortedSet<Integer> bookIds() {
        return bookIds;
    }

    public int bookCount() {
        return bookIds.size();
    }

    public int termCount() {
        return postings.size();
    }
}
