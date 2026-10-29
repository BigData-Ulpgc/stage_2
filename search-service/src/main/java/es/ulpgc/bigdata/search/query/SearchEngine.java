package es.ulpgc.bigdata.search.query;

import es.ulpgc.bigdata.model.BookMetadata;
import es.ulpgc.bigdata.search.index.IndexSnapshot;
import es.ulpgc.bigdata.search.index.PostingSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Runs a validated query on ONE snapshot:
 *
 * <ol>
 *   <li>reads the posting lists of the terms (one MongoDB query, PostingSource);</li>
 *   <li>intersects them (AND): walks the SHORTEST list and keeps an id only if every other
 *       list has it (binary search), so the cost depends on the rarest term;</li>
 *   <li>drops the ids that are not published in the snapshot;</li>
 *   <li>applies the filters on the snapshot metadata, in memory;</li>
 *   <li>cuts the requested page.</li>
 * </ol>
 *
 * The shortest list is sorted, so the matches come out sorted by book_id with no extra sort.
 * Thread-safe: no state of its own; the snapshot is immutable.
 */
public final class SearchEngine {

    private final PostingSource postings;

    public SearchEngine(PostingSource postings) {
        this.postings = Objects.requireNonNull(postings, "postings");
    }

    /** All matches counted in total; only the page [offset, offset + limit) returned. */
    public record Result(int indexVersion, int total, List<BookMetadata> page) {
    }

    public Result search(IndexSnapshot snapshot, SearchQuery query) {
        Map<String, int[]> lists = postings.postings(snapshot.collection(), query.terms());
        if (lists.size() < query.terms().size()) {
            return new Result(snapshot.version(), 0, List.of());     // some term is in no book: AND gives nothing
        }
        int[][] byLength = lists.values().toArray(new int[0][]);
        Arrays.sort(byLength, Comparator.comparingInt(list -> list.length));

        List<BookMetadata> matches = new ArrayList<>();
        int[] shortest = byLength[0];
        candidates:
        for (int id : shortest) {
            for (int i = 1; i < byLength.length; i++) {
                if (Arrays.binarySearch(byLength[i], id) < 0) {
                    continue candidates;
                }
            }
            BookMetadata book = snapshot.book(id).orElse(null);    // null: indexed but not published
            if (book != null && query.accepts(book)) {
                matches.add(book);
            }
        }
        int from = Math.min(query.offset(), matches.size());
        int to = Math.min(from + query.limit(), matches.size());
        return new Result(snapshot.version(), matches.size(), List.copyOf(matches.subList(from, to)));
    }
}
