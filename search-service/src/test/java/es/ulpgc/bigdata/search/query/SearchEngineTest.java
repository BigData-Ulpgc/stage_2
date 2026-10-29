package es.ulpgc.bigdata.search.query;

import es.ulpgc.bigdata.datamart.index.Tokenizer;
import es.ulpgc.bigdata.model.BookMetadata;
import es.ulpgc.bigdata.model.YearSource;
import es.ulpgc.bigdata.search.index.IndexSnapshot;
import es.ulpgc.bigdata.search.index.PostingSource;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** AND, published-only, filters and pagination, with posting lists given in memory. */
class SearchEngineTest {

    /** Posting lists as MongoDB would return them (book 9 is indexed but NOT published). */
    private static final Map<String, int[]> INDEX = Map.of(
            "river", new int[]{2, 3, 4, 9},
            "adventure", new int[]{1, 2, 3, 9},
            "whale", new int[]{9});

    private static final IndexSnapshot SNAPSHOT = new IndexSnapshot(7, "inverted_index", List.of(
            book(1, "Jane Austen", "en", 1998),
            book(2, "Mark Twain", "en", 2004),
            book(3, "Mark Twain", "en", 2004),
            book(4, "Victor Hugo", "fr", 2006)));

    private final List<String> collectionsAsked = new ArrayList<>();
    private final PostingSource source = (collection, terms) -> {
        collectionsAsked.add(collection);
        Map<String, int[]> found = new HashMap<>();
        terms.forEach(t -> { if (INDEX.containsKey(t)) found.put(t, INDEX.get(t)); });
        return found;
    };
    private final SearchEngine engine = new SearchEngine(source);

    private static BookMetadata book(int id, String author, String language, int year) {
        return new BookMetadata(id, "Book " + id, author, language, year, YearSource.RELEASE_DATE, null, null, null);
    }

    private SearchEngine.Result search(String... nameValue) {
        Map<String, List<String>> params = new HashMap<>();
        for (int i = 0; i < nameValue.length; i += 2) {
            params.put(nameValue[i], List.of(nameValue[i + 1]));
        }
        return engine.search(SNAPSHOT, SearchQuery.parse(params, Tokenizer.standard()));
    }

    private static List<Integer> ids(SearchEngine.Result result) {
        return result.page().stream().map(BookMetadata::bookId).toList();
    }

    @Test
    void oneTermReturnsItsPublishedBooksSortedById() {
        SearchEngine.Result result = search("q", "river");

        assertEquals(List.of(2, 3, 4), ids(result));                  // 9 is not published
        assertEquals(3, result.total());
        assertEquals(7, result.indexVersion());
        assertEquals(List.of("inverted_index"), collectionsAsked);    // the snapshot's collection, once
    }

    @Test
    void severalTermsAreIntersected() {
        assertEquals(List.of(2, 3), ids(search("q", "river adventure")));
    }

    @Test
    void aTermOnlyInUnpublishedBooksGivesNothing() {
        SearchEngine.Result result = search("q", "whale");

        assertEquals(0, result.total());
        assertTrue(result.page().isEmpty());
    }

    @Test
    void aTermNotInTheIndexGivesNothingEvenWithOtherTerms() {
        assertEquals(0, search("q", "river zzzzqqq").total());
    }

    @Test
    void filtersApplyOnTheSnapshotMetadata() {
        assertEquals(List.of(2, 3), ids(search("q", "river", "author", "twain")));
        assertEquals(List.of(4), ids(search("q", "river", "language", "fr")));
        assertEquals(List.of(1), ids(search("q", "adventure", "year", "1998")));
        assertEquals(List.of(), ids(search("q", "river", "author", "twain", "language", "fr")));
    }

    @Test
    void totalCountsEveryMatchAndThePageIsCut() {
        SearchEngine.Result second = search("q", "river", "offset", "1", "limit", "1");

        assertEquals(3, second.total());
        assertEquals(List.of(3), ids(second));
    }

    @Test
    void anOffsetBeyondTheTotalGivesAnEmptyPage() {
        SearchEngine.Result result = search("q", "river", "offset", "50");

        assertEquals(3, result.total());
        assertTrue(result.page().isEmpty());
    }
}
