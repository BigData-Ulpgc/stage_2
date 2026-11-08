package es.ulpgc.bigdata.search.index;

import es.ulpgc.bigdata.model.BookMetadata;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The snapshot alone, without MongoDB. */
class IndexSnapshotTest {

    static BookMetadata book(int id) {
        return new BookMetadata(id, "Title " + id, null, "en", null, null, null, null, null);
    }

    @Test
    void knowsWhichBooksArePublished() {
        IndexSnapshot snapshot = new IndexSnapshot(42, "inverted_index", List.of(book(1342), book(76)));

        assertEquals(42, snapshot.version());
        assertEquals("inverted_index", snapshot.collection());
        assertTrue(snapshot.isPublished(1342));
        assertFalse(snapshot.isPublished(2701));
        assertEquals("Title 76", snapshot.book(76).orElseThrow().title());
        assertTrue(snapshot.book(2701).isEmpty());
        assertEquals(2, snapshot.bookCount());
    }

    @Test
    void booksAreSortedByIdWhateverTheInputOrder() {
        IndexSnapshot snapshot = new IndexSnapshot(1, "c", List.of(book(1342), book(5), book(76)));

        assertEquals(List.of(5, 76, 1342), snapshot.books().stream().map(BookMetadata::bookId).toList());
    }

    @Test
    void filterValuesCountLanguagesAndSpanTheYears() {
        IndexSnapshot snapshot = new IndexSnapshot(1, "c", List.of(
                withLanguageAndYear(1, "en", 1998), withLanguageAndYear(2, "fr", 2006),
                withLanguageAndYear(3, "en", 1926), withLanguageAndYear(4, "de", null),
                withLanguageAndYear(5, null, 2004)));

        IndexSnapshot.FilterValues values = snapshot.filterValues();

        assertEquals(List.of("en", "de", "fr"), List.copyOf(values.languages().keySet()));   // 2, then ties by code
        assertEquals(2, values.languages().get("en"));
        assertEquals(1926, values.minYear());
        assertEquals(2006, values.maxYear());
    }

    @Test
    void withoutYearsTheRangeIsNull() {
        IndexSnapshot.FilterValues values = new IndexSnapshot(1, "c", List.of(book(1))).filterValues();

        assertEquals(null, values.minYear());
        assertEquals(null, values.maxYear());
    }

    private static BookMetadata withLanguageAndYear(int id, String language, Integer year) {
        return new BookMetadata(id, null, null, language, year,
                year == null ? null : es.ulpgc.bigdata.model.YearSource.RELEASE_DATE, null, null, null);
    }

    @Test
    void cannotBeChangedFromOutside() {
        List<BookMetadata> input = new ArrayList<>(List.of(book(1)));
        IndexSnapshot snapshot = new IndexSnapshot(1, "c", input);

        input.add(book(2));                                  // the caller keeps its list...

        assertFalse(snapshot.isPublished(2));                // ...but the snapshot does not see it
        assertThrows(UnsupportedOperationException.class, () -> snapshot.books().clear());
    }
}
