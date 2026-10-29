package es.ulpgc.bigdata.search.query;

import es.ulpgc.bigdata.datamart.index.Tokenizer;
import es.ulpgc.bigdata.model.BookMetadata;
import es.ulpgc.bigdata.model.YearSource;
import es.ulpgc.bigdata.search.http.ApiException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Every validation rule of GET /search in api/openapi.yaml, without MongoDB. */
class SearchQueryTest {

    private static final Tokenizer TOKENIZER = Tokenizer.standard();

    private static SearchQuery parse(String... nameValue) {
        Map<String, List<String>> params = new HashMap<>();
        for (int i = 0; i < nameValue.length; i += 2) {
            params.computeIfAbsent(nameValue[i], k -> new java.util.ArrayList<>()).add(nameValue[i + 1]);
        }
        return SearchQuery.parse(params, TOKENIZER);
    }

    private static ApiException rejected(String... nameValue) {
        return assertThrows(ApiException.class, () -> parse(nameValue));
    }

    @Test
    void onlyQGivesTheDefaults() {
        SearchQuery query = parse("q", "Pride and Prejudice");

        assertEquals("Pride and Prejudice", query.q());
        assertEquals(Set.of("pride", "prejudice"), query.terms());      // "and" is a stopword
        assertNull(query.author());
        assertNull(query.language());
        assertNull(query.year());
        assertEquals(0, query.offset());
        assertEquals(20, query.limit());
    }

    @Test
    void everyParameterIsRead() {
        SearchQuery query = parse("q", "river", "author", " Twain ", "language", "en", "year", "2004",
                "offset", "1", "limit", "1");

        assertEquals("twain", query.author());                         // trimmed and lower-cased once
        assertEquals("en", query.language());
        assertEquals(2004, query.year());
        assertEquals(1, query.offset());
        assertEquals(1, query.limit());
    }

    @Test
    void qMissingOrBlankIsMissingParameter() {
        ApiException missing = rejected();
        assertEquals(400, missing.status());
        assertEquals("MISSING_PARAMETER", missing.code());
        assertEquals("q", missing.field());

        assertEquals("MISSING_PARAMETER", rejected("q", "   ").code());
    }

    @Test
    void qWithOnlyStopwordsOrShortWordsHasNoTerms() {
        ApiException e = rejected("q", "the of a I");

        assertEquals("INVALID_PARAMETER", e.code());
        assertEquals("q", e.field());
        assertTrue(e.getMessage().contains("no searchable terms"));
    }

    @Test
    void qLongerThan200CharactersIsRejected() {
        assertEquals("q", rejected("q", "a".repeat(201)).field());
    }

    @ParameterizedTest(name = "{0}={1} -> 400")
    @CsvSource({
            "language, EN", "language, english", "language, xx",
            "year, 98", "year, 12345", "year, abc", "year, 1998.0",
            "offset, -1", "offset, one",
            "limit, 0", "limit, 101", "limit, ten",
            "author, ' '"
    })
    void invalidValuesAreRejectedNamingTheField(String name, String value) {
        ApiException e = rejected("q", "river", name, value);

        assertEquals(400, e.status());
        assertEquals("INVALID_PARAMETER", e.code());
        assertEquals(name, e.field());
    }

    @Test
    void theLanguageErrorListsTheSupportedCodes() {
        assertEquals("language must be one of: da, de, el, en, eo, es, fi, fr, it, la, nl, pt, ru, sv, zh",
                rejected("q", "river", "language", "xx").getMessage());
    }

    @Test
    void aRepeatedParameterUsesTheFirstValue() {
        assertEquals(5, parse("q", "river", "limit", "5", "limit", "500").limit());
    }

    @Test
    void filtersAreCombinedWithAnd() {
        BookMetadata huck = new BookMetadata(76, "Huck", "Mark Twain", "en", 2004, YearSource.RELEASE_DATE, null, null, null);
        BookMetadata noAuthor = new BookMetadata(5, "Constitution", null, "en", 1975, YearSource.RELEASE_DATE, null, null, null);

        assertTrue(parse("q", "x1", "author", "TWAIN").accepts(huck));            // substring, any case
        assertTrue(parse("q", "x1", "author", "twain", "language", "en", "year", "2004").accepts(huck));
        assertFalse(parse("q", "x1", "author", "twain", "year", "2005").accepts(huck));
        assertFalse(parse("q", "x1", "language", "fr").accepts(huck));
        assertFalse(parse("q", "x1", "author", "twain").accepts(noAuthor));        // null author never matches
    }
}
