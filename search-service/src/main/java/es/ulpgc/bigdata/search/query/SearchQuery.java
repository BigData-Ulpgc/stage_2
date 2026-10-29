package es.ulpgc.bigdata.search.query;

import es.ulpgc.bigdata.datamart.index.Tokenizer;
import es.ulpgc.bigdata.datamart.metadata.LanguageCodes;
import es.ulpgc.bigdata.model.BookMetadata;
import es.ulpgc.bigdata.search.http.ApiException;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A validated GET /search request (api/openapi.yaml). Building one is the ONLY way to get
 * one, and every rule is checked there, so the search code never sees an invalid value.
 *
 * @param q        the q parameter as received (echoed in the response)
 * @param terms    the terms of q after tokenizing it with the SAME tokenizer as Indexing
 * @param author   lower-cased substring to look for in the author, or null (no filter)
 * @param language ISO 639-1 code, or null
 * @param year     4-digit year, or null
 */
public record SearchQuery(String q, Set<String> terms, String author, String language, Integer year,
                          int offset, int limit) {

    public static final int DEFAULT_LIMIT = 20;
    public static final int MAX_LIMIT = 100;
    static final int MAX_Q_LENGTH = 200;
    static final int MAX_AUTHOR_LENGTH = 100;

    public SearchQuery {
        Objects.requireNonNull(q, "q");
        terms = Set.copyOf(terms);
    }

    /**
     * Validates the query parameters, in the order of the contract. The first invalid one
     * is reported (400) with its name in "field". Unknown parameters are ignored; if a
     * parameter is repeated, the first value counts.
     */
    public static SearchQuery parse(Map<String, List<String>> params, Tokenizer tokenizer) {
        String q = first(params, "q");
        if (q == null || q.isBlank()) {
            throw ApiException.missing("q");
        }
        if (q.length() > MAX_Q_LENGTH) {
            throw ApiException.invalid("q", "q must be at most " + MAX_Q_LENGTH + " characters");
        }
        Set<String> terms = tokenizer.uniqueTerms(q);
        if (terms.isEmpty()) {
            throw ApiException.invalid("q", "q has no searchable terms (only stopwords or 1-character words)");
        }

        String author = first(params, "author");
        if (author != null) {
            if (author.isBlank() || author.length() > MAX_AUTHOR_LENGTH) {
                throw ApiException.invalid("author", "author must have between 1 and " + MAX_AUTHOR_LENGTH + " characters");
            }
            author = author.strip().toLowerCase(Locale.ROOT);
        }

        String language = first(params, "language");
        if (language != null && !LanguageCodes.supportedCodes().contains(language)) {
            throw ApiException.invalid("language",
                    "language must be one of: " + String.join(", ", LanguageCodes.supportedCodes()));
        }

        Integer year = null;
        String yearText = first(params, "year");
        if (yearText != null) {
            year = integerIn(yearText, 1000, 9999);
            if (year == null) {
                throw ApiException.invalid("year", "year must be a 4-digit year");
            }
        }

        int offset = 0;
        String offsetText = first(params, "offset");
        if (offsetText != null) {
            Integer value = integerIn(offsetText, 0, Integer.MAX_VALUE);
            if (value == null) {
                throw ApiException.invalid("offset", "offset must be an integer >= 0");
            }
            offset = value;
        }

        int limit = DEFAULT_LIMIT;
        String limitText = first(params, "limit");
        if (limitText != null) {
            Integer value = integerIn(limitText, 1, MAX_LIMIT);
            if (value == null) {
                throw ApiException.invalid("limit", "limit must be an integer between 1 and " + MAX_LIMIT);
            }
            limit = value;
        }

        return new SearchQuery(q, terms, author, language, year, offset, limit);
    }

    /** Do the metadata of a book pass every filter of the query (AND)? */
    public boolean accepts(BookMetadata book) {
        if (author != null && (book.author() == null || !book.author().toLowerCase(Locale.ROOT).contains(author))) {
            return false;
        }
        if (language != null && !language.equals(book.language())) {
            return false;
        }
        return year == null || year.equals(book.year());
    }

    private static String first(Map<String, List<String>> params, String name) {
        List<String> values = params.get(name);
        return values == null || values.isEmpty() ? null : values.get(0);
    }

    /** The text as an int in [min, max], or null if it is not one ("abc", "1e3", "-1"...). */
    private static Integer integerIn(String text, int min, int max) {
        try {
            int value = Integer.parseInt(text.strip());
            return value >= min && value <= max ? value : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
