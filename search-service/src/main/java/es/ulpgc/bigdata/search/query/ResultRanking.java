package es.ulpgc.bigdata.search.query;

import es.ulpgc.bigdata.model.BookMetadata;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The order of the results of a search, applied to ALL the matches before cutting the page, so
 * pagination stays consistent. It is the one place to change to rank in another way (by year, by
 * relevance...): the search itself and the API do not depend on the order chosen.
 *
 * Must be deterministic: the same matches always in the same order, or a client paging through
 * the results could see a book twice or miss it.
 */
@FunctionalInterface
public interface ResultRanking {

    /** Ascending book_id: the order of the contract (api/openapi.yaml). */
    ResultRanking BY_BOOK_ID = (matches, query) -> {
        List<BookMetadata> ordered = new ArrayList<>(matches);
        ordered.sort(Comparator.comparingInt(BookMetadata::bookId));
        return ordered;
    };

    /**
     * @param matches every book that passes the query, already published and filtered
     * @param query   the query, for a ranking that depends on its terms
     * @return the same books in the order to show them
     */
    List<BookMetadata> rank(List<BookMetadata> matches, SearchQuery query);
}
