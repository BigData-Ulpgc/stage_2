package es.ulpgc.bigdata.search.benchmark;

import es.ulpgc.bigdata.search.query.SearchEngine;
import es.ulpgc.bigdata.search.query.SearchQuery;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The benchmarks are not run by mvn verify; this keeps their data honest. */
class SyntheticIndexTest {

    private static final int BOOKS = 10_000;

    @Test
    void theSameSizeAlwaysGivesTheSameIndex() {
        SearchQuery query = SyntheticIndex.query(Set.of("common", "medium"), null, "en", null, 0, 20);
        assertEquals(new SyntheticIndex(BOOKS).expectedTotal(query), new SyntheticIndex(BOOKS).expectedTotal(query));
    }

    @Test
    void eachTermIsInRoughlyItsFractionOfTheBooks() {
        SyntheticIndex index = new SyntheticIndex(BOOKS);
        SyntheticIndex.TERMS.forEach((term, fraction) -> {
            int total = index.expectedTotal(SyntheticIndex.query(Set.of(term), null, null, null, 0, 20));
            assertEquals(fraction, (double) total / BOOKS, 0.02, term);
        });
    }

    @Test
    void everyFilterKeepsSomeBooksAndTheSearchAgreesWithTheCheck() {
        SyntheticIndex index = new SyntheticIndex(BOOKS);
        Set<String> common = Set.of("common");
        for (SearchQuery query : new SearchQuery[]{
                SyntheticIndex.query(common, "author 0042", null, null, 0, 20),
                SyntheticIndex.query(common, null, "en", null, 0, 20),
                SyntheticIndex.query(common, null, null, 1900, 0, 20),
                SyntheticIndex.query(Set.of("common", "rare"), null, null, null, 0, 20)}) {
            assertTrue(index.expectedTotal(query) > 0, query.toString());
            index.verify(query);
        }
    }

    @Test
    void aPageNearTheEndIsNotEmpty() {
        SyntheticIndex index = new SyntheticIndex(BOOKS);
        Set<String> common = Set.of("common");
        int total = index.expectedTotal(SyntheticIndex.query(common, null, null, null, 0, 20));
        SearchEngine.Result last = index.engine.search(index.snapshot,
                SyntheticIndex.query(common, null, null, null, total * 99 / 100, 20));
        assertEquals(20, last.page().size());
    }

    @Test
    void scenarioNamesSplitIntoTerms() {
        assertEquals(Set.of("common", "rare"), SyntheticIndex.terms("common_rare"));
    }
}
