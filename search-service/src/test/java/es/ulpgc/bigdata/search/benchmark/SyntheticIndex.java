package es.ulpgc.bigdata.search.benchmark;

import es.ulpgc.bigdata.model.BookMetadata;
import es.ulpgc.bigdata.model.YearSource;
import es.ulpgc.bigdata.search.index.IndexSnapshot;
import es.ulpgc.bigdata.search.index.PostingSource;
import es.ulpgc.bigdata.search.query.SearchEngine;
import es.ulpgc.bigdata.search.query.SearchQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A reproducible index in memory, so the microbenchmarks measure the search code and not MongoDB
 * (the time of MongoDB is measured by the load tests). The same size always gives the same index.
 *
 * Each term is in a fixed fraction of the books, chosen independently. That makes the size of each
 * list easy to reason about, but real words appear together more often than this.
 */
final class SyntheticIndex {

    /** Fraction of the books that contain each term. */
    static final Map<String, Double> TERMS = Map.of(
            "common", 0.50,
            "frequent", 0.30,
            "medium", 0.10,
            "rare", 0.01);

    private static final long SEED = 20261108L;
    private static final int AUTHORS = 500;

    final IndexSnapshot snapshot;
    final SearchEngine engine;
    private final Map<String, int[]> postings;

    SyntheticIndex(int bookCount) {
        Random random = new Random(SEED);
        List<BookMetadata> books = new ArrayList<>(bookCount);
        for (int id = 1; id <= bookCount; id++) {
            books.add(book(id, random));
        }
        Map<String, int[]> lists = new HashMap<>();
        // Sorted names: the order of a Map must not change which random numbers each term gets.
        TERMS.keySet().stream().sorted().forEach(term ->
                lists.put(term, postingList(bookCount, TERMS.get(term), random)));
        this.postings = Map.copyOf(lists);
        this.snapshot = new IndexSnapshot(1, "synthetic", books);
        PostingSource source = (collection, terms) -> terms.stream()
                .filter(postings::containsKey)
                .collect(Collectors.toMap(term -> term, postings::get));
        this.engine = new SearchEngine(source);
    }

    /** "common_medium" means the terms common and medium: JMH splits the value of a @Param by commas. */
    static Set<String> terms(String scenario) {
        return Set.of(scenario.split("_"));
    }

    static SearchQuery query(Set<String> terms, String author, String language, Integer year, int offset, int limit) {
        return new SearchQuery(String.join(" ", terms), terms, author, language, year, offset, limit);
    }

    /** How many books match, computed without the search code. The benchmarks compare with it. */
    int expectedTotal(SearchQuery query) {
        int total = 0;
        for (BookMetadata book : snapshot.books()) {
            boolean inEveryList = query.terms().stream()
                    .allMatch(term -> Arrays.binarySearch(postings.get(term), book.bookId()) >= 0);
            if (inEveryList && query.accepts(book)) {
                total++;
            }
        }
        return total;
    }

    /** Fails if the search does not give the expected books, so a broken benchmark cannot produce numbers. */
    void verify(SearchQuery query) {
        int expected = expectedTotal(query);
        int actual = engine.search(snapshot, query).total();
        if (expected != actual) {
            throw new IllegalStateException("wrong result for " + query + ": expected " + expected + ", got " + actual);
        }
    }

    private static BookMetadata book(int id, Random random) {
        String author = random.nextInt(100) < 3
                ? null
                : "Author %04d, Given".formatted(random.nextInt(AUTHORS));
        String language = random.nextInt(100) < 60 ? "en" : random.nextBoolean() ? "es" : "fr";
        boolean hasYear = random.nextInt(100) >= 5;
        Integer year = hasYear ? 1800 + random.nextInt(200) : null;
        YearSource source = hasYear ? YearSource.RELEASE_DATE : null;
        return new BookMetadata(id, "Book " + id, author, language, year, source, null, null, null);
    }

    private static int[] postingList(int bookCount, double fraction, Random random) {
        int[] ids = new int[bookCount];
        int size = 0;
        for (int id = 1; id <= bookCount; id++) {
            if (random.nextDouble() < fraction) {
                ids[size++] = id;
            }
        }
        return Arrays.copyOf(ids, size);
    }
}
