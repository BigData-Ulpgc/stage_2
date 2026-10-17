package es.ulpgc.bigdata.datamart.index;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * In-memory-only inverted index: term -> sorted set of book_id.
 *
 *   boat  -> {10, 20}
 *   red   -> {10, 30}
 *
 * It writes nothing to disk: flush does nothing and the data is lost on close.
 * It is used for tests and as the "perfect index" reference in the benchmark.
 */
public class InMemoryInvertedIndex implements InvertedIndex {

    /**
     * Level 1 (HashMap): find a term's container, in constant time.
     * Level 2 (TreeSet): the container keeps the ids without repetitions and always sorted.
     */
    private final Map<String, SortedSet<Integer>> index = new HashMap<>();

    @Override
    public String name() {
        return "memory";
    }

    @Override
    public void addDocument(int bookId, Set<String> terms) {
        if (bookId < 0) {
            throw new IllegalArgumentException("book_id negativo: " + bookId);
        }
        Objects.requireNonNull(terms, "terms");
        for (String term : terms) {
            index.computeIfAbsent(term, t -> new TreeSet<>())   // 1. find or create the container
                 .add(bookId);                                   // 2. put the id in (if it is already there, it does nothing)
        }
    }

    @Override
    public List<Integer> postings(String term) {
        SortedSet<Integer> ids = index.get(term);
        return ids == null ? List.of() : List.copyOf(ids);    // copy: nobody can touch the index from outside
    }

    /** In memory there is nothing to persist. */
    @Override
    public void flush() {
    }

    @Override
    public void clear() {
        index.clear();
    }

    /** Does not use disk. */
    @Override
    public long diskUsageBytes() {
        return 0;
    }

    /** It has no files or connections to release. */
    @Override
    public void close() {
    }

    // --- Own methods, outside the contract (useful for tests and debugging) ---

    /** All the terms of the index, in alphabetical order. */
    public SortedSet<String> terms() {
        return new TreeSet<>(index.keySet());
    }

    /** Number of distinct terms. */
    public int termCount() {
        return index.size();
    }
}