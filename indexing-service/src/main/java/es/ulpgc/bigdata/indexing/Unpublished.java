package es.ulpgc.bigdata.indexing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The books the next publication makes visible (DR-08), sorted, in two groups that never share
 * an id:
 *
 * @param indexed     in index_log and not in index_books: indexed since the last publication
 * @param interrupted in index_books with a version above CURRENT: a publication that inserted
 *                    them and died before switching CURRENT (D2). Invisible to Search; the next
 *                    publication completes them
 */
public record Unpublished(List<Integer> indexed, List<Integer> interrupted) {

    public Unpublished {
        indexed = List.copyOf(Objects.requireNonNull(indexed, "indexed"));
        interrupted = List.copyOf(Objects.requireNonNull(interrupted, "interrupted"));
    }

    /** Every book of both groups, sorted. */
    public List<Integer> all() {
        List<Integer> all = new ArrayList<>(indexed.size() + interrupted.size());
        all.addAll(indexed);
        all.addAll(interrupted);
        Collections.sort(all);
        return List.copyOf(all);
    }

    public boolean isEmpty() {
        return indexed.isEmpty() && interrupted.isEmpty();
    }
}
