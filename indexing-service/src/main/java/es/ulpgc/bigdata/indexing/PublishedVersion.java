package es.ulpgc.bigdata.indexing;

import java.util.List;
import java.util.Objects;

/**
 * One entry of the history of publications, index_publications (§6.5): for the report and for
 * the recovery. Written after CURRENT has switched to this version.
 *
 * @param version     the version published
 * @param books       the books that became visible in it, sorted
 * @param totalBooks  books visible in total in this version
 * @param publishedAt ISO-8601 UTC to the second; null only if CURRENT did not have it (an entry
 *                    repaired from a CURRENT written by another program)
 * @param collection  the collection of the inverted index of this version
 */
public record PublishedVersion(int version, List<Integer> books, long totalBooks,
                               String publishedAt, String collection) {

    public PublishedVersion {
        books = List.copyOf(Objects.requireNonNull(books, "books"));
    }
}
