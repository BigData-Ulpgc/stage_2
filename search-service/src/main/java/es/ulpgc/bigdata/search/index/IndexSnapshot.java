package es.ulpgc.bigdata.search.index;

import es.ulpgc.bigdata.model.BookMetadata;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * One published version of the index, as Search sees it: the version number, the MongoDB
 * collection that holds its posting lists, and the books published up to that version with
 * their metadata (contract §6.5 of the plan).
 *
 * It is never modified after it is built. That is what makes concurrent requests safe
 * without locks: a request takes ONE snapshot when it starts and uses only that one, so a
 * response never mixes two versions. A new version is loaded into a NEW snapshot and swapped
 * in (SnapshotHolder; reload on publication in DP-06).
 *
 * The posting lists are NOT here: they stay in MongoDB and each request reads only those of
 * its terms. The index may contain books that are indexed but not yet published; whoever
 * searches must keep only the ids for which isPublished is true.
 */
public final class IndexSnapshot {

    private final int version;
    private final String collection;
    /** book_id -> metadata of every published book, ascending by id. */
    private final SortedMap<Integer, BookMetadata> books;

    public IndexSnapshot(int version, String collection, Collection<BookMetadata> publishedBooks) {
        this.version = version;
        this.collection = Objects.requireNonNull(collection, "collection");
        TreeMap<Integer, BookMetadata> byId = new TreeMap<>();
        for (BookMetadata book : publishedBooks) {
            byId.put(book.bookId(), book);
        }
        this.books = Collections.unmodifiableSortedMap(byId);
    }

    /** Version of index_meta CURRENT this snapshot was built from, e.g. 42. */
    public int version() {
        return version;
    }

    /** MongoDB collection with the posting lists of this version (CURRENT.collection). */
    public String collection() {
        return collection;
    }

    /** Is the book visible in this version? Books indexed after it are not. */
    public boolean isPublished(int bookId) {
        return books.containsKey(bookId);
    }

    /** Metadata of a published book; empty if it is not published in this version. */
    public Optional<BookMetadata> book(int bookId) {
        return Optional.ofNullable(books.get(bookId));
    }

    /** Every published book, ascending by book_id. */
    public Collection<BookMetadata> books() {
        return books.values();
    }

    public int bookCount() {
        return books.size();
    }
}
