package es.ulpgc.bigdata.indexing;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Where the publications of the index live (contract §6.5):
 * <pre>
 *   index_meta          {_id: "CURRENT", version, collection, books, published_at}   the visible version
 *   index_books         {_id: book_id, version}       the FIRST version in which each book became visible
 *   index_publications  {_id: version, books: [...], books_published, total_books, published_at, collection}
 * </pre>
 * Each method is one small step of a publication; the order and the decisions are in Publisher.
 * Every method may throw the exception of the storage when it does not answer (MongoException:
 * the API answers 503).
 */
public interface PublicationRepository {

    /** The version CURRENT points to; empty if nothing has been published yet. */
    Optional<Publication> current();

    /**
     * The books the publication on top of {@code currentVersion} makes visible (D1, D2): those
     * of index_log that are not in index_books, and those of index_books with a version above
     * {@code currentVersion}.
     *
     * @param currentVersion the version of CURRENT; null if nothing has been published yet
     */
    Unpublished unpublished(Integer currentVersion);

    /**
     * Records the books in index_books with this version. A book that is already there keeps
     * its version ($setOnInsert): it is the first one in which it became visible, and changing
     * it would hide the book from a Search that has read CURRENT but not index_books yet.
     */
    void addBooks(Collection<Integer> bookIds, int version);

    /** Books visible in {@code version}: those of index_books with a version <= it. */
    long countVisible(int version);

    /**
     * Switches CURRENT to {@code next} only if it still points to {@code expectedVersion}: the
     * fencing of coordination contract §5. The first version (expectedVersion null) is an
     * insert that fails if CURRENT already exists.
     *
     * @return false if CURRENT is not at expectedVersion anymore: another replica published
     *         first, nothing was changed
     */
    boolean switchCurrent(Integer expectedVersion, Publication next);

    /** Writes the history entry of a version, replacing it if it is already there (idempotent). */
    void recordHistory(PublishedVersion entry);

    /** Whether the history has the entry of this version. */
    boolean hasHistory(int version);

    /** The books whose first version is exactly this one, sorted (to repair a missing history entry). */
    List<Integer> booksOfVersion(int version);

    /**
     * Of these books, those visible in {@code version} (in index_books with a version <= it), in
     * no particular order: the ones whose state may become PUBLISHED (T10, DR-08b).
     */
    List<Integer> visibleAmong(Collection<Integer> bookIds, int version);
}
