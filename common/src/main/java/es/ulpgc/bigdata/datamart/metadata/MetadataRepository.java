package es.ulpgc.bigdata.datamart.metadata;

import es.ulpgc.bigdata.model.BookMetadata;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Store of book metadata. The rest of the system only knows this interface:
 * it does not know whether there is MongoDB, another database or memory behind it.
 *
 * Rules every implementation must follow (contract §6.3, DR-04):
 *  - save/saveAll are idempotent: saving a book_id again replaces it, never duplicates it.
 *  - saveAll is NOT all or nothing: if it fails halfway, part of the batch may be saved.
 *    Repeating the whole batch is always safe (idempotence), and it is what Indexing does:
 *    a book only becomes INDEXED after its batch succeeded, otherwise its lease expires and
 *    the book is processed again. (MongoDB has no multi-document transactions without a
 *    replica set, and the deployment runs a single instance.)
 *  - findByIds returns the books that exist, sorted by book_id; unknown ids are skipped.
 *
 * There are no queries by author, language or year: Search keeps the metadata of the
 * published books in memory (its snapshot) and applies the filters there. It only needs
 * to read books by id: all of them when it starts, the new ones on each publication.
 */
public interface MetadataRepository extends AutoCloseable {

    void save(BookMetadata metadata);

    void saveAll(List<BookMetadata> batch);

    Optional<BookMetadata> findById(int bookId);

    List<BookMetadata> findByIds(Collection<Integer> bookIds);

    long count();

    void clear();

    /** Without "throws Exception": closing the repository does not force catching anything. */
    @Override
    void close();
}
