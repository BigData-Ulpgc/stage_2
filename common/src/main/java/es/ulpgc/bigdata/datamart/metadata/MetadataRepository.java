package es.ulpgc.bigdata.datamart.metadata;

import es.ulpgc.bigdata.model.BookMetadata;

import java.util.List;
import java.util.Optional;

/**
 * Store of book metadata. The rest of the system only knows this interface:
 * it does not know whether there is SQLite, another database or memory behind it.
 *
 * Rules every implementation must follow (and the benchmark takes for granted):
 *  - save/saveAll are idempotent: saving a book_id again updates it.
 *  - saveAll is all or nothing: if one row fails, none is saved.
 *  - findByAuthor/findByTitle search by exact equality and return
 *    the results sorted by book_id.
 */
public interface MetadataRepository extends AutoCloseable {

    void save(BookMetadata metadata);

    void saveAll(List<BookMetadata> batch);

    Optional<BookMetadata> findById(int bookId);

    List<BookMetadata> findByAuthor(String author);

    List<BookMetadata> findByTitle(String title);

    long count();

    void clear();

    /** Without "throws Exception": closing the repository does not force catching anything. */
    @Override
    void close();
}