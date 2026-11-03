package es.ulpgc.bigdata.indexing;

import es.ulpgc.bigdata.datamart.metadata.MetadataRepository;
import es.ulpgc.bigdata.datamart.metadata.MetadataRepositoryException;
import es.ulpgc.bigdata.model.BookMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fake MetadataRepository for the tests: a thread-safe map, so IndexingService can be tested
 * without MongoDB. failing(true) makes save behave like MongoDB being down.
 */
class InMemoryMetadataRepository implements MetadataRepository {

    private final Map<Integer, BookMetadata> books = new ConcurrentHashMap<>();
    private volatile boolean failing = false;

    void failing(boolean failing) {
        this.failing = failing;
    }

    @Override
    public void save(BookMetadata metadata) {
        if (failing) {
            throw new MetadataRepositoryException("MongoDB no responde (simulado)", null);
        }
        books.put(metadata.bookId(), metadata);
    }

    @Override
    public void saveAll(List<BookMetadata> batch) {
        batch.forEach(this::save);
    }

    @Override
    public Optional<BookMetadata> findById(int bookId) {
        return Optional.ofNullable(books.get(bookId));
    }

    @Override
    public List<BookMetadata> findByIds(Collection<Integer> bookIds) {
        List<BookMetadata> found = new ArrayList<>();
        bookIds.stream().sorted().distinct().forEach(id -> findById(id).ifPresent(found::add));
        return found;
    }

    @Override
    public long count() {
        return books.size();
    }

    @Override
    public void clear() {
        books.clear();
    }

    @Override
    public void close() {
    }
}
