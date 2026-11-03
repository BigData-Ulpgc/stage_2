package es.ulpgc.bigdata.search.fixture;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import es.ulpgc.bigdata.config.MongoSettings;
import es.ulpgc.bigdata.datamart.index.MongoInvertedIndex;
import es.ulpgc.bigdata.datamart.index.Tokenizer;
import es.ulpgc.bigdata.datamart.metadata.MetadataParser;
import es.ulpgc.bigdata.datamart.metadata.MongoMetadataRepository;
import es.ulpgc.bigdata.model.BookLocation;
import es.ulpgc.bigdata.model.BookMetadata;
import es.ulpgc.bigdata.search.index.IndexSnapshotLoader;
import org.bson.Document;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Writes a published index into MongoDB the way the Indexing Service will (plan §3.1 and
 * §6.5), so that Search can be developed and tested before Indexing exists (DP-03):
 *
 * <pre>
 *   books           metadata of every book       MetadataParser + MongoMetadataRepository (common)
 *   &lt;collection&gt;    posting lists                Tokenizer.standard() + MongoInvertedIndex (common)
 *   index_books     {_id, version} per published book
 *   index_meta      {_id: "CURRENT", version, collection, books, published_at}   written LAST
 * </pre>
 *
 * It uses exactly the common code Indexing uses, so Search sees the same terms and metadata
 * it will see in production. It is NOT the Indexing Service: no queues, no leases, no
 * concurrency. Deterministic: the same books give the same documents (published_at is fixed).
 */
public final class MongoFixtureLoader {

    /** Fixed so that loading the fixture twice gives identical documents. */
    static final String PUBLISHED_AT = "2026-10-29T00:00:00Z";

    /** A book as it would come out of the datalake: header and body text. */
    public record Book(int id, String header, String body) {
        public Book {
            Objects.requireNonNull(header, "header");
            Objects.requireNonNull(body, "body");
        }
    }

    private MongoFixtureLoader() {
    }

    /**
     * Empties the collections of the contract and loads the books.
     *
     * @param versionOf      book_id -> version in which it was published; books missing from
     *                       the map are indexed but NOT published (invisible to Search)
     * @param currentVersion version written to CURRENT; books published with a higher version
     *                       are also invisible (a publication stopped before switching CURRENT)
     */
    public static void load(String mongoUri, String database, String indexCollection,
                            List<Book> books, Map<Integer, Integer> versionOf, int currentVersion) {
        MongoSettings settings = new MongoSettings(mongoUri, database, indexCollection);
        try (MongoClient client = settings.openClient();
             MongoInvertedIndex index = settings.openIndex()) {
            MongoDatabase db = client.getDatabase(database);
            db.getCollection(IndexSnapshotLoader.INDEX_META).drop();
            db.getCollection(IndexSnapshotLoader.INDEX_BOOKS).drop();
            index.clear();
            MongoMetadataRepository metadata = new MongoMetadataRepository(db);
            metadata.clear();

            // 1. Metadata and posting lists of every book (published or not).
            Tokenizer tokenizer = Tokenizer.standard();
            MetadataParser parser = new MetadataParser();
            List<BookMetadata> parsed = new ArrayList<>();
            for (Book book : books) {
                parsed.add(parser.parse(locationOf(book.id()), book.header()));
                index.addDocument(book.id(), tokenizer.uniqueTerms(book.body()));
            }
            metadata.saveAll(parsed);
            index.flush();

            // 2. Publication: index_books first, CURRENT last (contract §6.5).
            List<Document> published = new ArrayList<>();
            int visible = 0;
            for (Book book : books) {
                Integer version = versionOf.get(book.id());
                if (version != null) {
                    published.add(new Document("_id", book.id()).append("version", version));
                    if (version <= currentVersion) {
                        visible++;
                    }
                }
            }
            if (!published.isEmpty()) {
                db.getCollection(IndexSnapshotLoader.INDEX_BOOKS).insertMany(published);
            }
            db.getCollection(IndexSnapshotLoader.INDEX_META).insertOne(new Document("_id", IndexSnapshotLoader.CURRENT)
                    .append("version", currentVersion)
                    .append("collection", indexCollection)
                    .append("books", visible)
                    .append("published_at", PUBLISHED_AT));
        }
    }

    /** Where the range datalake keeps the book (header_path / body_path of the contract). */
    static BookLocation locationOf(int id) {
        int start = (id / 1000) * 1000;
        String dir = "/data/datalake/range/" + String.format(Locale.ROOT, "%05d-%05d", start, start + 999) + "/";
        return new BookLocation(id, Path.of(dir + id + ".header.txt"), Path.of(dir + id + ".body.txt"));
    }
}
