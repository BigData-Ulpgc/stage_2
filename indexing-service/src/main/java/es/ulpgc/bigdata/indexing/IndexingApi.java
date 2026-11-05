package es.ulpgc.bigdata.indexing;

import com.mongodb.MongoException;
import es.ulpgc.bigdata.datamart.metadata.MetadataRepositoryException;
import es.ulpgc.bigdata.http.ApiException;
import es.ulpgc.bigdata.http.ServiceServer;
import es.ulpgc.bigdata.model.BookIds;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The HTTP API of api/indexing-openapi.yaml on top of ServiceServer (common): JSON with
 * explicit nulls, explicit Jetty thread pool, GET /status (which never touches MongoDB, D4)
 * and the common error format.
 *
 * It only translates (D8): it validates the request, calls IndexingService or Publisher and
 * turns the result into JSON, or the exception into the HTTP code of the contract. ServiceServer already
 * answers ApiException (400, 501), unknown routes (404) and unexpected errors (500, details
 * only in the log); here are only the exceptions of this service:
 *  - BookNotFoundException                        404 NOT_FOUND;
 *  - EmptyBookException                           422 EMPTY_BOOK: nothing was written;
 *  - MongoException, MetadataRepositoryException  503 STORAGE_UNAVAILABLE: temporary, and
 *                                                 update and publish are idempotent, so
 *                                                 retrying is safe.
 * Same error format and codes as Search (D3). The messages of the responses are in English:
 * they are part of the public contract.
 */
public final class IndexingApi {

    private static final Logger LOG = LoggerFactory.getLogger(IndexingApi.class);

    private final IndexingService service;
    private final Publisher publisher;

    public IndexingApi(IndexingService service, Publisher publisher) {
        this.service = Objects.requireNonNull(service, "service");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
    }

    /** The server with every route and error handler; the caller calls start(port). */
    public Javalin create(String serviceName, int maxThreads) {
        Javalin app = ServiceServer.create(serviceName, maxThreads);
        app.post("/index/update/{book_id}", this::update);
        app.get("/index/status", this::indexStatus);
        app.post("/index/publish", this::publish);
        // Route with its final contract, answering 501 until its task (D1).
        app.post("/index/rebuild", ctx -> {
            throw notImplemented("POST /index/rebuild", "DR-10");
        });

        // Javalin picks the most specific handler for the type of the exception, so these
        // win over the 500 one of ServiceServer.
        app.exception(BookNotFoundException.class, (e, ctx) -> ServiceServer.writeError(ctx, 404, "NOT_FOUND",
                "book " + e.bookId() + " is not ready in the datalake", "book_id"));
        app.exception(EmptyBookException.class, (e, ctx) -> ServiceServer.writeError(ctx, 422, "EMPTY_BOOK",
                "book " + e.bookId() + " has no indexable terms", "book_id"));
        app.exception(MongoException.class, (e, ctx) -> storageUnavailable(ctx, e));
        app.exception(MetadataRepositoryException.class, (e, ctx) -> storageUnavailable(ctx, e));
        return app;
    }

    /** 501 NOT_IMPLEMENTED: the route and its contract exist, the task that implements it does not yet. */
    private static ApiException notImplemented(String route, String task) {
        return new ApiException(501, "NOT_IMPLEMENTED", route + " is not implemented yet (" + task + ")", null);
    }

    // ------------------------------------------------------------------
    // POST /index/update/{book_id}
    // ------------------------------------------------------------------

    /** Synchronous (D2): a 200 means indexed, visible after the next publish. */
    private void update(Context ctx) {
        int bookId = bookId(ctx.pathParam("book_id"));          // 400 before touching anything
        UpdateResult result = service.update(bookId);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("book_id", result.bookId());
        body.put("index", "updated");
        body.put("terms", result.terms());
        ctx.json(body);
    }

    /** The canonical form of BookIds (common): 1..999,999,999, ASCII digits, no sign or leading zeros. */
    static int bookId(String text) {
        return BookIds.parse(text).orElseThrow(() -> ApiException.invalid("book_id", BOOK_ID_MESSAGE));
    }

    static final String BOOK_ID_MESSAGE = "book_id must be a positive integer (1 to 999999999, digits only)";

    // ------------------------------------------------------------------
    // GET /index/status
    // ------------------------------------------------------------------

    private static final double BYTES_PER_MB = 1024 * 1024;

    /** Reads MongoDB, unlike GET /status: 503 if it does not answer. */
    private void indexStatus(Context ctx) {
        IndexStatus status = service.status();

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("books_indexed", status.booksIndexed());
        body.put("last_update", status.lastUpdate() == null
                ? null
                : status.lastUpdate().truncatedTo(ChronoUnit.SECONDS).toString());   // "2026-11-03T10:14:52Z"
        body.put("index_size_MB", megabytes(status.indexSizeBytes()));
        body.put("published_version", status.publishedVersion());
        body.put("books_published", status.booksPublished());
        body.put("rebuild", null);                                                     // until DR-10
        ctx.json(body);
    }

    /** Bytes to MB (1,048,576 bytes) with one decimal: 44_774_195 -> 42.7. */
    static double megabytes(long bytes) {
        return Math.round(bytes / BYTES_PER_MB * 10) / 10.0;
    }

    // ------------------------------------------------------------------
    // POST /index/publish
    // ------------------------------------------------------------------

    /**
     * Synchronous: a 200 means CURRENT points to the returned version. With nothing new it
     * changes nothing and describes CURRENT (D5); version and published_at are null only if
     * nothing has ever been published. If the CAS keeps failing (another replica publishing
     * at the same time) Publisher throws IllegalStateException: 500 from ServiceServer.
     */
    private void publish(Context ctx) {
        PublishResult result = publisher.publish();

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("version", result.version());
        body.put("books_published", result.booksPublished());
        body.put("total_books", result.totalBooks());
        body.put("published_at", result.publishedAt());
        ctx.json(body);
    }

    // ------------------------------------------------------------------
    // Errors
    // ------------------------------------------------------------------

    private static void storageUnavailable(Context ctx, Exception e) {
        LOG.warn("MongoDB did not answer on {} {}: {}", ctx.method(), ctx.path(), e.getMessage());
        ServiceServer.writeError(ctx, 503, "STORAGE_UNAVAILABLE", "the index storage (MongoDB) does not answer", null);
    }
}
