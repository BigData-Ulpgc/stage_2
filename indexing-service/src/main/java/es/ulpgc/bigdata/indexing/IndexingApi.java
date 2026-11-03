package es.ulpgc.bigdata.indexing;

import com.mongodb.MongoException;
import es.ulpgc.bigdata.datamart.metadata.MetadataRepositoryException;
import es.ulpgc.bigdata.http.ServiceServer;
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
 * explicit nulls, explicit Jetty thread pool and GET /status, which never touches MongoDB (D4).
 *
 * It only translates (D8): it validates the request, calls IndexingService and turns the
 * result into JSON, or the exception into the HTTP code of the contract:
 *  - ApiException                                 its own status and code (400, 501);
 *  - BookNotFoundException                        404 NOT_FOUND;
 *  - MongoException, MetadataRepositoryException  503 STORAGE_UNAVAILABLE: temporary, and
 *                                                 update is idempotent, so retrying is safe;
 *  - anything else                                500 INTERNAL_ERROR, a bug of ours: the
 *                                                 details go to the log, never to the client.
 * Same error format and codes as Search (D3). The messages of the responses are in English:
 * they are part of the public contract.
 */
public final class IndexingApi {

    private static final Logger LOG = LoggerFactory.getLogger(IndexingApi.class);

    private final IndexingService service;

    public IndexingApi(IndexingService service) {
        this.service = Objects.requireNonNull(service, "service");
    }

    /** The server with every route and error handler; the caller calls start(port). */
    public Javalin create(String serviceName, int maxThreads) {
        Javalin app = ServiceServer.create(serviceName, maxThreads);
        app.post("/index/update/{book_id}", this::update);
        app.get("/index/status", this::indexStatus);
        // Routes with their final contract, answering 501 until their task (D1).
        app.post("/index/publish", ctx -> {
            throw ApiException.notImplemented("POST /index/publish", "DR-08");
        });
        app.post("/index/rebuild", ctx -> {
            throw ApiException.notImplemented("POST /index/rebuild", "DR-10");
        });

        // Javalin picks the most specific handler for the type of the exception.
        app.exception(ApiException.class, (e, ctx) -> error(ctx, e.status(), e.code(), e.getMessage(), e.field()));
        app.exception(BookNotFoundException.class, (e, ctx) -> error(ctx, 404, "NOT_FOUND",
                "book " + e.bookId() + " is not ready in the datalake", "book_id"));
        app.exception(MongoException.class, (e, ctx) -> storageUnavailable(ctx, e));
        app.exception(MetadataRepositoryException.class, (e, ctx) -> storageUnavailable(ctx, e));
        app.exception(Exception.class, (e, ctx) -> {
            LOG.error("Unexpected error on {} {}", ctx.method(), ctx.path(), e);
            error(ctx, 500, "INTERNAL_ERROR", "internal error", null);
        });
        return app;
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

    /** A positive integer, as the contract says (minimum: 1). */
    static int bookId(String text) {
        try {
            int id = Integer.parseInt(text);
            if (id >= 1) {
                return id;
            }
        } catch (NumberFormatException e) {
            // not a number, or does not fit in an int: falls through to the 400
        }
        throw ApiException.invalid("book_id", "book_id must be a positive integer");
    }

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
    // Errors
    // ------------------------------------------------------------------

    private static void storageUnavailable(Context ctx, Exception e) {
        LOG.warn("MongoDB did not answer on {} {}: {}", ctx.method(), ctx.path(), e.getMessage());
        error(ctx, 503, "STORAGE_UNAVAILABLE", "the index storage (MongoDB) does not answer", null);
    }

    private static void error(Context ctx, int status, String code, String message, String field) {
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("code", code);
        error.put("message", message);
        error.put("field", field);
        ctx.status(status).json(Map.of("error", error));
    }
}
