package es.ulpgc.bigdata.search.http;

import com.mongodb.MongoException;
import es.ulpgc.bigdata.datamart.index.Tokenizer;
import es.ulpgc.bigdata.datamart.metadata.MetadataRepositoryException;
import es.ulpgc.bigdata.http.ServiceServer;
import es.ulpgc.bigdata.model.BookMetadata;
import es.ulpgc.bigdata.search.index.IndexSnapshot;
import es.ulpgc.bigdata.search.index.SnapshotHolder;
import es.ulpgc.bigdata.search.query.SearchEngine;
import es.ulpgc.bigdata.search.query.SearchQuery;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The HTTP API of api/openapi.yaml on top of ServiceServer (common): JSON with explicit
 * nulls, explicit Jetty thread pool and GET /status.
 *
 * Every handler takes the snapshot ONCE (snapshots.current()) and uses only that one, so
 * a response is computed from a single index version, which it returns as index_version.
 *
 * Errors: an ApiException becomes its status and the common error body; MongoDB not
 * answering becomes 503 STORAGE_UNAVAILABLE (the client may retry); anything else 500.
 */
public final class SearchApi {

    private static final Logger LOG = LoggerFactory.getLogger(SearchApi.class);

    private final SnapshotHolder snapshots;
    private final SearchEngine engine;
    private final Tokenizer tokenizer;

    public SearchApi(SnapshotHolder snapshots, SearchEngine engine, Tokenizer tokenizer) {
        this.snapshots = Objects.requireNonNull(snapshots, "snapshots");
        this.engine = Objects.requireNonNull(engine, "engine");
        this.tokenizer = Objects.requireNonNull(tokenizer, "tokenizer");
    }

    /** The server with every route and error handler; the caller calls start(port). */
    public Javalin create(String serviceName, int maxThreads) {
        Javalin app = ServiceServer.create(serviceName, maxThreads, snapshots::status);
        app.get("/search", this::search);
        app.exception(ApiException.class, (e, ctx) -> error(ctx, e.status(), e.code(), e.getMessage(), e.field()));
        app.exception(MongoException.class, (e, ctx) -> storageUnavailable(ctx, e));
        app.exception(MetadataRepositoryException.class, (e, ctx) -> storageUnavailable(ctx, e));
        app.exception(Exception.class, (e, ctx) -> {
            LOG.error("Unexpected error on {} {}", ctx.method(), ctx.path(), e);
            error(ctx, 500, "INTERNAL_ERROR", "internal error", null);
        });
        return app;
    }

    // ------------------------------------------------------------------
    // GET /search
    // ------------------------------------------------------------------

    private void search(Context ctx) {
        SearchQuery query = SearchQuery.parse(ctx.queryParamMap(), tokenizer);   // 400 before touching anything
        IndexSnapshot snapshot = snapshot();
        SearchEngine.Result result = engine.search(snapshot, query);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("query", query.q());
        body.put("filters", filtersOf(ctx));
        body.put("total", result.total());
        body.put("offset", query.offset());
        body.put("limit", query.limit());
        body.put("index_version", result.indexVersion());
        List<Map<String, Object>> results = new ArrayList<>(result.page().size());
        for (BookMetadata book : result.page()) {
            results.add(summary(book));
        }
        body.put("results", results);
        ctx.json(body);
    }

    /** Only the filters present in the request, as received (year as a number). */
    private static Map<String, Object> filtersOf(Context ctx) {
        Map<String, Object> filters = new LinkedHashMap<>();
        String author = ctx.queryParam("author");
        if (author != null) {
            filters.put("author", author);
        }
        String language = ctx.queryParam("language");
        if (language != null) {
            filters.put("language", language);
        }
        String year = ctx.queryParam("year");
        if (year != null) {
            filters.put("year", Integer.parseInt(year.strip()));   // already validated
        }
        return filters;
    }

    // ------------------------------------------------------------------
    // Shared
    // ------------------------------------------------------------------

    /** The snapshot of this request; 503 INDEX_NOT_READY if nothing is published yet. */
    IndexSnapshot snapshot() {
        return snapshots.current().orElseThrow(ApiException::indexNotReady);
    }

    /** BookSummary of the contract. */
    static Map<String, Object> summary(BookMetadata book) {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("book_id", book.bookId());
        summary.put("title", book.title());
        summary.put("author", book.author());
        summary.put("language", book.language());
        summary.put("year", book.year());
        return summary;
    }

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
