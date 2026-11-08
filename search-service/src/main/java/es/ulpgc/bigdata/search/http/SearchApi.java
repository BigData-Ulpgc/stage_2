package es.ulpgc.bigdata.search.http;

import com.mongodb.MongoException;
import es.ulpgc.bigdata.datamart.index.Tokenizer;
import es.ulpgc.bigdata.datamart.metadata.MetadataRepositoryException;
import es.ulpgc.bigdata.http.ApiException;
import es.ulpgc.bigdata.http.ServiceServer;
import es.ulpgc.bigdata.model.BookIds;
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
 * Errors: ServiceServer already answers an ApiException (common) with its status and the
 * common error body, an unknown route with 404 and anything unexpected with 500; here is only
 * MongoDB not answering, which becomes 503 STORAGE_UNAVAILABLE (the client may retry).
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
        return create(serviceName, maxThreads, CorsPolicy.NONE);
    }

    /** The same, with the origins that may call the API from a browser. */
    public Javalin create(String serviceName, int maxThreads, CorsPolicy cors) {
        Javalin app = ServiceServer.create(serviceName, maxThreads, snapshots::status, cors::applyTo);
        app.get("/search", this::search);
        app.get("/search/filters", this::filters);
        app.get("/books/{book_id}", this::book);
        app.exception(MongoException.class, (e, ctx) -> storageUnavailable(ctx, e));
        app.exception(MetadataRepositoryException.class, (e, ctx) -> storageUnavailable(ctx, e));
        return app;
    }

    // ------------------------------------------------------------------
    // GET /search
    // ------------------------------------------------------------------

    private void search(Context ctx) {
        SearchQuery query = SearchQuery.parse(queryParams(ctx), tokenizer);      // 400 before touching anything
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

    /** The decoded parameters; 400 if the query string is not valid percent-encoding (q=%ZZ). */
    private static Map<String, List<String>> queryParams(Context ctx) {
        try {
            return ctx.queryParamMap();
        } catch (IllegalArgumentException e) {
            throw new ApiException(400, "INVALID_PARAMETER", "the query string is not valid percent-encoding", null);
        }
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
    // GET /books/{book_id}
    // ------------------------------------------------------------------

    /** Metadata of one book of the snapshot; 404 if it is not published (even if it is indexed). */
    private void book(Context ctx) {
        int bookId = bookId(ctx.pathParam("book_id"));            // 400 before looking at the index
        IndexSnapshot snapshot = snapshot();
        BookMetadata book = snapshot.book(bookId).orElseThrow(() -> new ApiException(404, "NOT_FOUND",
                "book " + bookId + " is not in the published index", "book_id"));

        Map<String, Object> body = summary(book);
        body.put("year_source", book.yearSource() == null ? null : book.yearSource().key());
        body.put("release_date", book.releaseDate());
        body.put("index_version", snapshot.version());
        ctx.json(body);
    }

    /** The canonical form of BookIds (common), the same rule as Ingestion and Indexing. */
    private static int bookId(String text) {
        return BookIds.parse(text).orElseThrow(() -> ApiException.invalid("book_id",
                "book_id must be a positive integer (1 to " + BookIds.MAX + ", digits only)"));
    }

    // ------------------------------------------------------------------
    // GET /search/filters
    // ------------------------------------------------------------------

    /** Languages and year range of the snapshot, precomputed when it was built. */
    private void filters(Context ctx) {
        IndexSnapshot snapshot = snapshot();
        IndexSnapshot.FilterValues values = snapshot.filterValues();

        List<Map<String, Object>> languages = new ArrayList<>();
        values.languages().forEach((code, books) -> {
            Map<String, Object> language = new LinkedHashMap<>();
            language.put("code", code);
            language.put("books", books);
            languages.add(language);
        });
        Map<String, Object> years = new LinkedHashMap<>();
        years.put("min", values.minYear());
        years.put("max", values.maxYear());

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("books", snapshot.bookCount());
        body.put("languages", languages);
        body.put("years", years);
        body.put("index_version", snapshot.version());
        ctx.json(body);
    }

    // ------------------------------------------------------------------
    // Shared
    // ------------------------------------------------------------------

    /** The snapshot of this request; 503 INDEX_NOT_READY if nothing is published yet. */
    IndexSnapshot snapshot() {
        return snapshots.current().orElseThrow(SearchApi::indexNotReady);
    }

    /** 503 INDEX_NOT_READY: no index version has been published (or loaded) yet. */
    private static ApiException indexNotReady() {
        return new ApiException(503, "INDEX_NOT_READY", "no index version has been published yet", null);
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
        ServiceServer.writeError(ctx, 503, "STORAGE_UNAVAILABLE", "the index storage (MongoDB) does not answer", null);
    }
}
