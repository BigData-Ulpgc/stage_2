package es.ulpgc.bigdata.ingestion;

import es.ulpgc.bigdata.http.ApiException;
import es.ulpgc.bigdata.http.ServiceServer;
import es.ulpgc.bigdata.ingestion.source.BookNotFoundException;
import es.ulpgc.bigdata.ingestion.source.SourceUnavailableException;
import es.ulpgc.bigdata.model.BookIds;
import es.ulpgc.bigdata.model.BookLocation;
import io.javalin.Javalin;
import io.javalin.http.Context;

import java.net.http.HttpTimeoutException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * HTTP API of the Ingestion Service (assignment §3.2.1; contract api/ingestion-openapi.yaml).
 * Internal: consumed by the Control Module and by operators, never by the front-end.
 *
 * <pre>
 *   POST /ingest/{book_id}          download, split and save: 200 downloaded | already_available
 *   GET  /ingest/status/{book_id}   200 available | not_available
 *   GET  /ingest/list               200 every complete book of the datalake
 * </pre>
 *
 * Errors use the common format of ServiceServer:
 * <pre>
 *   400 INVALID_PARAMETER   book_id not in canonical form (BookIds)
 *   404 NOT_FOUND           the source does not have the book
 *   422 INVALID_BOOK        the text has no START/END markers
 *   502 SOURCE_UNAVAILABLE  the source failed (HTTP 5xx, 429, 403, network, I/O)
 *   504 SOURCE_TIMEOUT      the source did not answer in INGESTION_REQUEST_TIMEOUT_SECONDS
 * </pre>
 * 404 and 422 are permanent (retrying gives the same answer); 502 and 504 are safe to retry.
 */
public final class IngestionApi {

    static final String BOOK_ID_MESSAGE = "book_id must be a positive integer (1 to 999999999, digits only)";

    private final IngestionService service;
    private final Path dataDir;

    /**
     * @param dataDir DATA_DIR: the {@code path} of the responses is relative to it, so they never
     *                show where the volume is mounted on the host
     */
    public IngestionApi(IngestionService service, Path dataDir) {
        this.service = Objects.requireNonNull(service, "service");
        this.dataDir = Objects.requireNonNull(dataDir, "dataDir").toAbsolutePath().normalize();
    }

    /** The server with every route and error handler; the caller calls start(port). */
    public Javalin create(String serviceName, int maxThreads) {
        Javalin app = ServiceServer.create(serviceName, maxThreads,
                () -> Map.of("source", service.sourceDescription()));
        app.post("/ingest/{book_id}", this::ingest);
        app.get("/ingest/status/{book_id}", this::status);
        app.get("/ingest/list", this::list);

        // Javalin picks the most specific handler for the type of the exception, so these
        // win over the 500 one of ServiceServer.
        app.exception(BookNotFoundException.class, (e, ctx) -> ServiceServer.writeError(ctx, 404, "NOT_FOUND",
                "book " + e.bookId() + " is not available at the source", "book_id"));
        app.exception(InvalidBookException.class, (e, ctx) -> ServiceServer.writeError(ctx, 422, "INVALID_BOOK",
                e.getMessage(), "book_id"));
        app.exception(SourceUnavailableException.class, (e, ctx) -> {
            if (e.getCause() instanceof HttpTimeoutException) {
                ServiceServer.writeError(ctx, 504, "SOURCE_TIMEOUT", e.getMessage(), null);
            } else {
                ServiceServer.writeError(ctx, 502, "SOURCE_UNAVAILABLE", e.getMessage(), null);
            }
        });
        return app;
    }

    /** Synchronous: a 200 means the book is complete in the datalake. */
    private void ingest(Context ctx) throws Exception {
        int bookId = bookId(ctx.pathParam("book_id"));
        IngestionService.IngestResult result = service.ingest(bookId);
        ctx.json(book(bookId,
                result.outcome() == IngestionService.Outcome.DOWNLOADED ? "downloaded" : "already_available",
                Optional.of(result.location())));
    }

    private void status(Context ctx) {
        int bookId = bookId(ctx.pathParam("book_id"));
        Optional<BookLocation> location = service.locate(bookId);
        ctx.json(book(bookId, location.isPresent() ? "available" : "not_available", location));
    }

    private void list(Context ctx) {
        List<Integer> books = service.list();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("count", books.size());
        body.put("books", books);
        ctx.json(body);
    }

    private Map<String, Object> book(int bookId, String status, Optional<BookLocation> location) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("book_id", bookId);
        body.put("status", status);
        body.put("path", location.map(l -> path(bookId, l)).orElse(null));
        return body;
    }

    /**
     * The folder of the book relative to DATA_DIR, with "/" on every system, plus the id:
     * datalake/range/01000-01999/1342 (the assignment's example is datalake/20251008/14/1342).
     */
    String path(int bookId, BookLocation location) {
        Path folder = location.headerPath().toAbsolutePath().normalize().getParent();
        Path relative = folder.startsWith(dataDir) ? dataDir.relativize(folder) : folder;
        StringBuilder text = new StringBuilder();
        for (Path part : relative) {
            text.append(part).append('/');
        }
        return text.append(bookId).toString();
    }

    /** The canonical form of BookIds (common): 1..999,999,999, ASCII digits, no sign or leading zeros. */
    static int bookId(String text) {
        return BookIds.parse(text).orElseThrow(() -> ApiException.invalid("book_id", BOOK_ID_MESSAGE));
    }
}
