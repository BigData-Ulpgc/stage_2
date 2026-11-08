package es.ulpgc.bigdata.control;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.HazelcastClientOfflineException;
import com.hazelcast.core.HazelcastException;
import es.ulpgc.bigdata.coordination.BackgroundConnection;
import es.ulpgc.bigdata.coordination.BookState;
import es.ulpgc.bigdata.coordination.BookTask;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.Stage;
import es.ulpgc.bigdata.http.ApiException;
import es.ulpgc.bigdata.http.ServiceServer;
import es.ulpgc.bigdata.model.BookIds;
import io.javalin.Javalin;
import io.javalin.http.Context;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * HTTP API of the Control Module (plan §3.5; contract api/control-openapi.yaml). Internal:
 * consumed by operators and by the benchmarks, never by the front-end.
 *
 * <pre>
 *   POST /control/jobs              202 seed books: {"book_ids": [1342, 84]}
 *   GET  /control/status            200 books per state, queues, publication, lease check
 *   GET  /control/tasks/{book_id}   200 the state of one book
 *   POST /control/retry-failed      200 the FAILED books back to their queue (T11)
 * </pre>
 *
 * Every /control route needs the Hazelcast cluster; while it is not connected they answer
 * 503 COORDINATION_UNAVAILABLE at once, instead of waiting for the client's 120 s invocation
 * timeout. GET /status does not need it.
 */
public final class ControlApi {

    /** Largest POST /control/jobs: the 100,000-book synthetic dataset in one call. */
    static final int MAX_BOOK_IDS = 100_000;
    static final String BOOK_ID_MESSAGE = "book_id must be a positive integer (1 to 999999999, digits only)";

    private final BackgroundConnection connection;
    private final Supplier<Map<String, Object>> tasksStatus;

    /**
     * @param tasksStatus what ControlTasks reports (publication, lease check), or an empty map
     *                    before the tasks start
     */
    public ControlApi(BackgroundConnection connection, Supplier<Map<String, Object>> tasksStatus) {
        this.connection = Objects.requireNonNull(connection, "connection");
        this.tasksStatus = Objects.requireNonNull(tasksStatus, "tasksStatus");
    }

    /** The server with every route and error handler; the caller calls start(port). */
    public Javalin create(String serviceName, int maxThreads) {
        Javalin app = ServiceServer.create(serviceName, maxThreads, this::serviceStatus);
        app.post("/control/jobs", this::jobs);
        app.get("/control/status", this::status);
        app.get("/control/tasks/{book_id}", this::task);
        app.post("/control/retry-failed", this::retryFailed);

        // The cluster was lost between the state check and the call
        app.exception(HazelcastException.class, (e, ctx) -> unavailable(ctx, e));
        app.exception(HazelcastClientOfflineException.class, (e, ctx) -> unavailable(ctx, e));
        app.exception(HazelcastClientNotActiveException.class, (e, ctx) -> unavailable(ctx, e));
        return app;
    }

    private Map<String, Object> serviceStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("coordination", connection.state().label());
        connection.failure().ifPresent(failure -> status.put("coordination_error", failure));
        return status;
    }

    /** T1 for every id that the cluster does not know yet; the others are left as they are. */
    private void jobs(Context ctx) {
        Set<Integer> ids = bookIds(ctx.body());
        int added = coordination().seed(ids);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("submitted", ids.size());
        body.put("added", added);
        body.put("already_known", ids.size() - added);
        ctx.status(202).json(body);
    }

    private void status(Context ctx) {
        Coordination coordination = coordination();
        Map<String, Object> books = new LinkedHashMap<>();
        long total = 0;
        for (Map.Entry<BookState, Long> entry : coordination.countByState().entrySet()) {
            books.put(entry.getKey().name(), entry.getValue());
            total += entry.getValue();
        }
        books.put("total", total);
        Map<String, Object> queues = new LinkedHashMap<>();
        for (Stage stage : Stage.values()) {
            queues.put(stage.queue(), coordination.queueSize(stage));
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("coordination", connection.state().label());
        body.put("books", books);
        body.put("queues", queues);
        body.putAll(tasksStatus.get());
        ctx.json(body);
    }

    private void task(Context ctx) {
        int bookId = BookIds.parse(ctx.pathParam("book_id"))
                .orElseThrow(() -> ApiException.invalid("book_id", BOOK_ID_MESSAGE));
        BookTask task = coordination().task(bookId).orElseThrow(() -> new ApiException(404, "NOT_FOUND",
                "book " + bookId + " was never submitted", "book_id"));
        ctx.json(task(task));
    }

    private void retryFailed(Context ctx) {
        ctx.json(Map.of("requeued", coordination().retryFailed()));
    }

    private Coordination coordination() {
        BackgroundConnection.State state = connection.state();
        if (state != BackgroundConnection.State.CONNECTED) {
            throw new ApiException(503, "COORDINATION_UNAVAILABLE",
                    "the Hazelcast cluster is not available (" + state.label() + "), try again later", null);
        }
        return connection.coordination().orElseThrow(() -> new ApiException(503, "COORDINATION_UNAVAILABLE",
                "the Hazelcast cluster is not available (closed), try again later", null));
    }

    private static void unavailable(Context ctx, RuntimeException e) {
        ServiceServer.writeError(ctx, 503, "COORDINATION_UNAVAILABLE",
                "the Hazelcast cluster is not available (" + e.getClass().getSimpleName() + "), try again later", null);
    }

    /**
     * The body of POST /control/jobs: {"book_ids": [...]}, 1 to MAX_BOOK_IDS JSON integers in
     * the range of BookIds. Duplicates are dropped. Any bad id rejects the whole request, so a
     * request either seeds everything or nothing.
     */
    static Set<Integer> bookIds(String text) {
        JsonElement body = JsonParser.parseString(text);   // not JSON: 400 of ServiceServer
        if (!body.isJsonObject()) {
            throw ApiException.invalid("body", "the body must be a JSON object {\"book_ids\": [...]}");
        }
        JsonObject object = body.getAsJsonObject();
        if (!object.has("book_ids") || object.get("book_ids").isJsonNull()) {
            throw ApiException.missing("book_ids");
        }
        if (!object.get("book_ids").isJsonArray()) {
            throw ApiException.invalid("book_ids", "book_ids must be an array of book ids");
        }
        JsonArray array = object.getAsJsonArray("book_ids");
        if (array.isEmpty() || array.size() > MAX_BOOK_IDS) {
            throw ApiException.invalid("book_ids", "book_ids must have 1 to " + MAX_BOOK_IDS + " ids, got " + array.size());
        }
        Set<Integer> ids = new LinkedHashSet<>();
        for (int i = 0; i < array.size(); i++) {
            int id = bookId(array.get(i));
            if (id == 0) {
                throw ApiException.invalid("book_ids", "book_ids[" + i + "] is not a book id: " + array.get(i)
                        + " (" + BOOK_ID_MESSAGE + ")");
            }
            ids.add(id);
        }
        return ids;
    }

    /** The id, or 0 if the element is not a JSON integer in the range of BookIds. */
    private static int bookId(JsonElement element) {
        if (!(element instanceof JsonPrimitive primitive) || !primitive.isNumber()) {
            return 0;                                       // "1342", true, null, [..]
        }
        try {
            int id = new BigDecimal(primitive.getAsString()).intValueExact();   // 1342.5, 1e10: no
            return BookIds.isValid(id) ? id : 0;
        } catch (ArithmeticException | NumberFormatException e) {
            return 0;
        }
    }

    /** BookTask in the snake_case of the API, times in ISO-8601 UTC (null when not set). */
    static Map<String, Object> task(BookTask task) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("book_id", task.bookId());
        body.put("state", task.state());
        body.put("owner", task.owner());
        body.put("lease_until", instant(task.leaseUntil()));
        body.put("attempts", task.attempts());
        body.put("failed_stage", task.failedStage());
        body.put("last_error", task.lastError());
        body.put("enqueued_at", instant(task.enqueuedAt()));
        body.put("started_at", instant(task.startedAt()));
        body.put("finished_at", instant(task.finishedAt()));
        return body;
    }

    private static String instant(long epochMillis) {
        return epochMillis == 0 ? null : Instant.ofEpochMilli(epochMillis).toString();
    }
}
