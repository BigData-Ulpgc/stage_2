package es.ulpgc.bigdata.control;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Calls POST /index/publish of the Indexing Service (api/indexing-openapi.yaml), as its contract
 * asks of a caller: connect timeout 2 s, request timeout 30 s, and every failure (timeout,
 * connection error, 409 REBUILD_IN_PROGRESS, 503 STORAGE_UNAVAILABLE...) is safe to retry,
 * because a publication is idempotent: a retry after a lost answer returns the version the
 * first call created.
 *
 * The 30 s bound the whole exchange, body included (the timeout of HttpRequest alone only covers
 * the headers, see GutenbergClient). It matters here: Indexing notifies Hazelcast inside its
 * publication, and with a cluster that dropped silently that can take up to the 120 s of a
 * client invocation (review of #32); the Control gives up after 30 s and tries again later.
 */
public class IndexingClient {

    /** What POST /index/publish answered. {@code version} is null if nothing was ever published. */
    public record PublishOutcome(Integer version, int booksPublished, long totalBooks, String publishedAt) {
    }

    /** The publication did not happen or its answer was lost: retry later. */
    public static final class PublishFailedException extends Exception {
        public PublishFailedException(String message) {
            super(message);
        }

        public PublishFailedException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(2);
    static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

    private final URI publish;
    private final Duration requestTimeout;
    private final HttpClient http;

    public IndexingClient(URI indexingUrl) {
        this(indexingUrl, REQUEST_TIMEOUT);
    }

    IndexingClient(URI indexingUrl, Duration requestTimeout) {
        this.publish = URI.create(Objects.requireNonNull(indexingUrl, "indexingUrl") + "/index/publish");
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
        this.http = HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build();
    }

    public PublishOutcome publish() throws PublishFailedException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(publish)
                .timeout(requestTimeout)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        CompletableFuture<HttpResponse<String>> call = http.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        HttpResponse<String> response;
        try {
            response = call.get(requestTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            call.cancel(true);
            throw new PublishFailedException("no answer from " + publish + " in " + requestTimeout.toSeconds() + " s", e);
        } catch (InterruptedException e) {
            call.cancel(true);
            throw e;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw new PublishFailedException(cause.getClass().getSimpleName() + " calling " + publish
                    + (cause.getMessage() == null ? "" : ": " + cause.getMessage()), cause instanceof IOException ? cause : e);
        }
        if (response.statusCode() != 200) {
            throw new PublishFailedException("HTTP " + response.statusCode() + " from " + publish + ": "
                    + response.body().substring(0, Math.min(200, response.body().length())));
        }
        try {
            JsonObject body = JsonParser.parseString(response.body()).getAsJsonObject();
            if (!body.has("books_published") || !body.has("total_books")) {
                throw new PublishFailedException("answer of " + publish + " without its fields: " + response.body());
            }
            JsonElement version = body.get("version");
            JsonElement publishedAt = body.get("published_at");
            return new PublishOutcome(
                    version == null || version.isJsonNull() ? null : version.getAsInt(),
                    body.get("books_published").getAsInt(),
                    body.get("total_books").getAsLong(),
                    publishedAt == null || publishedAt.isJsonNull() ? null : publishedAt.getAsString());
        } catch (JsonSyntaxException | IllegalStateException | UnsupportedOperationException | NumberFormatException e) {
            throw new PublishFailedException("unexpected answer of " + publish + ": " + response.body(), e);
        }
    }
}
