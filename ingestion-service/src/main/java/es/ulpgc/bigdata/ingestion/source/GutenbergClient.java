package es.ulpgc.bigdata.ingestion.source;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Downloads the raw text of a book from Project Gutenberg: {@code <base>/<id>/pg<id>.txt}
 * (Stage 1 SPEC §2). Migrated from Stage 1; what changed:
 * <ul>
 *   <li>404 and 410 are BookNotFoundException; every other failure (5xx, 429, a 403 of the
 *       anti-bot protection, timeout, network) is SourceUnavailableException. Stage 1 returned
 *       an empty Optional for all of them.</li>
 *   <li>The base URL is a parameter (INGESTION_GUTENBERG_URL), so the tests use a local HTTP
 *       server and never Gutenberg.</li>
 *   <li>It sends a User-Agent that says who downloads, and follows redirects.</li>
 *   <li>INGESTION_REQUEST_TIMEOUT_SECONDS bounds the WHOLE download, body included (the
 *       timeout of HttpRequest alone only covers the headers).</li>
 * </ul>
 * It does not retry: the worker decides (CF-08), and the coordination contract requeues the
 * book on a transient failure.
 *
 * Thread-safe: one instance is shared by every worker of the replica (one HttpClient, one
 * connection pool).
 */
public final class GutenbergClient implements BookSource {

    public static final String DEFAULT_BASE_URL = "https://www.gutenberg.org/cache/epub";
    static final String USER_AGENT = "stage2-search-engine/0.1 (ULPGC Big Data course; educational use)";

    private final String baseUrl;
    private final Duration requestTimeout;
    private final HttpClient http;

    public GutenbergClient(String baseUrl, Duration connectTimeout, Duration requestTimeout) {
        Objects.requireNonNull(baseUrl, "baseUrl");
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        URI.create(this.baseUrl);                                 // fails at startup on a malformed URL
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
        this.http = HttpClient.newBuilder()
                .connectTimeout(Objects.requireNonNull(connectTimeout, "connectTimeout"))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    @Override
    public String fetch(int bookId) throws BookNotFoundException, SourceUnavailableException, InterruptedException {
        URI uri = uriOf(bookId);
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(requestTimeout)
                .header("User-Agent", USER_AGENT)
                .GET()
                .build();
        HttpResponse<String> response;
        // The timeout of HttpRequest only covers the response HEADERS: a body that arrives
        // slowly can last much longer (measured: a 3 s body with a 1 s timeout). The whole
        // download, body included, is bounded here, so one attempt fits within the lease.
        CompletableFuture<HttpResponse<String>> download = http.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        try {
            response = download.get(requestTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            download.cancel(true);                                 // aborts the exchange
            throw timeout(uri, new HttpTimeoutException("no complete response in " + requestTimeout.toSeconds() + " s"));
        } catch (InterruptedException e) {
            download.cancel(true);
            throw e;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof HttpTimeoutException timeout) {
                throw timeout(uri, timeout);
            }
            if (cause instanceof IOException io) {
                throw new SourceUnavailableException(io.getClass().getSimpleName() + " downloading " + uri
                        + (io.getMessage() == null ? "" : ": " + io.getMessage()), io);
            }
            throw new IllegalStateException("unexpected failure downloading " + uri, cause);
        }
        int status = response.statusCode();
        if (status >= 200 && status < 300) {
            return response.body();
        }
        if (status == 404 || status == 410) {
            throw new BookNotFoundException(bookId, "HTTP " + status + " from " + uri);
        }
        throw new SourceUnavailableException("HTTP " + status + " from " + uri, status);
    }

    /** The cause is an HttpTimeoutException, so the API answers 504 (IngestionApi). */
    private SourceUnavailableException timeout(URI uri, HttpTimeoutException cause) {
        return new SourceUnavailableException("timeout (" + requestTimeout.toSeconds() + " s) downloading " + uri, cause);
    }

    URI uriOf(int bookId) {
        return URI.create(baseUrl + "/" + bookId + "/pg" + bookId + ".txt");
    }

    @Override
    public String describe() {
        return "gutenberg " + baseUrl;
    }
}
