import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Load generator for GET /search, through nginx. No dependencies: java benchmarks/search/LoadGenerator.java
 *
 * Closed loop: each client sends a query, waits for the answer and sends the next one, without
 * pauses. Client concurrency is therefore the number of requests in the system at once. When the
 * system saturates, clients wait instead of queuing more work, so latencies under saturation are
 * lower than an open-loop generator at the same arrival rate would see.
 *
 * Before the load it asks every query once and keeps its total. During the load, an answer with
 * another total or index version counts as a wrong result: more replicas must not change results.
 */
public class LoadGenerator {

    private static final Pattern TOTAL = Pattern.compile("\"total\":(\\d+)");
    private static final Pattern VERSION = Pattern.compile("\"index_version\":(\\d+)");
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

    private static final String HEADER = "timestamp_utc,experiment,replicas,http_threads,mongo_pool,clients,repetition,"
            + "duration_s,ok_requests,http_errors,wrong_results,connection_errors,throughput_rps,"
            + "mean_ms,p50_ms,p95_ms,p99_ms,max_ms,replicas_seen,requests_per_replica";

    private record Expected(String uri, long total, long version) {
    }

    /** What one client saw inside the measurement window. */
    private static final class ClientResult {
        long[] latenciesNanos = new long[4096];
        int ok;
        long httpErrors;
        long wrongResults;
        long connectionErrors;
        final Map<String, Long> servedBy = new HashMap<>();

        void addLatency(long nanos) {
            if (ok == latenciesNanos.length) {
                latenciesNanos = Arrays.copyOf(latenciesNanos, ok * 2);
            }
            latenciesNanos[ok++] = nanos;
        }
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> options = parse(args);
        String url = options.getOrDefault("url", "http://localhost:8080");
        Path queries = Path.of(options.getOrDefault("queries", "shared/queries.txt"));
        int clients = positive(options, "clients", null);
        int warmupSeconds = nonNegative(options, "warmup", "15");
        int durationSeconds = positive(options, "duration", "30");

        HttpClient http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        List<Expected> expected = baseline(http, url, queries);

        long start = System.nanoTime();
        long measureFrom = start + warmupSeconds * 1_000_000_000L;
        long measureTo = measureFrom + durationSeconds * 1_000_000_000L;

        ClientResult[] results = new ClientResult[clients];
        Thread[] threads = new Thread[clients];
        for (int i = 0; i < clients; i++) {
            ClientResult result = new ClientResult();
            results[i] = result;
            Random random = new Random(1000L + i);
            threads[i] = new Thread(() -> runClient(http, expected, random, measureFrom, measureTo, result), "client-" + i);
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }

        String row = summarize(results, options, clients, durationSeconds);
        System.out.println(HEADER);
        System.out.println(row);
        if (options.containsKey("out")) {
            append(Path.of(options.get("out")), row);
        }
    }

    /** Asks each query once. Stops if the system does not answer, or if no query finds any book. */
    private static List<Expected> baseline(HttpClient http, String url, Path queries) throws IOException, InterruptedException {
        List<Expected> expected = new ArrayList<>();
        long withResults = 0;
        for (String line : Files.readAllLines(queries, StandardCharsets.UTF_8)) {
            String q = line.strip();
            if (q.isEmpty() || q.startsWith("#")) {
                continue;
            }
            String uri = url + "/search?q=" + URLEncoder.encode(q, StandardCharsets.UTF_8);
            HttpResponse<String> response = http.send(request(uri), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException("baseline: " + uri + " answered " + response.statusCode());
            }
            long total = number(TOTAL, response.body());
            withResults += total > 0 ? 1 : 0;
            expected.add(new Expected(uri, total, number(VERSION, response.body())));
        }
        if (withResults == 0) {
            throw new IllegalStateException("baseline: no query finds any book, so the load would measure nothing");
        }
        return expected;
    }

    private static void runClient(HttpClient http, List<Expected> queries, Random random,
                                  long measureFrom, long measureTo, ClientResult result) {
        while (System.nanoTime() < measureTo) {
            Expected query = queries.get(random.nextInt(queries.size()));
            long sent = System.nanoTime();
            try {
                HttpResponse<String> response = http.send(request(query.uri()), HttpResponse.BodyHandlers.ofString());
                long received = System.nanoTime();
                if (received >= measureFrom && received < measureTo) {
                    record(response, query, received - sent, result);
                }
            } catch (IOException e) {
                long received = System.nanoTime();
                if (received >= measureFrom && received < measureTo) {
                    result.connectionErrors++;
                }
                sleepAfterFailure();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static void record(HttpResponse<String> response, Expected query, long nanos, ClientResult result) {
        if (response.statusCode() != 200) {
            result.httpErrors++;
            return;
        }
        String body = response.body();
        if (number(TOTAL, body) != query.total() || number(VERSION, body) != query.version()) {
            result.wrongResults++;
            return;
        }
        result.addLatency(nanos);
        response.headers().firstValue("X-Served-By").ifPresent(replica -> result.servedBy.merge(replica, 1L, Long::sum));
    }

    /** A refused connection fails at once: without this, a client would spin and load the host. */
    private static void sleepAfterFailure() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static HttpRequest request(String uri) {
        return HttpRequest.newBuilder(URI.create(uri)).timeout(REQUEST_TIMEOUT).GET().build();
    }

    private static long number(Pattern pattern, String body) {
        Matcher matcher = pattern.matcher(body);
        return matcher.find() ? Long.parseLong(matcher.group(1)) : -1;
    }

    private static String summarize(ClientResult[] results, Map<String, String> options, int clients, int durationSeconds) {
        int ok = 0;
        long httpErrors = 0;
        long wrong = 0;
        long connection = 0;
        Map<String, Long> servedBy = new HashMap<>();
        for (ClientResult result : results) {
            ok += result.ok;
            httpErrors += result.httpErrors;
            wrong += result.wrongResults;
            connection += result.connectionErrors;
            result.servedBy.forEach((replica, count) -> servedBy.merge(replica, count, Long::sum));
        }
        long[] all = new long[ok];
        int next = 0;
        double sum = 0;
        for (ClientResult result : results) {
            System.arraycopy(result.latenciesNanos, 0, all, next, result.ok);
            next += result.ok;
            for (int i = 0; i < result.ok; i++) {
                sum += result.latenciesNanos[i];
            }
        }
        Arrays.sort(all);

        String perReplica = servedBy.values().stream().sorted().map(String::valueOf)
                .reduce((a, b) -> a + ";" + b).orElse("");
        return String.format(Locale.ROOT,
                "%s,%s,%s,%s,%s,%d,%s,%d,%d,%d,%d,%d,%.1f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%s",
                Instant.now(), options.getOrDefault("experiment", "manual"), options.getOrDefault("replicas", ""),
                options.getOrDefault("http-threads", ""), options.getOrDefault("mongo-pool", ""), clients,
                options.getOrDefault("repetition", "1"), durationSeconds, ok, httpErrors, wrong, connection,
                (double) ok / durationSeconds, ok == 0 ? 0 : millis(sum / ok), percentile(all, 0.50),
                percentile(all, 0.95), percentile(all, 0.99), ok == 0 ? 0 : millis(all[ok - 1]),
                servedBy.size(), perReplica);
    }

    /** Nearest-rank percentile, in milliseconds. */
    private static double percentile(long[] sorted, double p) {
        if (sorted.length == 0) {
            return 0;
        }
        int rank = (int) Math.ceil(p * sorted.length);
        return millis(sorted[Math.max(rank - 1, 0)]);
    }

    private static double millis(double nanos) {
        return nanos / 1_000_000.0;
    }

    private static void append(Path file, String row) throws IOException {
        if (file.getParent() != null) {
            Files.createDirectories(file.getParent());
        }
        String text = (Files.exists(file) ? "" : HEADER + "\n") + row + "\n";
        Files.writeString(file, text, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private static Map<String, String> parse(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            if (!args[i].startsWith("--") || i + 1 == args.length) {
                throw new IllegalArgumentException("expected --name value, got: " + args[i]);
            }
            options.put(args[i].substring(2), args[i + 1]);
        }
        return options;
    }

    private static int positive(Map<String, String> options, String name, String byDefault) {
        int value = nonNegative(options, name, byDefault);
        if (value == 0) {
            throw new IllegalArgumentException("--" + name + " must be greater than 0");
        }
        return value;
    }

    private static int nonNegative(Map<String, String> options, String name, String byDefault) {
        String text = options.getOrDefault(name, byDefault);
        if (text == null) {
            throw new IllegalArgumentException("--" + name + " is required");
        }
        int value = Integer.parseInt(text);
        if (value < 0) {
            throw new IllegalArgumentException("--" + name + " must not be negative");
        }
        return value;
    }
}
