package es.ulpgc.bigdata.search.http;

import io.javalin.config.JavalinConfig;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Which web origins may call this API from a browser (variable CORS_ORIGINS). The front-end is
 * served from another origin, and without these headers the browser would hide the responses.
 *
 * <pre>
 *   CORS_ORIGINS unset                                      no origin: only non-browser clients
 *   CORS_ORIGINS=http://localhost:3000,https://example.org  those origins
 *   CORS_ORIGINS=*                                          any origin
 * </pre>
 *
 * An invalid value stops the start with a message naming the variable, like every other setting.
 */
public record CorsPolicy(List<String> origins) {

    public static final String VARIABLE = "CORS_ORIGINS";
    public static final CorsPolicy NONE = new CorsPolicy(List.of());

    private static final String ANY = "*";
    private static final Pattern ORIGIN = Pattern.compile("https?://[^/\\s]+");

    public CorsPolicy {
        origins = List.copyOf(origins);
    }

    public static CorsPolicy parse(String value) {
        if (value == null || value.isBlank()) {
            return NONE;
        }
        List<String> origins = Arrays.stream(value.split(",")).map(String::strip).toList();
        if (origins.equals(List.of(ANY))) {
            return new CorsPolicy(origins);
        }
        for (String origin : origins) {
            if (!ORIGIN.matcher(origin).matches()) {
                throw new IllegalArgumentException(VARIABLE + " must be * or a comma-separated list of origins"
                        + " like http://localhost:3000, got '" + origin + "'");
            }
        }
        return new CorsPolicy(origins);
    }

    /** Adds the rule to the server being created; does nothing if no origin is allowed. */
    public void applyTo(JavalinConfig config) {
        if (origins.isEmpty()) {
            return;
        }
        config.bundledPlugins.enableCors(cors -> cors.addRule(rule -> {
            if (origins.equals(List.of(ANY))) {
                rule.anyHost();
            } else {
                rule.allowHost(origins.get(0), origins.subList(1, origins.size()).toArray(String[]::new));
            }
        }));
    }
}
