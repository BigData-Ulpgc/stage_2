package es.ulpgc.bigdata.config;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Reads the configuration of a service from environment variables, each with a default value.
 *
 * Environment variables are what Docker Compose sets for each container, and what a shell sets
 * for a jar started with java -jar, so the same code runs in both deployments. No port, path or
 * thread count is written in the code of a service: it asks for it here.
 *
 * A variable that is set but malformed (INDEXING_PORT=abc) stops the service at startup with a
 * message naming the variable, instead of silently falling back to the default.
 */
public final class Env {

    private final Function<String, String> source;

    private Env(Function<String, String> source) {
        this.source = Objects.requireNonNull(source, "source");
    }

    /** The real environment of the process. */
    public static Env system() {
        return new Env(System::getenv);
    }

    /** A fixed set of variables, for tests. */
    public static Env of(Map<String, String> variables) {
        Map<String, String> copy = Map.copyOf(variables);
        return new Env(copy::get);
    }

    /** The value of the variable, trimmed; the default if it is unset or blank. */
    public String string(String name, String defaultValue) {
        String value = source.apply(name);
        return value == null || value.isBlank() ? defaultValue : value.strip();
    }

    /** An integer variable; the default if it is unset or blank. */
    public int integer(String name, int defaultValue) {
        String value = string(name, null);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer, got '" + value + "'");
        }
    }

    /** An integer that must be at least {@code min} (thread counts, sizes...). */
    public int integerAtLeast(String name, int defaultValue, int min) {
        int value = integer(name, defaultValue);
        if (value < min) {
            throw new IllegalArgumentException(name + " must be >= " + min + ", got " + value);
        }
        return value;
    }

    /** A TCP port: 1-65535, or 0 to let the operating system choose a free one (tests). */
    public int port(String name, int defaultValue) {
        int value = integer(name, defaultValue);
        if (value < 0 || value > 65535) {
            throw new IllegalArgumentException(name + " must be a port between 0 and 65535, got " + value);
        }
        return value;
    }
}
