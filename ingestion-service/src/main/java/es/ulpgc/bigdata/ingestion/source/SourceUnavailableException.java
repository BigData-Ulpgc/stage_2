package es.ulpgc.bigdata.ingestion.source;

/**
 * The book could not be read now (timeout, connection error, HTTP 5xx or 429, I/O error): a
 * transient error, the book is retried later (DOWNLOAD_FAILED).
 *
 * When the source answered with an HTTP error, {@link #httpStatus()} has its code, so a caller
 * can tell throttling (429, or the 403 of an anti-bot protection) from a failing server without
 * reading the message.
 */
public final class SourceUnavailableException extends Exception {

    /** No HTTP answer: timeout, connection or read error. */
    public static final int NO_HTTP_STATUS = -1;

    private final int httpStatus;

    public SourceUnavailableException(String message) {
        this(message, NO_HTTP_STATUS);
    }

    public SourceUnavailableException(String message, int httpStatus) {
        super(message);
        this.httpStatus = httpStatus;
    }

    public SourceUnavailableException(String message, Throwable cause) {
        super(message, cause);
        this.httpStatus = NO_HTTP_STATUS;
    }

    /** The HTTP status the source answered, or {@link #NO_HTTP_STATUS}. */
    public int httpStatus() {
        return httpStatus;
    }

    /**
     * The source is refusing us, not failing: HTTP 429 (too many requests) or 403 (what the
     * anti-bot protection of Gutenberg answers). The whole replica should slow down.
     */
    public boolean isThrottling() {
        return httpStatus == 429 || httpStatus == 403;
    }
}
