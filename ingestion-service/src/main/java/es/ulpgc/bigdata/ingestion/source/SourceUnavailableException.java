package es.ulpgc.bigdata.ingestion.source;

/**
 * The book could not be read now (timeout, connection error, HTTP 5xx or 429, I/O error): a
 * transient error, the book is retried later (DOWNLOAD_FAILED).
 */
public final class SourceUnavailableException extends Exception {

    public SourceUnavailableException(String message) {
        super(message);
    }

    public SourceUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
