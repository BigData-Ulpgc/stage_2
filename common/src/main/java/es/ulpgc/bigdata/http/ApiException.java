package es.ulpgc.bigdata.http;

/**
 * An error that an API answers with the common error format of every service
 * (api/openapi.yaml, api/indexing-openapi.yaml):
 * <pre>{"error": {"code": "INVALID_PARAMETER", "message": "...", "field": "book_id"}}</pre>
 * Thrown anywhere while handling a request; the handler that ServiceServer installs turns it
 * into the response, so a route only has to throw it.
 *
 * Only the factories that every service shares live here. A code that belongs to one service
 * (Search's INDEX_NOT_READY, Indexing's NOT_IMPLEMENTED) is built in that service with the
 * public constructor. The message is part of the public contract, so it is in English.
 */
public final class ApiException extends RuntimeException {

    private final int status;
    private final String code;
    private final String field;

    /**
     * @param status HTTP status, e.g. 400
     * @param code   machine-readable code of the contract, e.g. "INVALID_PARAMETER"
     * @param field  request parameter that caused the error, or null
     */
    public ApiException(int status, String code, String message, String field) {
        super(message);
        this.status = status;
        this.code = code;
        this.field = field;
    }

    /** 400 MISSING_PARAMETER: a required parameter is absent or blank. */
    public static ApiException missing(String field) {
        return new ApiException(400, "MISSING_PARAMETER", field + " is required", field);
    }

    /** 400 INVALID_PARAMETER: present but malformed or out of range. */
    public static ApiException invalid(String field, String message) {
        return new ApiException(400, "INVALID_PARAMETER", message, field);
    }

    public int status() {
        return status;
    }

    public String code() {
        return code;
    }

    public String field() {
        return field;
    }
}
