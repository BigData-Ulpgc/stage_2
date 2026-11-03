package es.ulpgc.bigdata.indexing;

/**
 * An error the API answers with the common format of api/indexing-openapi.yaml, the same as
 * Search's (api/openapi.yaml):
 * <pre>{"error": {"code": "INVALID_PARAMETER", "message": "...", "field": "book_id"}}</pre>
 * Thrown while handling a request; IndexingApi turns it into the response.
 *
 * Same shape as Search's ApiException (search-service). Candidate to move to common once
 * both services are merged, so the two APIs share one implementation.
 */
public final class ApiException extends RuntimeException {

    private final int status;
    private final String code;
    private final String field;

    public ApiException(int status, String code, String message, String field) {
        super(message);
        this.status = status;
        this.code = code;
        this.field = field;
    }

    /** 400 INVALID_PARAMETER: present but malformed or out of range. */
    public static ApiException invalid(String field, String message) {
        return new ApiException(400, "INVALID_PARAMETER", message, field);
    }

    /** 501 NOT_IMPLEMENTED: the route and its contract exist, the task that implements it does not yet. */
    public static ApiException notImplemented(String route, String task) {
        return new ApiException(501, "NOT_IMPLEMENTED", route + " is not implemented yet (" + task + ")", null);
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
