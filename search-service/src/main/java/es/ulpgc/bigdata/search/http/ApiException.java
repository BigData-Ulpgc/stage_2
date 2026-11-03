package es.ulpgc.bigdata.search.http;

/**
 * An error the API answers with the common format of api/openapi.yaml:
 * <pre>{"error": {"code": "INVALID_PARAMETER", "message": "...", "field": "limit"}}</pre>
 * Thrown anywhere while handling a request; SearchApi turns it into the response.
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

    /** 400 MISSING_PARAMETER: a required parameter is absent or blank. */
    public static ApiException missing(String field) {
        return new ApiException(400, "MISSING_PARAMETER", field + " is required", field);
    }

    /** 400 INVALID_PARAMETER: present but malformed or out of range. */
    public static ApiException invalid(String field, String message) {
        return new ApiException(400, "INVALID_PARAMETER", message, field);
    }

    /** 503 INDEX_NOT_READY: no index version has been published (or loaded) yet. */
    public static ApiException indexNotReady() {
        return new ApiException(503, "INDEX_NOT_READY", "no index version has been published yet", null);
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
