package es.ulpgc.bigdata.ingestion;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * api/ingestion-openapi.yaml must describe the running service (assignment §3): every route and
 * every error code of IngestionApi. The structure and the examples of the file are checked with
 * openapi-spec-validator and openapi-schema-validator (see the PR of CF-06).
 */
class OpenApiContractTest {

    /** Maven runs the tests from ingestion-service/, the contract is at the repository root. */
    private static final Path OPENAPI = Path.of("..", "api", "ingestion-openapi.yaml");

    @Test
    void everyRouteIsInTheContract() throws Exception {
        String spec = Files.readString(OPENAPI, StandardCharsets.UTF_8);
        for (String route : List.of("/ingest/{book_id}:", "/ingest/status/{book_id}:", "/ingest/list:", "/status:")) {
            assertTrue(spec.contains("\n  " + route), route + " missing in " + OPENAPI);
        }
    }

    @Test
    void theErrorCodesAreExactlyTheOnesTheServiceUses() throws Exception {
        String spec = Files.readString(OPENAPI, StandardCharsets.UTF_8);
        Matcher codes = Pattern.compile("enum: \\[(INVALID_PARAMETER[A-Z_, ]+)]").matcher(spec);
        assertTrue(codes.find(), "no error code enum in " + OPENAPI);
        assertEquals(List.of("INVALID_PARAMETER", "NOT_FOUND", "INVALID_BOOK", "SOURCE_UNAVAILABLE", "SOURCE_TIMEOUT", "INTERNAL_ERROR"),
                Arrays.stream(codes.group(1).split(",")).map(String::strip).toList());
    }
}
