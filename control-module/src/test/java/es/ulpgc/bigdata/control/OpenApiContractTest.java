package es.ulpgc.bigdata.control;

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
 * api/control-openapi.yaml must describe the running service (assignment §3): every route and
 * every error code of ControlApi. The structure and the examples of the file are checked with
 * openapi-spec-validator and openapi-schema-validator (see the PR of CF-09).
 */
class OpenApiContractTest {

    /** Maven runs the tests from control-module/, the contract is at the repository root. */
    private static final Path OPENAPI = Path.of("..", "api", "control-openapi.yaml");

    @Test
    void everyRouteIsInTheContract() throws Exception {
        String spec = Files.readString(OPENAPI, StandardCharsets.UTF_8);
        for (String route : List.of("/control/jobs:", "/control/status:", "/control/tasks/{book_id}:",
                "/control/retry-failed:", "/status:")) {
            assertTrue(spec.contains("\n  " + route), route + " missing in " + OPENAPI);
        }
    }

    @Test
    void theErrorCodesAreExactlyTheOnesTheServiceUses() throws Exception {
        String spec = Files.readString(OPENAPI, StandardCharsets.UTF_8);
        Matcher codes = Pattern.compile("enum: \\[(INVALID_PARAMETER[A-Z_, ]+)]").matcher(spec);
        assertTrue(codes.find(), "no error code enum in " + OPENAPI);
        assertEquals(List.of("INVALID_PARAMETER", "MISSING_PARAMETER", "NOT_FOUND", "COORDINATION_UNAVAILABLE", "INTERNAL_ERROR"),
                Arrays.stream(codes.group(1).split(",")).map(String::strip).toList());
    }
}
