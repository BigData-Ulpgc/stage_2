package es.ulpgc.bigdata.search.http;

import es.ulpgc.bigdata.datamart.metadata.LanguageCodes;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * The language codes written in api/openapi.yaml must be exactly the ones the code accepts
 * (LanguageCodes in common, used by MetadataParser and by SearchQuery). If someone adds a
 * language to one and not the other, this test fails instead of the API lying.
 * (Suggested by Daniel R. in the review of PR #8.)
 */
class OpenApiContractTest {

    /** Maven runs the tests from search-service/, the contract is at the repository root. */
    private static final Path OPENAPI = Path.of("..", "api", "openapi.yaml");

    @Test
    void languageEnumMatchesLanguageCodes() throws Exception {
        String spec = Files.readString(OPENAPI, StandardCharsets.UTF_8);
        Matcher enums = Pattern.compile("enum: \\[(da, [a-z, ]+)]").matcher(spec);
        List<List<String>> found = new ArrayList<>();
        while (enums.find()) {
            found.add(Arrays.stream(enums.group(1).split(",")).map(String::strip).toList());
        }

        assertFalse(found.isEmpty(), "no language enum found in " + OPENAPI);
        for (List<String> codes : found) {
            assertEquals(List.copyOf(LanguageCodes.supportedCodes()), codes);
        }
    }
}
