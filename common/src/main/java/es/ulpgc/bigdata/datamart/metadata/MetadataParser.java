package es.ulpgc.bigdata.datamart.metadata;

import es.ulpgc.bigdata.model.BookLocation;
import es.ulpgc.bigdata.model.BookMetadata;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts the metadata from a Project Gutenberg header (shared/SPEC.md).
 *
 * Rules of the common contract (the same in Java, Python and C):
 *  - the FIRST line starting with "Title:", "Author:", ... is searched for;
 *  - only the first line of the value counts;
 *  - spaces are trimmed; a missing or empty field is null;
 *  - in release_date whatever is between brackets is discarded: "[eBook #1342]".
 *
 * It does not read files or save anything: it receives text and returns a BookMetadata.
 */
public class MetadataParser {

    // The SPEC regexes, compiled only once when the class is loaded.
    // MULTILINE: ^ and $ mean start and end of EACH LINE, not of the whole text.
    private static final Pattern TITLE =
            Pattern.compile("^Title:\\s*(.+)$", Pattern.MULTILINE);
    private static final Pattern AUTHOR =
            Pattern.compile("^Author:\\s*(.+)$", Pattern.MULTILINE);
    private static final Pattern RELEASE_DATE =
            Pattern.compile("^Release date:\\s*(.+?)(\\s*\\[.*)?$", Pattern.MULTILINE);
    private static final Pattern LANGUAGE =
            Pattern.compile("^Language:\\s*(.+)$", Pattern.MULTILINE);

    /**
     * @param location where the id and the paths come from (returned by the datalake)
     * @param header   content of that book's header.txt
     */
    public BookMetadata parse(BookLocation location, String header) {
        Objects.requireNonNull(location, "location");
        Objects.requireNonNull(header, "header");

        String text = normalizeLineEndings(header);

        return new BookMetadata(
                location.id(),
                firstMatch(TITLE, text),
                firstMatch(AUTHOR, text),
                firstMatch(LANGUAGE, text),
                firstMatch(RELEASE_DATE, text),
                location.bodyPath(),
                location.headerPath());
    }

    /** CRLF (Windows) -> LF, so that no \r ends up inside a value. */
    static String normalizeLineEndings(String text) {
        return text.replace("\r\n", "\n");
    }

    /** Group 1 of the first match, trimmed; null if there is none or it ends up empty. */
    static String firstMatch(Pattern pattern, String text) {
        Matcher m = pattern.matcher(text);
        if (!m.find()) {
            return null;
        }
        String value = m.group(1).strip();
        return value.isEmpty() ? null : value;
    }
}