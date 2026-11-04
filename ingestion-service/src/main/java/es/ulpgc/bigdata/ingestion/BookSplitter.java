package es.ulpgc.bigdata.ingestion;

import es.ulpgc.bigdata.model.RawBook;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Splits the raw text of a Project Gutenberg book into header and body, discarding the footer
 * (Stage 1 SPEC §2):
 *
 * <pre>
 *   header = everything before the "*** START OF THE|THIS PROJECT GUTENBERG EBOOK" line, trimmed
 *   body   = everything after that line and before the "*** END OF ..." marker, trimmed
 * </pre>
 *
 * Line endings are normalized to \n. Pure: no I/O, thread-safe. Migrated from Stage 1 with one
 * guard added (END marker on the START line); its output is checked against
 * stage_1/sample_dataset/book in the tests.
 *
 * An empty result means the text has no START/END markers: a permanent error for that book
 * (INVALID_BOOK in the coordination contract, 422 in POST /ingest).
 */
public final class BookSplitter {

    private static final Pattern START_MARKER = Pattern.compile(
            "\\*\\*\\* START OF (THE|THIS) PROJECT GUTENBERG EBOOK.*");
    private static final Pattern END_MARKER = Pattern.compile(
            "\\*\\*\\* END OF (THE|THIS) PROJECT GUTENBERG EBOOK.*");

    public Optional<RawBook> split(int bookId, String rawText) {
        String normalized = rawText.replace("\r\n", "\n");

        Matcher start = START_MARKER.matcher(normalized);
        if (!start.find()) {
            return Optional.empty();
        }
        Matcher end = END_MARKER.matcher(normalized);
        if (!end.find(start.end())) {
            return Optional.empty();
        }
        int startLineEnd = normalized.indexOf('\n', start.end());
        if (startLineEnd == -1 || startLineEnd > end.start()) {
            return Optional.empty();              // END on the START line: Stage 1 threw here
        }

        String header = normalized.substring(0, start.start()).trim();
        String body = normalized.substring(startLineEnd, end.start()).trim();
        return Optional.of(new RawBook(bookId, header, body));
    }
}
