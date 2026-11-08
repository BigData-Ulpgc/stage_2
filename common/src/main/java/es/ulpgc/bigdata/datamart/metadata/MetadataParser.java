package es.ulpgc.bigdata.datamart.metadata;

import es.ulpgc.bigdata.model.BookLocation;
import es.ulpgc.bigdata.model.BookMetadata;
import es.ulpgc.bigdata.model.YearSource;

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
 * Stage 2 rules (contract §6.3, checked against the 200 reference books in the
 * DR-04 header survey):
 *  - language: ISO 639-1 code of the first language (LanguageCodes), null if unknown;
 *  - year: last 4-digit number of "Original publication" (year of the work) or, if
 *    there is none, of "Release date" (year of the ebook); yearSource says which;
 *  - author: "Author:", or "Creator:" if there is no "Author:". "Translator:" never
 *    counts: the translator is not the author.
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
    private static final Pattern CREATOR =
            Pattern.compile("^Creator:\\s*(.+)$", Pattern.MULTILINE);
    private static final Pattern RELEASE_DATE =
            Pattern.compile("^Release date:\\s*(.+?)(\\s*\\[.*)?$", Pattern.MULTILINE);
    private static final Pattern ORIGINAL_PUBLICATION =
            Pattern.compile("^Original publication:\\s*(.+)$", Pattern.MULTILINE);
    private static final Pattern LANGUAGE =
            Pattern.compile("^Language:\\s*(.+)$", Pattern.MULTILINE);

    /** A 4-digit number on its own ("1998", not the "1998" inside "19987"). */
    private static final Pattern YEAR = Pattern.compile("\\b(\\d{4})\\b");

    /**
     * @param location where the id and the paths come from (returned by the datalake)
     * @param header   content of that book's header.txt
     */
    public BookMetadata parse(BookLocation location, String header) {
        Objects.requireNonNull(location, "location");
        Objects.requireNonNull(header, "header");

        String text = normalizeLineEndings(header);

        String author = firstMatch(AUTHOR, text);
        if (author == null) {
            author = firstMatch(CREATOR, text);
        }

        String releaseDate = firstMatch(RELEASE_DATE, text);
        Integer originalYear = yearOf(firstMatch(ORIGINAL_PUBLICATION, text));
        Integer releaseYear = yearOf(releaseDate);

        Integer year;
        YearSource yearSource;
        if (originalYear != null) {
            year = originalYear;
            yearSource = YearSource.ORIGINAL_PUBLICATION;
        } else if (releaseYear != null) {
            year = releaseYear;
            yearSource = YearSource.RELEASE_DATE;
        } else {
            year = null;
            yearSource = null;
        }

        return new BookMetadata(
                location.id(),
                firstMatch(TITLE, text),
                author,
                LanguageCodes.toIso(firstMatch(LANGUAGE, text)),
                year,
                yearSource,
                releaseDate,
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

    /**
     * LAST 4-digit number of a date ("June 1, 1998" -> 1998;
     * "London: Griffiths, Becket, and DeHondt, 1761" -> 1761), or null.
     * The last one, because the year closes both formats seen in the headers.
     */
    static Integer yearOf(String text) {
        if (text == null) {
            return null;
        }
        Matcher m = YEAR.matcher(text);
        Integer last = null;
        while (m.find()) {
            last = Integer.parseInt(m.group(1));
        }
        return last;
    }
}
