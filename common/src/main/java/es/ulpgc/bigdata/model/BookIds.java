package es.ulpgc.bigdata.model;

import java.util.OptionalInt;
import java.util.regex.Pattern;

/**
 * The single definition of a valid book id for every service: an integer from 1 to
 * 999,999,999 (MAX), written in canonical form.
 *
 * Canonical form: only ASCII digits, no sign, no leading zeros, no spaces. Integer.parseInt
 * is too permissive for an API: it accepts "+84", "0084" and non-ASCII digits such as
 * "١٢٣" (Arabic-Indic), so the same book could be named in several ways. Here each id has
 * exactly one valid text, the one Integer.toString gives.
 *
 * Why 9 digits: every canonical text fits in an int (no overflow to handle), and it is the
 * same limit the datalake uses for its file names (AbstractFileDatalake), so any id the API
 * accepts can be stored. Project Gutenberg is around 75,000.
 *
 * Pure: nothing of HTTP. Each API turns an empty result into its own 400, and the index-queue
 * consumer (DR-06) can check the ids it takes from Hazelcast with isValid.
 */
public final class BookIds {

    /** Largest valid id: 9 digits. */
    public static final int MAX = 999_999_999;

    /** [0-9] and not \d, to make it explicit that only ASCII digits are accepted. */
    private static final Pattern CANONICAL = Pattern.compile("[1-9][0-9]{0,8}");

    private BookIds() {
    }

    /** "1342" -> 1342 ; null, "", "0", "+84", "0084", " 84", "1e3", "١٢٣", "1000000000" -> empty. */
    public static OptionalInt parse(String text) {
        if (text == null || !CANONICAL.matcher(text).matches()) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(Integer.parseInt(text));      // cannot fail: at most 9 digits
    }

    /** The same rule for an id that is already an int (e.g. taken from a queue). */
    public static boolean isValid(int id) {
        return id >= 1 && id <= MAX;
    }
}
