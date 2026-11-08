package es.ulpgc.bigdata.datamart.metadata;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Gutenberg language names -> ISO 639-1 codes (contract §6.3).
 *
 * Same 15 languages as Search's SampleFixtureGenerator (team decision, 2026-10-21).
 * The 200 reference books are all "English" (DR-04 header survey); the other entries
 * cover other datasets. An unknown name gives null: the book is still indexed, it
 * just cannot be found with the language filter.
 *
 * Search validates its "language" parameter against supportedCodes(), so the parser
 * and the API accept exactly the same codes.
 */
public final class LanguageCodes {

    private static final Map<String, String> ISO_639_1 = Map.ofEntries(
            Map.entry("english", "en"), Map.entry("spanish", "es"), Map.entry("french", "fr"),
            Map.entry("german", "de"), Map.entry("italian", "it"), Map.entry("portuguese", "pt"),
            Map.entry("dutch", "nl"), Map.entry("latin", "la"), Map.entry("finnish", "fi"),
            Map.entry("swedish", "sv"), Map.entry("danish", "da"), Map.entry("greek", "el"),
            Map.entry("russian", "ru"), Map.entry("chinese", "zh"), Map.entry("esperanto", "eo"));

    /** Separators between languages: "English, French", "English; French", "English and French". */
    private static final Pattern SEPARATOR = Pattern.compile(",|;|\\band\\b");

    private LanguageCodes() {
    }

    /**
     * ISO code of the FIRST language of a "Language:" value (team decision for
     * multi-language books), or null if the value is null or the name is unknown.
     */
    public static String toIso(String headerValue) {
        if (headerValue == null) {
            return null;
        }
        String first = SEPARATOR.split(headerValue, 2)[0];
        return ISO_639_1.get(first.strip().toLowerCase(Locale.ROOT));
    }

    /** The codes toIso can return, sorted ("da", "de", "el", "en", ...). */
    public static SortedSet<String> supportedCodes() {
        return Collections.unmodifiableSortedSet(new TreeSet<>(ISO_639_1.values()));
    }
}
