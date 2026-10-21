package es.ulpgc.bigdata.model;

/**
 * Header line the year of a book was taken from (contract §6.3).
 *
 * Gutenberg's "Release date" is the date of the EBOOK, not of the work, so the
 * year is only the year of the work when it comes from "Original publication".
 * Keeping the source lets Search and the report say which one each year is.
 */
public enum YearSource {

    ORIGINAL_PUBLICATION("original_publication"),
    RELEASE_DATE("release_date");

    private final String key;

    YearSource(String key) {
        this.key = key;
    }

    /** Value stored in the database and returned by the API ("release_date"). */
    public String key() {
        return key;
    }
}
