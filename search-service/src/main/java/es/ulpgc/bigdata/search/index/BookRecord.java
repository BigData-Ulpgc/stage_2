package es.ulpgc.bigdata.search.index;

/**
 * Metadata of one book as the Search Service returns it (row of the `books` table of
 * metadata.db, contract §6.3). Any field except the id may be null.
 *
 * @param language   ISO 639-1 code in lower case ("en"), or null if not recognised
 * @param year       original publication year, or the year of the Gutenberg release date
 * @param yearSource "original_publication" or "release_date": where {@code year} comes from
 */
public record BookRecord(
        int bookId,
        String title,
        String author,
        String language,
        Integer year,
        String yearSource,
        String releaseDate
) {
    /** A book that is in the index but has no row in metadata.db yet. */
    static BookRecord withoutMetadata(int bookId) {
        return new BookRecord(bookId, null, null, null, null, null, null);
    }
}
