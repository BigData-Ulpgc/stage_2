package es.ulpgc.bigdata.model;

import java.nio.file.Path;

/**
 * Descriptive data of a book, extracted from the header and from its
 * location in the datalake. Fields missing from the header are null,
 * according to the common contract (shared/SPEC.md).
 *
 * Stage 2 additions (contract §6.3), used by the search filters:
 *  - language is an ISO 639-1 code ("en"), or null if it is not recognised;
 *  - year is the year of the work if the header has "Original publication",
 *    else the year of the ebook's "Release date"; yearSource says which one.
 *    Both are null, or neither is.
 */
public record BookMetadata(
        int bookId,
        String title,
        String author,
        String language,
        Integer year,
        YearSource yearSource,
        String releaseDate,
        Path bodyPath,
        Path headerPath
) {

    public BookMetadata {
        if ((year == null) != (yearSource == null)) {
            throw new IllegalArgumentException(
                    "year y yearSource van juntos: year=" + year + ", yearSource=" + yearSource);
        }
    }
}
