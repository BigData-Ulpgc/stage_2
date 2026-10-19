package es.ulpgc.bigdata.model;

import java.nio.file.Path;

/**
 * Descriptive data of a book, extracted from the header and from its
 * location in the datalake. Fields missing from the header are null,
 * according to the common contract (shared/SPEC.md).
 */

public record BookMetadata(
        int bookId,
        String title,
        String author,
        String language,
        String releaseDate,
        Path bodyPath,
        Path headerPath
) {
}