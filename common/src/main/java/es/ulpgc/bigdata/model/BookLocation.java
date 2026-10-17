package es.ulpgc.bigdata.model;

import java.nio.file.Path;

/**
 * Result of saving a book in the datalake: where the header and the body
 * physically ended up. It does not contain the text, only the paths.
 */
public record BookLocation(int id, Path headerPath, Path bodyPath) {
}
