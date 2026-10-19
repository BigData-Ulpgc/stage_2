package es.ulpgc.bigdata.model;

/**
 * Represents a book right after downloading it and splitting
 * header/body, BEFORE saving it in the datalake.
 */
public record RawBook(int id, String header, String body) {
}