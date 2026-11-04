package es.ulpgc.bigdata.coordination;

/**
 * States of a book (contract §4). In Hazelcast they are stored as their name, a String, so
 * that the members can run queries on them without our classes.
 */
public enum BookState {
    PENDING,
    INGESTING,
    DOWNLOADED,
    INDEXING,
    INDEXED,
    PUBLISHED,
    FAILED
}
