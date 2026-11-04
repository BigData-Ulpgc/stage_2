package es.ulpgc.bigdata.indexing;

/**
 * The published version of the index, as the CURRENT document of index_meta says (§6.5):
 * Search shows the books published with a version <= version; books = how many they are.
 */
public record Publication(int version, long books) {
}
