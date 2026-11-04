package es.ulpgc.bigdata.indexing;

import java.util.Optional;

/**
 * Where the publications of the index live (contract §6.5: index_meta CURRENT, index_books,
 * index_publications). DR-05 only reads which version is published; DR-08 adds publishing.
 */
public interface PublicationRepository {

    /** The version CURRENT points to; empty if nothing has been published yet. */
    Optional<Publication> current();
}
