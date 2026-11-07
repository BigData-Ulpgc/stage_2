package es.ulpgc.bigdata.indexing;

/**
 * What the rest of the system learns of a publication (DR-08b), outside MongoDB: Publisher
 * calls it after every publish, inside its lock, so two calls never overlap and the events
 * leave in the order of the versions. Being inside the lock, it must return at once: an
 * implementation that talks to another system queues the work for a thread of its own.
 * Publisher does not know Hazelcast: the implementation of the service is
 * HazelcastPublicationNotifier.
 *
 * A failure here never undoes nor fails the publication: CURRENT has already switched, which
 * is the truth (MongoDB). Publisher logs it and answers 200 anyway; what was not done is
 * repaired later (contract coordination.md §6, §7).
 */
public interface PublicationNotifier {

    /** Notifies nobody: for the tests and for a service without coordination. */
    PublicationNotifier NONE = result -> { };

    /**
     * After every publish call that answered, with what it answered: a new version
     * ({@code booksPublished > 0}), or CURRENT as it was (nothing new; {@code version} null if
     * nothing has ever been published).
     */
    void afterPublish(PublishResult result);

    /**
     * After the switch of a rebuild (DR-10, Publisher.publishInto): always a new version, also
     * when no book is new ({@code booksPublished} 0), because Search must load the new collection.
     */
    default void afterSwitch(PublishResult result) {
        afterPublish(result);
    }
}
