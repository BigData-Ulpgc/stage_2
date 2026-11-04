package es.ulpgc.bigdata.indexing;

import java.util.Optional;

/** Fake PublicationRepository for the tests: the test decides what CURRENT says. */
class InMemoryPublicationRepository implements PublicationRepository {

    private volatile Publication current;

    /** null = nothing published yet. */
    void setCurrent(Publication current) {
        this.current = current;
    }

    @Override
    public Optional<Publication> current() {
        return Optional.ofNullable(current);
    }
}
