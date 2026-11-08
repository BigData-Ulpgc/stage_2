package es.ulpgc.bigdata.control;

import es.ulpgc.bigdata.datalake.Datalake;
import es.ulpgc.bigdata.datamart.indexlog.IndexLog;
import es.ulpgc.bigdata.datamart.publication.MongoPublishedBooks;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 * The evidence of the recovery in the persistent storage: index_books and index_log in MongoDB,
 * the _READY markers of the datalake (DATA_DIR, mounted read-only in the Control's container).
 */
public record StorageEvidence(MongoPublishedBooks publishedBooks, IndexLog indexLog, Datalake datalake)
        implements Recovery.Evidence {

    public StorageEvidence {
        Objects.requireNonNull(publishedBooks, "publishedBooks");
        Objects.requireNonNull(indexLog, "indexLog");
        Objects.requireNonNull(datalake, "datalake");
    }

    @Override
    public Set<Integer> published() {
        return publishedBooks.publishedIds();
    }

    @Override
    public Set<Integer> indexed() {
        return indexLog.indexedIds();
    }

    @Override
    public Collection<Integer> downloaded() {
        return datalake.listBookIds();
    }
}
