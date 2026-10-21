package es.ulpgc.bigdata.datamart.metadata;

/**
 * Error of the metadata store. It wraps the database's own exception (MongoException)
 * so that whoever uses the repository only catches this one, whatever database is behind it.
 */
public class MetadataRepositoryException extends RuntimeException {

    public MetadataRepositoryException(String message, Throwable cause) {
        super(message, cause);
    }
}