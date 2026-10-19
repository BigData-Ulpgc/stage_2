package es.ulpgc.bigdata.datamart.metadata;

/**
 * Error of the metadata store. It wraps the SQLException so that whoever uses
 * the repository does not have to import anything from JDBC.
 */
public class MetadataRepositoryException extends RuntimeException {

    public MetadataRepositoryException(String message, Throwable cause) {
        super(message, cause);
    }
}