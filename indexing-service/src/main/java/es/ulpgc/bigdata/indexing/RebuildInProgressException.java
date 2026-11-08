package es.ulpgc.bigdata.indexing;

/**
 * A rebuild of the index runs in some replica (DR-10): POST /index/rebuild, /index/update and
 * /index/publish answer 409 REBUILD_IN_PROGRESS.
 */
public final class RebuildInProgressException extends RuntimeException {

    public RebuildInProgressException() {
        super("Hay una reconstrucción del índice en marcha");
    }
}
