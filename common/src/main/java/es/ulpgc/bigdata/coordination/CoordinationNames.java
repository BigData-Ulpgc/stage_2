package es.ulpgc.bigdata.coordination;

/**
 * Names of the Hazelcast structures of the coordination contract (api/contracts/coordination.md
 * §2). They are written ONLY here: hazelcast/hazelcast.yaml configures the same names, and
 * changing one means changing both and the contract (plan §7).
 */
public final class CoordinationNames {

    /** IMap&lt;Integer, BookTask&gt;: state of each book. */
    public static final String BOOK_STATE = "book-state";
    /** IQueue&lt;Integer&gt;: ids to download. */
    public static final String INGEST_QUEUE = "ingest-queue";
    /** IQueue&lt;Integer&gt;: ids to index. */
    public static final String INDEX_QUEUE = "index-queue";
    /** ReliableTopic&lt;IndexPublished&gt;: one message per published version. */
    public static final String INDEX_EVENTS = "index-events";
    /** IMap whose only use is the lock of {@link #INDEX_WRITER_LOCK}. */
    public static final String LOCKS = "locks";
    /** Key of the single index writer lock in {@link #LOCKS}. */
    public static final String INDEX_WRITER_LOCK = "index-writer";
    /** IMap&lt;String, Long&gt;: marker of an initialized cluster. */
    public static final String COORDINATION_META = "coordination-meta";
    /** Key in {@link #COORDINATION_META}: epoch ms of the last full seed or recovery. */
    public static final String INITIALIZED = "initialized";

    private CoordinationNames() {
    }
}
