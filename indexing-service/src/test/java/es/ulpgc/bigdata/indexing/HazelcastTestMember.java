package es.ulpgc.bigdata.indexing;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import es.ulpgc.bigdata.coordination.CoordinationNames;
import es.ulpgc.bigdata.coordination.CoordinationSettings;

import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * One Hazelcast member embedded in the test JVM, started once and shared by the tests of this
 * module: no Docker needed, so these tests always run.
 *
 * A reduced copy of common's HazelcastTestCluster (in common's tests, not published as a jar
 * yet): one member is enough here, Indexing only uses book-state and index-events. On 127.0.0.1
 * from port 5921 and with a random cluster name, so it never joins the compose cluster
 * (5701/5702). Replace with HazelcastTestCluster when common publishes its test-jar.
 */
final class HazelcastTestMember {

    private static HazelcastInstance member;
    private static String clusterName;

    private HazelcastTestMember() {
    }

    static synchronized HazelcastInstance start() {
        if (member == null) {
            System.setProperty("hazelcast.logging.type", "jdk");
            Logger.getLogger("com.hazelcast").setLevel(Level.WARNING);
            clusterName = "indexing-test-" + UUID.randomUUID();
            Config config = new Config();
            config.setClusterName(clusterName);
            config.setProperty("hazelcast.phone.home.enabled", "false");
            config.getJetConfig().setEnabled(false);
            config.getNetworkConfig().setPort(5921).setPortAutoIncrement(true);
            JoinConfig join = config.getNetworkConfig().getJoin();
            join.getAutoDetectionConfig().setEnabled(false);
            join.getMulticastConfig().setEnabled(false);
            join.getTcpIpConfig().setEnabled(false);
            config.getMapConfig(CoordinationNames.BOOK_STATE)
                    .addIndexConfig(new IndexConfig(IndexType.HASH, "state"));
            member = Hazelcast.newHazelcastInstance(config);
            Runtime.getRuntime().addShutdownHook(new Thread(Hazelcast::shutdownAll));
        }
        return member;
    }

    /** "127.0.0.1:<port>" of the member, for HZ_MEMBERS. */
    static String address() {
        return "127.0.0.1:" + start().getCluster().getLocalMember().getAddress().getPort();
    }

    static String clusterName() {
        start();
        return clusterName;
    }

    static CoordinationSettings settings() {
        return new CoordinationSettings(List.of(address()), clusterName(), 120, 3, 10);
    }

    /** Empties book-state and the queues and frees index-writer: every test starts from nothing. */
    static void reset() {
        HazelcastInstance hz = start();
        hz.getMap(CoordinationNames.LOCKS).forceUnlock(CoordinationNames.INDEX_WRITER_LOCK);
        hz.getMap(CoordinationNames.BOOK_STATE).clear();
        hz.getQueue(CoordinationNames.INGEST_QUEUE).clear();
        hz.getQueue(CoordinationNames.INDEX_QUEUE).clear();
    }
}
