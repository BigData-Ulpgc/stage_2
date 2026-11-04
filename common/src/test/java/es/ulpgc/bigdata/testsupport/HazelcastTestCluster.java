package es.ulpgc.bigdata.testsupport;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import es.ulpgc.bigdata.coordination.CoordinationNames;
import es.ulpgc.bigdata.coordination.CoordinationSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Two Hazelcast members embedded in the test JVM, started once and shared by every test.
 * Unlike MongoDB, no Docker is needed: the coordination tests always run, also in CI.
 *
 * Same settings as hazelcast/hazelcast.yaml where they matter to the tests (backup-count 1,
 * the index on book-state.state, no multicast, no Jet), on 127.0.0.1 from port 5901 and with a
 * random cluster name, so they never join the compose cluster (5701/5702).
 *
 * Difference with production: these members DO have our classes on their classpath. That the
 * official image works without them was verified against the compose cluster (contract §10).
 */
public final class HazelcastTestCluster {

    private static final List<HazelcastInstance> MEMBERS = new ArrayList<>();
    private static String clusterName;

    private HazelcastTestCluster() {
    }

    /** Starts the two members the first time. */
    public static synchronized void start() {
        if (!MEMBERS.isEmpty()) {
            return;
        }
        System.setProperty("hazelcast.logging.type", "jdk");
        Logger.getLogger("com.hazelcast").setLevel(Level.WARNING);
        clusterName = "coordination-test-" + UUID.randomUUID();
        for (int i = 0; i < 2; i++) {
            MEMBERS.add(Hazelcast.newHazelcastInstance(memberConfig()));
        }
        Runtime.getRuntime().addShutdownHook(new Thread(Hazelcast::shutdownAll));
        // Two members that did not find each other would be two clusters of one, and the tests
        // of backups and of several clients would prove nothing.
        int size = MEMBERS.get(0).getCluster().getMembers().size();
        if (size != 2) {
            throw new IllegalStateException("the test members formed a cluster of " + size + ", expected 2");
        }
    }

    private static Config memberConfig() {
        Config config = new Config();
        config.setClusterName(clusterName);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.getJetConfig().setEnabled(false);
        config.getNetworkConfig().setPort(5901).setPortAutoIncrement(true);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        // Without a port Hazelcast tries the configured one and the next two: 5901-5903
        join.getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        config.getMapConfig(CoordinationNames.BOOK_STATE).setBackupCount(1)
                .addIndexConfig(new IndexConfig(IndexType.HASH, "state"));
        return config;
    }

    /** Settings that point to the test members: lease 60 s, 3 attempts. */
    public static CoordinationSettings settings() {
        start();
        List<String> addresses = MEMBERS.stream()
                .map(m -> m.getCluster().getLocalMember().getAddress())
                .map(a -> a.getHost() + ":" + a.getPort())
                .toList();
        return new CoordinationSettings(addresses, clusterName, 60, 3, 10);
    }

    /** Destroys every coordination structure, so each test starts empty. */
    public static void reset() {
        HazelcastInstance any = MEMBERS.get(0);
        any.getMap(CoordinationNames.BOOK_STATE).clear();
        any.getQueue(CoordinationNames.INGEST_QUEUE).clear();
        any.getQueue(CoordinationNames.INDEX_QUEUE).clear();
        any.getMap(CoordinationNames.COORDINATION_META).clear();
        any.getMap(CoordinationNames.LOCKS).forceUnlock(CoordinationNames.INDEX_WRITER_LOCK);
    }
}
