package es.ulpgc.bigdata.coordination;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.RoutingMode;
import com.hazelcast.core.HazelcastInstance;
import es.ulpgc.bigdata.config.Env;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Where the Hazelcast cluster is and the coordination parameters (contract §8), read from
 * environment variables like every other setting (Env):
 *
 * <pre>
 *   HZ_MEMBERS            localhost:5701,localhost:5702   (inside Docker: hz-1:5701,hz-2:5701)
 *   HZ_CLUSTER_NAME       search-engine
 *   LEASE_SECONDS         120   lease of a claim (Ingestion, Indexing)
 *   MAX_ATTEMPTS          3     failed attempts per stage before FAILED (all)
 *   LEASE_CHECK_SECONDS   10    period of the expired-lease check (Control)
 * </pre>
 *
 * In this package and not in config because it belongs to the coordination contract.
 */
public record CoordinationSettings(List<String> members, String clusterName, int leaseSeconds,
                                   int maxAttempts, int leaseCheckSeconds) {

    public static final String HZ_MEMBERS = "HZ_MEMBERS";
    public static final String HZ_CLUSTER_NAME = "HZ_CLUSTER_NAME";
    public static final String LEASE_SECONDS = "LEASE_SECONDS";
    public static final String MAX_ATTEMPTS = "MAX_ATTEMPTS";
    public static final String LEASE_CHECK_SECONDS = "LEASE_CHECK_SECONDS";

    public static final String DEFAULT_MEMBERS = "localhost:5701,localhost:5702";
    public static final String DEFAULT_CLUSTER_NAME = "search-engine";
    public static final int DEFAULT_LEASE_SECONDS = 120;
    public static final int DEFAULT_MAX_ATTEMPTS = 3;
    public static final int DEFAULT_LEASE_CHECK_SECONDS = 10;

    public CoordinationSettings {
        members = List.copyOf(members);
        if (members.isEmpty()) {
            throw new IllegalArgumentException(HZ_MEMBERS + " must list at least one member");
        }
        Objects.requireNonNull(clusterName, HZ_CLUSTER_NAME);
    }

    public static CoordinationSettings fromEnv(Env env) {
        List<String> members = Arrays.stream(env.string(HZ_MEMBERS, DEFAULT_MEMBERS).split(","))
                .map(String::strip)
                .filter(m -> !m.isEmpty())
                .toList();
        return new CoordinationSettings(
                members,
                env.string(HZ_CLUSTER_NAME, DEFAULT_CLUSTER_NAME),
                env.integerAtLeast(LEASE_SECONDS, DEFAULT_LEASE_SECONDS, 1),
                env.integerAtLeast(MAX_ATTEMPTS, DEFAULT_MAX_ATTEMPTS, 1),
                env.integerAtLeast(LEASE_CHECK_SECONDS, DEFAULT_LEASE_CHECK_SECONDS, 1));
    }

    /**
     * THE Hazelcast client of a process: thread-safe, shared by every worker of the service.
     * Whoever calls this owns it and shuts it down when the service stops.
     *
     * SINGLE_MEMBER routing: the members announce their Docker-internal addresses (hz-1:5701),
     * which a process on the host cannot reach, so the client talks only to the addresses of
     * HZ_MEMBERS and moves to the next one if its member dies. Inside Docker it works the same.
     *
     * If the cluster is down the client keeps retrying (Hazelcast default: no connect
     * timeout), so a service survives a restart of the cluster (contract §7).
     */
    public HazelcastInstance openClient() {
        ClientConfig config = new ClientConfig();
        config.setClusterName(clusterName);
        config.getNetworkConfig().setAddresses(members);
        config.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.SINGLE_MEMBER);
        return HazelcastClient.newHazelcastClient(config);
    }
}
