package es.ulpgc.bigdata.config;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import es.ulpgc.bigdata.datamart.index.MongoInvertedIndex;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Where the inverted index (MongoInvertedIndex) and the metadata (MongoMetadataRepository)
 * live. Same keys and defaults as Stage 1's AppConfig, now read from environment variables:
 *
 * <pre>
 *   MONGO_URI          mongodb://localhost:27017   (inside Docker: mongodb://mongo:27017)
 *   MONGO_DATABASE     search_engine
 *   MONGO_COLLECTION   inverted_index
 * </pre>
 *
 * MONGO_COLLECTION only names the index collection; the metadata collection is always "books".
 */
public record MongoSettings(String uri, String database, String collection) {

    public static final String MONGO_URI = "MONGO_URI";
    public static final String MONGO_DATABASE = "MONGO_DATABASE";
    public static final String MONGO_COLLECTION = "MONGO_COLLECTION";
    public static final String DEFAULT_URI = "mongodb://localhost:27017";
    static final int POOL_WAIT_SECONDS = 5;

    public MongoSettings {
        Objects.requireNonNull(uri, MONGO_URI);
        Objects.requireNonNull(database, MONGO_DATABASE);
        Objects.requireNonNull(collection, MONGO_COLLECTION);
    }

    public static MongoSettings fromEnv(Env env) {
        return new MongoSettings(
                env.string(MONGO_URI, DEFAULT_URI),
                env.string(MONGO_DATABASE, MongoInvertedIndex.DEFAULT_DATABASE),
                env.string(MONGO_COLLECTION, MongoInvertedIndex.DEFAULT_COLLECTION));
    }

    /**
     * THE MongoClient of a process: it keeps its own connection pool and is thread-safe, so
     * every repository of the service shares it (plan DP-07: one client per process). Whoever
     * calls this owns the client and closes it when the service stops; the repositories that
     * receive its database never close it.
     *
     * Same settings as MongoInvertedIndex: if Mongo does not answer, operations fail after
     * 3 s instead of the driver's default 30 s.
     */
    public MongoClient openClient() {
        return MongoClients.create(clientSettings().build());
    }

    /**
     * THE MongoClient of a process, with a connection pool of a fixed size: at most that many
     * operations talk to MongoDB at once and the others wait. A request that waits longer than
     * POOL_WAIT_SECONDS fails with a MongoException, which the APIs answer with 503, instead of
     * keeping a thread busy for the driver's default of 2 minutes.
     */
    public MongoClient openClient(int maxPoolSize) {
        return MongoClients.create(pooledSettings(maxPoolSize));
    }

    MongoClientSettings pooledSettings(int maxPoolSize) {
        return clientSettings()
                .applyToConnectionPoolSettings(b -> b.maxSize(maxPoolSize).maxWaitTime(POOL_WAIT_SECONDS, TimeUnit.SECONDS))
                .build();
    }

    private MongoClientSettings.Builder clientSettings() {
        return MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(uri))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(3, TimeUnit.SECONDS));
    }

    /**
     * Opens the index with its OWN client (tests, benchmarks); fails within a few seconds if
     * MongoDB does not answer. Closing the index closes that client.
     */
    public MongoInvertedIndex openIndex() {
        return new MongoInvertedIndex(uri, database, collection);
    }

    /**
     * Opens the index on the client of the service (see openClient), in this database and
     * collection. Closing the index leaves the client open: whoever created it closes it.
     */
    public MongoInvertedIndex openIndex(MongoClient client) {
        Objects.requireNonNull(client, "client");
        return new MongoInvertedIndex(client.getDatabase(database), collection);
    }
}
