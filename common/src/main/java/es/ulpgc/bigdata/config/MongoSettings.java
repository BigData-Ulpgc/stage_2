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
        return MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(uri))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(3, TimeUnit.SECONDS))
                .build());
    }

    /** Opens the index; fails within a few seconds if MongoDB does not answer. */
    public MongoInvertedIndex openIndex() {
        return new MongoInvertedIndex(uri, database, collection);
    }
}
