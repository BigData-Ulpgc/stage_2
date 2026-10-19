package es.ulpgc.bigdata.config;

import es.ulpgc.bigdata.datamart.index.MongoInvertedIndex;

import java.util.Objects;

/**
 * Where the inverted index lives (MongoInvertedIndex). Same keys and defaults as Stage 1's
 * AppConfig, now read from environment variables:
 *
 * <pre>
 *   MONGO_URI          mongodb://localhost:27017   (inside Docker: mongodb://mongo:27017)
 *   MONGO_DATABASE     search_engine
 *   MONGO_COLLECTION   inverted_index
 * </pre>
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

    /** Opens the index; fails within a few seconds if MongoDB does not answer. */
    public MongoInvertedIndex openIndex() {
        return new MongoInvertedIndex(uri, database, collection);
    }
}
