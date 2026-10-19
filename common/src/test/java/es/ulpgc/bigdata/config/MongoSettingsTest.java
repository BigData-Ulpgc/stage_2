package es.ulpgc.bigdata.config;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MongoSettingsTest {

    @Test
    void defaultsAreTheStage1Ones() {
        MongoSettings settings = MongoSettings.fromEnv(Env.of(Map.of()));

        assertEquals("mongodb://localhost:27017", settings.uri());
        assertEquals("search_engine", settings.database());
        assertEquals("inverted_index", settings.collection());
    }

    @Test
    void environmentWins() {
        MongoSettings settings = MongoSettings.fromEnv(Env.of(Map.of(
                "MONGO_URI", "mongodb://mongo:27017",
                "MONGO_DATABASE", "search_engine_test",
                "MONGO_COLLECTION", "idx")));

        assertEquals("mongodb://mongo:27017", settings.uri());
        assertEquals("search_engine_test", settings.database());
        assertEquals("idx", settings.collection());
    }
}
