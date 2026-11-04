package es.ulpgc.bigdata.coordination;

import es.ulpgc.bigdata.config.Env;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CoordinationSettingsTest {

    @Test
    void defaultsMatchTheContract() {
        CoordinationSettings s = CoordinationSettings.fromEnv(Env.of(Map.of()));
        assertEquals(List.of("localhost:5701", "localhost:5702"), s.members());
        assertEquals("search-engine", s.clusterName());
        assertEquals(120, s.leaseSeconds());
        assertEquals(3, s.maxAttempts());
        assertEquals(10, s.leaseCheckSeconds());
    }

    @Test
    void membersAreACommaSeparatedListWithSpacesAllowed() {
        CoordinationSettings s = CoordinationSettings.fromEnv(Env.of(Map.of("HZ_MEMBERS", " hz-1:5701 , hz-2:5701 ,")));
        assertEquals(List.of("hz-1:5701", "hz-2:5701"), s.members());
    }

    @Test
    void anEmptyMemberListIsRejected() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> CoordinationSettings.fromEnv(Env.of(Map.of("HZ_MEMBERS", " , "))));
        assertTrue(e.getMessage().contains("HZ_MEMBERS"));
    }

    @Test
    void leaseAndAttemptsMustBePositive() {
        assertThrows(IllegalArgumentException.class,
                () -> CoordinationSettings.fromEnv(Env.of(Map.of("LEASE_SECONDS", "0"))));
        assertThrows(IllegalArgumentException.class,
                () -> CoordinationSettings.fromEnv(Env.of(Map.of("MAX_ATTEMPTS", "0"))));
        assertThrows(IllegalArgumentException.class,
                () -> CoordinationSettings.fromEnv(Env.of(Map.of("LEASE_CHECK_SECONDS", "x"))));
    }
}
