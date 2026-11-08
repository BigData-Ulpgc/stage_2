package es.ulpgc.bigdata.indexing.benchmark;

import org.junit.jupiter.api.Test;

import java.util.OptionalDouble;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProgressTest {

    /** 5 s of start-up with nothing indexed, then 10 books/s up to 100 books at 15 s. */
    private static Progress steadyAfterStartUp() {
        Progress p = new Progress();
        for (int s = 0; s <= 5; s++) {
            p.add(s, 0);
        }
        for (int s = 6; s <= 15; s++) {
            p.add(s, (s - 5) * 10L);
        }
        return p;
    }

    @Test
    void elPrimerLibroMarcaElArranque() {
        assertEquals(OptionalDouble.of(6), steadyAfterStartUp().secondsToReach(1));
        assertEquals(OptionalDouble.of(15), steadyAfterStartUp().secondsToReach(100));
        assertTrue(steadyAfterStartUp().secondsToReach(101).isEmpty(), "no llegó a 101");
    }

    @Test
    void elRitmoEntreEl10YEl90PorCientoDejaFueraElArranque() {
        Progress p = steadyAfterStartUp();
        assertEquals(10.0, p.rate(100, 0.1, 0.9).getAsDouble(), 1e-9);
        assertEquals(100 / 15.0, 100 / p.secondsToReach(100).getAsDouble(), 1e-9,
                "el total sí cuenta el arranque: por eso es más lento");
    }

    @Test
    void porTramosSeVeSiElRitmoCae() {
        Progress p = new Progress();
        p.add(0, 0);
        p.add(10, 50);     // 5 books/s
        p.add(30, 100);    // then 2.5
        assertEquals(5.0, p.rate(100, 0.0, 0.5).getAsDouble(), 1e-9);
        assertEquals(2.5, p.rate(100, 0.5, 1.0).getAsDouble(), 1e-9);
    }

    @Test
    void sinLlegarAlFinalDelTramoNoHayRitmo() {
        Progress p = new Progress();
        p.add(0, 0);
        p.add(10, 50);
        assertTrue(p.rate(100, 0.1, 0.9).isEmpty());
    }

    @Test
    void siTodoLlegaEnUnaMuestraNoHayRitmoQueCalcular() {
        Progress p = new Progress();
        p.add(0, 0);
        p.add(3, 100);
        assertTrue(p.rate(100, 0.1, 0.9).isEmpty(), "sin tiempo entre los dos momentos");
    }

    @Test
    void lasMuestrasNoPuedenIrHaciaAtras() {
        Progress p = new Progress();
        p.add(5, 10);
        assertThrows(IllegalArgumentException.class, () -> p.add(4, 10));
        assertThrows(IllegalArgumentException.class, () -> p.add(6, 9));
    }

    @Test
    void lasFraccionesDebenTenerSentido() {
        Progress p = steadyAfterStartUp();
        assertThrows(IllegalArgumentException.class, () -> p.rate(100, 0.9, 0.1));
        assertThrows(IllegalArgumentException.class, () -> p.rate(0, 0.1, 0.9));
        assertThrows(IllegalArgumentException.class, () -> p.rate(100, 0.1, 1.1));
    }
}
