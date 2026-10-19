package es.ulpgc.bigdata.model;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RawBookTest {

    @Test
    void creaUnRawBookConLosCamposEsperados() {
        RawBook book = new RawBook(10, "Title: Red Boat", "red boat sails");

        assertEquals(10, book.id());
        assertEquals("Title: Red Boat", book.header());
        assertEquals("red boat sails", book.body());
    }

    @Test
    void dosRawBookConLosMismosValoresSonIguales() {
        RawBook a = new RawBook(10, "h", "b");
        RawBook b = new RawBook(10, "h", "b");

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void dosRawBookConDistintoIdNoSonIguales() {
        RawBook a = new RawBook(10, "h", "b");
        RawBook b = new RawBook(20, "h", "b");

        assertNotEquals(a, b);
    }
}
