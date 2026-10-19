package es.ulpgc.bigdata.datalake;

import es.ulpgc.bigdata.model.BookLocation;
import es.ulpgc.bigdata.model.RawBook;

import java.util.List;
import java.util.Optional;

/**
 * Common contract for any physical organisation of the datalake
 * (book, range, time). The rest of the system depends on this interface,
 * never on a concrete implementation.
 */
public interface Datalake {

    String name();

    BookLocation save(RawBook book);

    Optional<BookLocation> locate(int bookId);

    List<Integer> listBookIds();
}