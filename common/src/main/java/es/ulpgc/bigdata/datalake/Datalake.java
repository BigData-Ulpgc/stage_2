package es.ulpgc.bigdata.datalake;

import es.ulpgc.bigdata.model.BookLocation;
import es.ulpgc.bigdata.model.RawBook;

import java.util.List;
import java.util.Optional;

/**
 * Common contract for any physical organisation of the datalake
 * (book, range, time). The rest of the system depends on this interface,
 * never on a concrete implementation.
 *
 * Rules (contract §6.2):
 *  - A book exists only when it is complete: save writes its ready marker last, and locate
 *    and listBookIds only see books that have it. A save interrupted halfway leaves nothing.
 *  - save never modifies a book that already exists: it returns where it is. Saving the same
 *    book again, also from two places at once, is safe.
 */
public interface Datalake {

    String name();

    BookLocation save(RawBook book);

    Optional<BookLocation> locate(int bookId);

    List<Integer> listBookIds();
}