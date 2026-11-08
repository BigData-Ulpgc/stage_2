package es.ulpgc.bigdata.search.fixture;

import es.ulpgc.bigdata.search.fixture.MongoFixtureLoader.Book;

import java.util.List;
import java.util.Map;

/**
 * The fixture of the Search tests: 9 books with headers in Project Gutenberg format and
 * SHORT bodies written for the tests, so that every expected result can be checked by eye.
 * Titles, authors and release dates of 1342, 76, 74, 98, 46, 2701 and 11 are those of
 * stage_1/sample_dataset; 67979 and 17489 add an "Original publication" line and a
 * non-English book, which the sample does not have.
 *
 * Publication state (CURRENT = version 2):
 * <pre>
 *   version 1   1342, 76, 74, 98
 *   version 2   46, 67979, 17489
 *   version 3   11      in index_books but CURRENT still says 2: a publication stopped halfway
 *   none        2701    indexed but not published
 * </pre>
 * So the visible books are exactly the 7 of versions 1 and 2. 2701 and 11 contain
 * "adventure" and "river" on purpose: a search that returned them would be showing books
 * that are not published.
 */
public final class TestBooks {

    public static final int CURRENT_VERSION = 2;

    public static final List<Book> BOOKS = List.of(
            book(1342, "Pride and Prejudice", "Jane Austen", "June 1, 1998", null, "English",
                    "It is a truth universally acknowledged, that a single man in possession of a good fortune, "
                            + "must be in want of a wife. Pride and prejudice in the country."),
            book(76, "Adventures of Huckleberry Finn", "Mark Twain", "June 29, 2004", null, "English",
                    "I went down the river on a raft with Jim. An adventure on the Mississippi river."),
            book(74, "The Adventures of Tom Sawyer, Complete", "Mark Twain", "July 1, 2004", null, "English",
                    "Tom painted the fence and went on an adventure by the river with Huck."),
            book(98, "A Tale of Two Cities", "Charles Dickens", "January 1, 1994", null, "English",
                    "It was the best of times, it was the worst of times. A tale of two cities, London and Paris."),
            book(46, "A Christmas Carol in Prose; Being a Ghost Story of Christmas", "Charles Dickens",
                    "August 11, 2004", null, "English",
                    "Marley was dead to begin with. Scrooge and the ghost of Christmas walked through London."),
            book(67979, "The Blue Castle: a novel", "L. M. Montgomery", "April 5, 2022",
                    "Toronto: McClelland and Stewart, 1926", "English",
                    "Valancy dreamed of a blue castle by the lake, a quiet adventure of her own."),
            book(17489, "Les misérables Tome I: Fantine", "Victor Hugo", "January 10, 2006", null, "French",
                    "Monseigneur Bienvenu vivait a Digne avec sa soeur. Une aventure de Jean Valjean."),
            book(11, "Alice's Adventures in Wonderland", "Lewis Carroll", "June 27, 2008", null, "English",
                    "Alice followed the white rabbit down the hole: an adventure by the river bank."),
            book(2701, "Moby Dick; Or, The Whale", "Herman Melville", "July 1, 2001", null, "English",
                    "Call me Ishmael. I went to sea on a whaling adventure, far from the river."));

    public static final Map<Integer, Integer> VERSION_OF = Map.of(
            1342, 1, 76, 1, 74, 1, 98, 1,
            46, 2, 67979, 2, 17489, 2,
            11, 3);

    /** Ids Search must see with CURRENT = 2, ascending. */
    public static final List<Integer> VISIBLE_IDS = List.of(46, 74, 76, 98, 1342, 17489, 67979);

    private TestBooks() {
    }

    /** Loads the fixture into the given database, posting lists in "inverted_index". */
    public static void load(String mongoUri, String database) {
        MongoFixtureLoader.load(mongoUri, database, "inverted_index", BOOKS, VERSION_OF, CURRENT_VERSION);
    }

    private static Book book(int id, String title, String author, String releaseDate,
                             String originalPublication, String language, String body) {
        StringBuilder header = new StringBuilder()
                .append("The Project Gutenberg eBook of ").append(title).append("\n\n")
                .append("Title: ").append(title).append("\n\n")
                .append("Author: ").append(author).append("\n\n")
                .append("Release date: ").append(releaseDate).append(" [eBook #").append(id).append("]\n\n");
        if (originalPublication != null) {
            header.append("Original publication: ").append(originalPublication).append("\n\n");
        }
        header.append("Language: ").append(language).append("\n");
        return new Book(id, header.toString(), body);
    }
}
